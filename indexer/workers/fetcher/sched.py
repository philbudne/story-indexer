"""
scheduler for threaded/queue fetcher

Scoreboard/issue/retire terminology comes from CPU instruction
scheduling.

hides details of data structures and locking.

agnostic about meaning of "id" (passed in from fetcher)

GENERALLY avoids logging, since logging with a held lock
is a bad idea (may involve a blocking DNS lookup!)
and full URLs are not (currently) passed to all calls
(they can be passed as "note" to start())
"""

import logging
import math
import os
import threading
import time
from enum import Enum
from typing import Any, Callable, NamedTuple, NoReturn, Type

# number of seconds after start of last request to keep idle slot around
# if no active/delayed requests and no errors (maintains request RTT)
SLOT_RECENT_MINUTES = 5

# exponentially decayed moving average coefficient for avg_seconds.
# (used in TCP for RTT averaging, and in system load averages)
# see https://en.wikipedia.org/wiki/Exponential_smoothing
# Scrapy essentially uses 0.5: (old_avg + sample) / 2
#    avg = ALPHA * new + BETA * avg
#    where ALPHA + BETA == 1.0, or BETA = 1.0 - ALPHA
#    => avg = new * ALPHA + (1 - ALPHA) * avg
#    => avg = new * ALPHA + avg - avg * ALPHA
#    => avg = (new - avg) * ALPHA + avg
ALPHA = 0.25  # applied to new samples

logger = logging.getLogger(__name__)  # used only for debug: URL not known here


LineOutputFunc = Callable[..., None]


class LockError(RuntimeError):
    """
    base class for locking exceptions
    """


class LockNotHeldError(LockError):
    """
    lock was not held, when should be
    """


class LockHeldError(LockError):
    """
    lock was held, when should not be
    """


class LockTimeout(LockError):
    """
    took too long to get lock
    """


class Lock:
    """
    wrapper for threading.Lock
    keeps track of thread that holds lock
    """

    def __init__(self, name: str, error_handler: Callable[[], None]):
        self.name = str
        self._lock = threading.Lock()
        self._error_handler = error_handler
        self._owner: threading.Thread | None = None

    def held(self) -> bool:
        """
        return True if current thread already holds lock
        """
        return self._owner is threading.current_thread()

    def _raise(self, exc_type: Type[LockError]) -> NoReturn:
        """
        here on fatal locking error
        call error handler and raise exception
        """
        self._error_handler()
        raise exc_type(self.name)

    def assert_held(self) -> None:
        if not self.held():
            self._raise(LockNotHeldError)

    def assert_not_held(self) -> None:
        if self.held():
            self._raise(LockHeldError)

    def __enter__(self) -> Any:
        self.assert_not_held()  # non-recursive lock
        if not self._lock.acquire(timeout=120):
            self._raise(LockTimeout)
        self._owner = threading.current_thread()

    def __exit__(self, type: Any, value: Any, traceback: Any) -> None:
        self._owner = None
        self._lock.release()


_NEVER = 0.0


class Timer:
    """
    measure intervals; doesn't start ticking until reset called.
    """

    def __init__(self, duration: float | None) -> None:
        """
        lock is container object lock (for asserts)
        """
        self.last = _NEVER
        self.duration = duration

    def elapsed(self) -> float:
        """
        return seconds since last "reset"
        """
        if self.last == _NEVER:
            return math.inf
        return time.monotonic() - self.last

    def reset(self) -> None:
        """
        (re)start from zero
        """
        self.last = time.monotonic()

    def expired(self) -> bool:
        """
        return True if duration set, and time duration has passed
        """
        return self.duration is not None and self.elapsed() >= self.duration

    def __str__(self) -> str:
        """
        used in log messages!
        """
        if self.last == _NEVER:
            return "never"
        # add a '*' if self.expired()?
        return f"{self.elapsed():.3f}"


class Alarm:
    """
    time until a future event (born expired)
    """

    def __init__(self) -> None:
        self.time = 0.0

    def set(self, delay: float) -> None:
        """
        if unexpired, extend expiration by delay seconds;
        else set expiration to delay seconds in future
        """
        now = time.monotonic()
        if self.time > now:
            self.time += delay
        else:
            self.time = now + delay

    def delay(self) -> float:
        """
        return seconds until alarm expires
        """
        return self.time - time.monotonic()

    def __str__(self) -> str:
        """
        used in log messages!
        """
        delay = self.delay()
        if delay >= 0:
            return "%.2f" % delay
        return "READY"


class ConnStatus(Enum):
    """
    connection status, reported to Slot.retire
    """

    NOCONN = -2
    BADURL = -1
    NODATA = 0
    DATA = 1
    THROTTLE = 2


class StartStatus(Enum):
    """
    status from start()
    """

    OK = 1  # ok to start
    SKIP = 2  # recently reported down
    BUSY = 3  # currently busy


DELAY_SKIP = -1.0  # recent attempt failed
DELAY_LONG = -2.0  # delay to long to hold


class FinishRet(NamedTuple):
    """
    return from slot.finish()
    """

    old_average: float
    average: float
    interval: float
    active: int
    delayed: int


class Slot:
    """
    A slot for a single id (eg domain) within a ScoreBoard
    """

    def __init__(self, slot_id: str, sb: "ScoreBoard"):
        self.slot_id = slot_id  # ie; domain
        self.sb = sb

        # time since last error at this workplace:
        self.last_conn_error = Timer(sb.conn_retry_seconds)

        # average request time kept as smoothed average.
        # zero means will be initialized by first sample
        # (could start at initial_interval_seconds * target_concurrency)
        self.avg_seconds = 0.0
        self.issue_interval = sb.initial_interval_seconds

        self.last_start = Timer(SLOT_RECENT_MINUTES)

        self.next_issue = Alarm()
        self.delayed = 0
        self.active = 0

        # O(n) removal, only used for debug_info
        # unclear if using a Set would be better or not...
        self.active_threads: list[str] = []

    def _get_delay(self) -> float:
        """
        return delay in seconds until fetch can begin.
        or value < 0 (DELAY_{SKIP,LONG})
        """

        # NOTE! Lock held: avoid logging!
        self.sb.big_lock.assert_held()

        # see if connection to domain failed "recently".
        if not self.last_conn_error.expired():
            return DELAY_SKIP

        # if next_issue time more than max_delay_seconds away
        # punt the story back to RabbitMQ fast delay queue.
        delay = self.next_issue.delay()
        if delay > self.sb.max_delay_seconds:
            return DELAY_LONG

        # attempt to keep a batch of requests to one (or a few) site(s)
        # from plugging up the queue (most prefetched messages "delayed"
        # with call_later).  When very few sites remain in queue
        # this can cause burts of fetches max_delay_seconds apart.
        if self.delayed >= self.sb.max_delayed_per_slot:
            return DELAY_LONG

        if delay < 0:
            # this site/slot clear to issue: consider some sort of
            # rate limit here to avoid filling up _message_queue when
            # incomming requests are well mixed (100 requests to 100
            # sites will go right to the message queue)!  Could have a
            # scoreboard wide "next_issue" clock??  This is mostly a
            # problem when "prefetch" is large (more than 2x the
            # number of worker threads).
            delay = 0.0  # never issued or past due

        self.next_issue.set(self.issue_interval)  # increment next_issue time
        self.delayed += 1
        self.sb.delayed += 1
        return delay

    def _start(self) -> StartStatus:
        """
        Here in a worker thread, convert from delayed to active
        returns False if connection failed recently
        """
        # NOTE! Lock held: avoid logging!
        self.sb.big_lock.assert_held()

        self.delayed -= 1
        self.sb.delayed -= 1

        # see if connection to domain failed "recently".
        # last test so that preference is short delay
        # (and hope an active fetch succeeds).
        if not self.last_conn_error.expired():
            return StartStatus.SKIP

        if self.active >= self.sb.target_concurrency:
            return StartStatus.BUSY

        self.active += 1
        self.last_start.reset()

        self.active_threads.append(threading.current_thread().name)
        return StartStatus.OK

    def finish(self, conn_status: ConnStatus, sec: float) -> FinishRet:
        """
        called when a fetch attempt has ended.
        """
        with self.sb.big_lock:
            # NOTE! Avoid logging while holding lock!!!

            assert self.active > 0
            self.active -= 1
            # remove on list is O(n), but n is small (concurrency target)
            self.active_threads.remove(threading.current_thread().name)

            old_average = self.avg_seconds
            if conn_status == ConnStatus.NOCONN:
                # failed to get connection, reset time since last error:
                # will prevent new connection attempts until it expires
                # in conn_retry_seconds, avoiding repeated 30 second
                # connection timeouts.
                self.last_conn_error.reset()
                # discard avg connection time estimate:
                new_average = 0.0
            elif conn_status == ConnStatus.THROTTLE:
                if self.avg_seconds >= self.sb.throttle_interval_seconds:
                    # exponential backoff.
                    # see comments where throttle_multiplier is set.
                    new_average = old_average * self.sb.throttle_multiplier
                else:
                    new_average = self.sb.throttle_interval_seconds
            else:  # DATA or NODATA
                if self.avg_seconds == 0:
                    new_average = sec
                else:
                    # exponentially decayed moving average
                    # (see comments on ALPHA defn)
                    new_average = self.avg_seconds + (sec - self.avg_seconds) * ALPHA

            # per-scrapy: only lower average if data received
            # (errors could be returned more quickly than large data
            #  hopefully no one is at the end of a 9600 baud line anymore)
            if new_average > self.avg_seconds or (
                conn_status == ConnStatus.DATA and new_average < self.avg_seconds
            ):
                # average success time has changed, update issue interval
                self.avg_seconds = new_average
                interval = self.avg_seconds / self.sb.target_concurrency
                if interval < self.sb.min_interval_seconds:
                    interval = self.sb.min_interval_seconds
                self.issue_interval = interval

            # adjust scoreboard counters
            self.sb._slot_finished(self.active == 0)

            # pass everything by keyword
            return FinishRet(
                old_average=old_average,
                average=self.avg_seconds,
                interval=self.issue_interval,
                active=self.active,
                delayed=self.delayed,
            )

    def _consider_removing(self) -> None:
        """
        called periodically to check if we're disposable.
        """
        self.sb.big_lock.assert_held()  # PARANOIA
        if (
            self.active == 0  # no active fetches
            and self.delayed == 0  # nothing up our sleeve
            and self.last_start.expired()  # not used in SLOT_RECENT_MINUTES
            and self.last_conn_error.expired()  # no error in conn_retry_seconds
        ):
            self.sb._remove_slot(self.slot_id)


TS_IDLE = "idle"


class ThreadStatus:
    info: str  # thread status (URL or TS_IDLE)
    ts: float  # time.monotonic of last change


class StartRet(NamedTuple):
    """
    return value from start()
    """

    status: StartStatus
    slot: Slot | None


class Stats(NamedTuple):
    """
    statistics returned by periodic()
    """

    slots: int
    active_fetches: int
    active_slots: int
    delayed: int


class ScoreBoard:
    """
    keep score of active requests by "id" (str)
    """

    # arguments changed often in development,
    # so all must be passed by keyword
    def __init__(
        self,
        *,
        target_concurrency: int,  # max active with same id (domain)
        max_delay_seconds: float,  # max time to hold
        conn_retry_seconds: float,  # seconds before connect retry
        min_interval_seconds: float,
        max_delayed_per_slot: int,
        throttle_interval_seconds: float,
        initial_interval_seconds: float,
    ):
        # single lock, rather than one each for scoreboard, active count,
        # and each slot.  Time spent with lock held should be small,
        # and lock ordering issues likely to make code complex and fragile!

        self.big_lock = Lock(
            "big_lock", self.debug_info_nolock
        )  # covers ALL variables!
        self.target_concurrency = target_concurrency
        self.max_delay_seconds = max_delay_seconds
        self.conn_retry_seconds = conn_retry_seconds
        self.min_interval_seconds = min_interval_seconds
        self.max_delayed_per_slot = max_delayed_per_slot
        self.throttle_interval_seconds = throttle_interval_seconds
        self.initial_interval_seconds = initial_interval_seconds

        # Multiplier to apply to slot average when a THROTTLE request
        # seen and average is already over throttle_interval_seconds.
        effective_mult = 2

        # There may be max_delayed_per_slot requests about to go out,
        # so it's likely to see THROTTLE max_delayed_per_slot times in
        # a row.  With the goal of multiplying the average by
        # effective_mult the interval each time a max_delayed_per_slot
        # THROTTLE requests are seen, calculate the multiplier to by
        # calculating the max_delayed_per_slot'th root of effective_mult:
        self.throttle_multiplier = math.exp(
            math.log(effective_mult) / max_delayed_per_slot
        )

        self.slots: dict[str, Slot] = {}  # map "id" (domain) to Slot
        self.active_slots = 0
        self.active_fetches = 0
        self.delayed = 0

        # map thread name to ThreadStatus
        self.thread_status: dict[str, ThreadStatus] = {}

        # so Main shows up in dumps (may be lock holder)
        self._set_thread_status(TS_IDLE)

    def _get_slot(self, slot_id: str) -> Slot:
        self.big_lock.assert_held()  # PARANOIA
        slot = self.slots.get(slot_id, None)
        if not slot:
            slot = self.slots[slot_id] = Slot(slot_id, self)
        return slot

    def _remove_slot(self, slot_id: str) -> None:
        del self.slots[slot_id]

    def get_delay(self, slot_id: str) -> tuple[float, int]:
        """
        called when story first picked up from message queue.
        returns (hold_sec, num_delayed)

        hold_sec: seconds to hold message before starting (delayed counts incremented)
        (negative values are DELAY_{SKIP,LONG}). num_delayed: number of delayed fetches
        for this slot.
        """
        with self.big_lock:
            slot = self._get_slot(slot_id)
            return (slot._get_delay(), slot.delayed)

    def start(self, slot_id: str, note: str) -> StartRet:
        """
        here from worker thread, after delay (if any)
        """
        with self.big_lock:
            slot = self._get_slot(slot_id)
            status = slot._start()
            if status == StartStatus.OK:
                # *MUST* call slot.finished() when done
                if slot.active == 1:  # was idle
                    self.active_slots += 1
                self.active_fetches += 1
                self._set_thread_status(note)  # full URL
                return StartRet(status, slot)
            return StartRet(status, None)

    def _slot_finished(self, slot_idle: bool) -> None:
        """
        here from slot.finished()
        """
        # NOTE! lock held: avoid logging
        self.big_lock.assert_held()
        assert self.active_fetches > 0
        self.active_fetches -= 1
        if slot_idle:
            assert self.active_slots > 0
            self.active_slots -= 1
        self._set_thread_status(TS_IDLE)

    def _set_thread_status(self, info: str) -> None:
        """
        save status for debug info
        """
        # no lock assert: only current thread can write it's own status item
        index = threading.current_thread().name
        ts = self.thread_status.get(index, None)
        if not ts:
            ts = self.thread_status[index] = ThreadStatus()
        ts.info = info
        ts.ts = time.monotonic()

    def periodic(self, dump_file: str | None) -> Stats:
        """
        called periodically from main thread.
        """
        # so Main shows up as lock holder:
        self._set_thread_status("periodic")
        if dump_file:
            tmp = dump_file + ".tmp"
            with open(tmp, "w") as f:

                def ofunc(format: str, *args: object) -> None:
                    f.write((format % args) + "\n")

                stats = self._periodic(ofunc)
            os.rename(tmp, dump_file)
        else:
            stats = self._periodic(None)
        self._set_thread_status(TS_IDLE)
        return stats

    def _periodic(self, func: LineOutputFunc | None) -> Stats:
        with self.big_lock:
            # do aging less frequently?
            # or at fixed interval (not at whim of --interval)
            # get full list first for stable iteration!
            for slot in list(self.slots.values()):
                slot._consider_removing()

            if func:
                self._dump_nolock(func)

            return Stats(
                slots=len(self.slots),
                active_fetches=self.active_fetches,
                active_slots=self.active_slots,
                delayed=self.delayed,
            )

    def _append_dump_nolock(self, fname: str) -> None:
        """
        call for debug only
        """
        with open(fname, "a") as f:
            ts = time.strftime("%F %T", time.gmtime())
            f.write(f"==== {ts}\n")

            def ofunc(format: str, *args: object) -> None:
                f.write((format % args) + "\n")

            self._dump_nolock(ofunc)

    def debug_info_nolock(self) -> None:
        """
        NOTE!!! called when lock attempt times out!
        dumps info without attempting to get lock!
        """
        self._dump_nolock(logger.info)

    def _dump_nolock(self, func: LineOutputFunc) -> None:
        """
        dump debugging info using supplied func passed a % style format;
        can be a logger method, like logger.info.

        NOTE!! Calling with a logger method AND the lock held should
        be avoided, since logging handlers make blocking DNS lookups
        (not on each call).
        """

        func(
            "%s fetches, %d slots, %d delayed",
            self.active_fetches,
            self.active_slots,
            self.delayed,
        )

        # dump all slots:
        if self.slots:
            func("slots:")
        # may be called without lock, so grab list first:
        for id_, slot in list(self.slots.items()):
            func(
                # display: slot id, avg request time, active req, delayed req,
                # time since last req start, last req error, time until next issuable
                "%s: %s avg %.3f, %da, %dd, last issue: %s, last err: %s, next: %s",
                id_,
                ",".join(slot.active_threads),
                slot.avg_seconds,
                slot.active,
                slot.delayed,
                slot.last_start,
                slot.last_conn_error,
                slot.next_issue,
            )

        # dump status for each thread
        # can come here without lock, so take snapshot of owner:
        lock_owner_thread = self.big_lock._owner
        if lock_owner_thread:
            lock_owner_name = lock_owner_thread.name
        else:
            lock_owner_name = "NONE"

        func("threads:")
        now = time.monotonic()
        for name, ts in list(self.thread_status.items()):
            if name == lock_owner_name:
                have_lock = " *LOCK*"
            else:
                have_lock = ""
            if ts.info != TS_IDLE or have_lock:
                # display: thread name, secs since last status update, status
                func("%s%s %.3f %s", name, have_lock, now - ts.ts, ts.info)
