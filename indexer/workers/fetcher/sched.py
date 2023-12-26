"""
scheduler for threaded fetcher

Scoreboard/issue/retire terminology comes from CPU instruction
scheduling.

hides details of data structures and locking.
"""

import logging
import math
import threading
import time
from enum import Enum
from typing import Any, Dict, NamedTuple, Optional

from indexer.app import App

logger = logging.getLogger(__name__)


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


class Lock:
    """
    wrapper for threading.Lock
    keeps track of thread that holds lock
    """

    def __init__(self, name: str):
        self.name = str
        self._lock = threading.Lock()
        self._owner: Optional[threading.Thread] = None

    def held(self) -> bool:
        """
        return True if current thread already holds lock
        """
        return self._owner is threading.current_thread()

    def _assert(self, condition: bool) -> None:
        assert condition  # XXX show thread & self.name!!

    def assert_held(self) -> None:
        if not self.held():
            raise LockNotHeldError(self.name)

    def assert_not_held(self) -> None:
        if self.held():
            raise LockHeldError(self.name)

    def acquire(self) -> None:
        self.assert_not_held()  # non-recursive lock
        self._lock.acquire()
        self._owner = threading.current_thread()

    def release(self) -> None:
        self._owner = None
        self._lock.release()

    def __enter__(self) -> Any:
        self.acquire()

    def __exit__(self, type: Any, value: Any, traceback: Any) -> None:
        self.release()


_NEVER = 0.0


class Stopwatch:
    """
    measure intervals; doesn't start ticking until reset called.
    """

    def __init__(self) -> None:
        """
        lock is container object lock (for asserts)
        """
        self.last = _NEVER

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


class IssueStatus(Enum):
    """
    return value from Slot._issue
    """

    OK = 0  # slot assigned
    BUSY = 1  # too many fetches active or too soon
    SKIPPED = 2  # recent connection error


class Slot:
    """
    A slot for a single id (eg domain) within a ScoreBoard
    """

    def __init__(self, slot_id: str, sb: "ScoreBoard"):
        self.slot_id = slot_id  # ie; domain
        self.sb = sb

        self.active = 0
        self.last_issue = Stopwatch()

        # time since last error at this workplace
        self.last_conn_error = Stopwatch()

    def _issue(self) -> IssueStatus:
        """
        return True if safe to issue (must call "retire" after)
        return False if cannot be issued now
        """
        self.sb.big_lock.assert_held()
        if self.active >= self.sb.max_per_slot:
            return IssueStatus.BUSY

        if self.last_issue.elapsed() < self.sb.min_seconds:
            # issued recently
            return IssueStatus.BUSY

        # see if connection to domain failed "recently".
        # last test so that preference is short delay
        # (and hope an active fetch succeeds).
        if self.last_conn_error.elapsed() < self.sb.conn_retry_seconds:
            return IssueStatus.SKIPPED

        self.active += 1
        self.last_issue.reset()
        return IssueStatus.OK

    def retire(self, got_connection: bool) -> None:
        """
        called when a fetch attempt has ended
        """
        with self.sb.big_lock:
            assert self.active > 0
            self.active -= 1
            if not got_connection:
                self.last_conn_error.reset()

            # adjust scoreboard counters
            self.sb._slot_retired(self.active == 0)

            # consider removing idle slot if no bans in place
            self._consider_removing()

    def _consider_removing(self) -> None:
        self.sb.big_lock.assert_held()  # PARANOIA
        if (
            self.active == 0
            and self.last_issue.elapsed() >= self.sb.min_seconds
            and self.last_conn_error.elapsed() >= self.sb.conn_retry_seconds
        ):
            logger.debug("removing idle slot %s", self.slot_id)
            self.sb._remove_slot(self.slot_id)


# status/value tuple: popular in GoLang
class IssueReturn(NamedTuple):
    status: IssueStatus
    slot: Optional[Slot]  # if status == OK


class ScoreBoard:
    """
    keep score of active requests by "id" (str)
    """

    def __init__(
        self,
        max_active: int,  # total concurrent active limit
        max_per_slot: int,  # max active with same id (domain)
        min_seconds: float,  # seconds between issues for slot
        conn_retry_seconds: float,  # seconds before connect retry
    ):
        # single lock, rather than one each for scoreboard, active count,
        # and each slot.  Time spent with lock held should be small,
        # and lock ordering issues likely to make code complex and fragile!

        self.big_lock = Lock("big_lock")  # covers ALL variables!
        self.max_active = max_active
        self.max_per_slot = max_per_slot
        self.min_seconds = min_seconds
        self.conn_retry_seconds = conn_retry_seconds
        self.slots: Dict[str, Slot] = {}
        self.active_fetches = 0
        self.active_slots = 0
        self.thread_status: Dict[str, Optional[str]] = {}

    def _get_slot(self, slot_id: str) -> Slot:
        # _COULD_ try to use IP addresses to map to slots, BUT would
        # have to deal with multiple addrs per domain name and
        # possibility of non-overlapping sets from different fqdns
        self.big_lock.assert_held()  # PARANOIA
        slot = self.slots.get(slot_id, None)
        if not slot:
            slot = self.slots[slot_id] = Slot(slot_id, self)
        return slot

    def _remove_slot(self, slot_id: str) -> None:
        del self.slots[slot_id]

    def issue(self, slot_id: str, note: str) -> IssueReturn:
        with self.big_lock:
            if self.active_fetches < self.max_active:
                slot = self._get_slot(slot_id)
                status = slot._issue()
                if status == IssueStatus.OK:
                    # *MUST* call slot.retire() when done
                    if slot.active == 1:  # was idle
                        self.active_slots += 1
                    self.active_fetches += 1
                    self._set_thread_status(note)  # full URL
                    return IssueReturn(status, slot)
            else:
                status = IssueStatus.BUSY
        return IssueReturn(status, None)

    def _slot_retired(self, idle: bool) -> None:
        """
        here from slot.retired()
        """
        self.big_lock.assert_held()
        assert self.active_fetches > 0
        self.active_fetches -= 1
        if idle:
            assert self.active_slots > 0
            self.active_slots -= 1
        self._set_thread_status("-")

    def _set_thread_status(self, info: str) -> None:
        index = threading.current_thread().name
        self.thread_status[index] = info

    def status(self, app: App) -> None:
        """
        called periodically from main thread
        """
        with self.big_lock:
            # do this less frequently?
            for slot in list(self.slots.values()):
                slot._consider_removing()

            logger.info(
                "%d slots; %d URLs in %d domains active",
                len(self.slots),
                self.active_fetches,
                self.active_slots,
            )

            app.gauge("active.fetches", self.active_fetches)
            app.gauge("active.slots", self.active_slots)
