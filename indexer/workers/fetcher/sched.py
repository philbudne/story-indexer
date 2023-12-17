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
from typing import Any, Dict, Optional

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


class Slot:
    """
    A slot for a single id (eg domain) within a ScoreBoard
    """

    def __init__(self, id: str, sb: "ScoreBoard"):
        self.id = id  # ie; domain
        self.sb = sb

        self.active = 0
        self.last_issue = Stopwatch()
        self.last_error = Stopwatch()

    def issue(self) -> bool:
        """
        return True if safe to issue (must call "retire" after)
        return False if cannot be issued now
        """
        if self.active >= self.sb.max_per_slot:
            return False

        if self.last_error.elapsed() < self.sb.retry_seconds:
            # failed recently
            return False

        if self.last_issue.elapsed() < self.sb.min_seconds:
            # issued recently
            return False

        self.active += 1
        self.last_issue.reset()
        return True

    def retire(self, succ: bool) -> None:
        """
        called when a fetch attempt has ended
        """
        self.active -= 1
        if not succ:
            # "zero seconds since last error at this workplace"
            self.last_error.reset()

        self.sb._retire_slot(self, succ)


class ScoreBoard:
    """
    keep score of active requests by "id" (str)
    """

    def __init__(
        self,
        max_active: int,  # total concurrent active limit
        max_per_slot: int,  # max active with same id (domain)
        min_seconds: float,  # seconds between issues for slot
        retry_seconds: float,  # seconds before retry
    ):
        # single lock, rather than one each for scoreboard, active count,
        # and each slot.  Time spent with lock held should be small,
        # and lock ordering issues likely to make code complex and fragile!

        self.big_lock = Lock("big_lock")  # covers ALL variables!
        self.max_active = max_active
        self.max_per_slot = max_per_slot
        self.min_seconds = min_seconds
        self.retry_seconds = retry_seconds
        self.slots: Dict[str, Slot] = {}
        self.active = 0

    def _get_slot(self, id: str) -> Slot:
        # _COULD_ try to use IP addresses to map to slots, BUT would
        # have to deal with multiple addrs per domain name and
        # possibility of overlapping sets!
        self.big_lock.assert_held()  # PARANOIA
        slot = self.slots.get(id, None)
        if not slot:
            slot = self.slots[id] = Slot(id, self)
        return slot

    def issue(self, id: str, note: str) -> Optional[Slot]:
        with self.big_lock:
            if self.active < self.max_active:
                slot = self._get_slot(id)
                if slot.issue():
                    # *MUST* call slot.retire() when done
                    self.active += 1
                    return slot

        # "more Pythonic" to raise Exception?
        # but 3x slower, and fairly common
        return None

    def _retire_slot(self, slot: Slot, succ: bool) -> None:
        """
        here when an attempt has ended.
        """
        with self.big_lock:
            assert self.active > 0
            self.active -= 1

    def status(self) -> None:
        """
        called periodically from main thread
        """
        # XXX if "reissue" done for each redirect step,
        # perhaps delete slots that have been idle for a LONG time
        # so memory not occupied with slots from transient redirects.
        with self.big_lock:  # ensure consistent results (risk: could hang)
            logger.info("%d slots, %d active", len(self.slots), self.active)
