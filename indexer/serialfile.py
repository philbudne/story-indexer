"""
API for storing an integer API cookie
(could be generalized to a string)

Abstracted to class in a module in case there is ever
a "properties" database table (ES index?) for such things
"""

import logging
import os

logger = logging.getLogger(__name__)


class SerialFile:
    """
    read/write a file that keeps a numeric cookie

    MUST call last and write in pairs
    (NEVER assume last thing written is the contents of the file)!
    """

    def __init__(self, path: str, force: bool):
        self.path = path
        self.force = force
        self.old_stats: os.stat_result | None = None
        self._last = 0  # used when force=True

    def next(self) -> int:
        """
        return serial number of next item to read, or 0
        """
        if self.force:
            return self._last

        try:
            with open(self.path) as f:
                next_ = int(f.readline().strip())
                self.old_stats = os.fstat(f.fileno())
        except FileNotFoundError:
            next_ = 0
            logger.info("%s not found: using %d", self.path, next_)
            self.old_stats = None
        return next_

    def write(self, next_: int) -> None:
        """
        write cookie to file.
        Do NOT assume file will contain last written contents!!!
        """
        if self.force:
            self._last = next_
            return

        if self.old_stats:
            old_mtime = self.old_stats.st_mtime
            self.old_stats = None
            new_stat = os.stat(self.path)
            if new_stat.st_mtime != old_mtime:
                logger.warning(
                    "%s: modification time changed since read (not writing)", self.path
                )
                return

        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            f.write(f"{next_}\n")
        # try to survive unremovable .prev file
        try:
            if os.path.exists(self.path):
                os.rename(self.path, self.path + ".prev")
        except OSError:
            pass
        os.rename(tmp, self.path)
