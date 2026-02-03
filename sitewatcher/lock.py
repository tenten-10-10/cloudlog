from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Iterator, Optional


class FileLock:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._fp = None

    def try_acquire(self) -> bool:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fp = self._path.open("a+")
        try:
            import fcntl  # type: ignore

            fcntl.flock(fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except Exception:
            fp.close()
            return False

        self._fp = fp
        return True

    def release(self) -> None:
        fp = self._fp
        self._fp = None
        if fp is None:
            return
        try:
            import fcntl  # type: ignore

            fcntl.flock(fp.fileno(), fcntl.LOCK_UN)
        finally:
            fp.close()

    @contextlib.contextmanager
    def acquired(self) -> Iterator[bool]:
        ok = self.try_acquire()
        try:
            yield ok
        finally:
            if ok:
                self.release()


def try_lock(path: Path) -> Optional[FileLock]:
    lock = FileLock(path)
    if lock.try_acquire():
        return lock
    return None

