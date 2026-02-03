from __future__ import annotations

import difflib


def unified_diff(old: str, new: str, *, fromfile: str = "before", tofile: str = "after", n: int = 3) -> str:
    old_lines = (old or "").splitlines(keepends=True)
    new_lines = (new or "").splitlines(keepends=True)
    diff_iter = difflib.unified_diff(old_lines, new_lines, fromfile=fromfile, tofile=tofile, n=n)
    return "".join(diff_iter)

