from __future__ import annotations

import datetime as dt
from typing import Optional


def parse_headers_text(text: str) -> dict[str, str]:
    headers: dict[str, str] = {}
    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        key = k.strip()
        value = v.strip()
        if key and value:
            headers[key] = value
    return headers


def headers_to_text(headers: dict[str, str]) -> str:
    if not headers:
        return ""
    lines = []
    for k in sorted(headers.keys(), key=lambda s: s.lower()):
        lines.append(f"{k}: {headers[k]}")
    return "\n".join(lines)


def format_ts(ts: Optional[int]) -> str:
    if not ts:
        return "-"
    d = dt.datetime.fromtimestamp(int(ts))
    return d.strftime("%Y-%m-%d %H:%M:%S")

