#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

import requests

JST = timezone(timedelta(hours=9))
DEFAULT_URL = "https://script.google.com/macros/s/AKfycby6yQ6G6lip3Y3o1cN-hf1R9MKagILwQt0i_CkFoYLitzBt96r4OBaBsTAgE6B59rI3/exec"
DEFAULT_KEY = "showashokai"


def fetch_json(url: str, params: dict[str, str] | None = None) -> dict:
    response = requests.get(url, params=params or {}, timeout=15)
    if response.status_code != 200:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text[:200]}")
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"non-JSON response: {response.text[:200]}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"invalid payload type: {type(payload).__name__}")
    if payload.get("ok") is False:
        raise RuntimeError(f"API error: {payload}")
    return payload


def main() -> int:
    url = (os.getenv("SHEETS_WEBAPP_URL") or DEFAULT_URL).strip()
    key = (os.getenv("SHEETS_WEBAPP_KEY") or DEFAULT_KEY).strip()
    user_id = "smoke-u1"
    ts = datetime.now(tz=JST).replace(microsecond=0).isoformat()

    checks: list[tuple[str, dict[str, str] | None]] = [
        ("health", None),
        ("init", {"action": "init", "key": key}),
        (
            "append_event",
            {
                "action": "append_event",
                "key": key,
                "userId": user_id,
                "eventType": "CLOCK_IN",
                "timestamp": ts,
                "note": "",
                "source": "web",
            },
        ),
        (
            "list_events",
            {
                "action": "list_events",
                "key": key,
                "userId": user_id,
                "limit": "10",
            },
        ),
    ]

    for name, params in checks:
        payload = fetch_json(url, params)
        print(f"[ok] {name}: {payload}")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[fail] {exc}", file=sys.stderr)
        raise SystemExit(1)
