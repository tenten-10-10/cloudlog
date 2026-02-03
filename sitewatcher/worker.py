from __future__ import annotations

import logging
import os
import time
from pathlib import Path

from sitewatcher.job import run_job_once
from sitewatcher.web.db import AppDB


log = logging.getLogger("sitewatcher.worker")


def resolve_data_dir() -> Path:
    raw = os.getenv("SITEWATCHER_DATA_DIR", ".sitewatcher")
    p = Path(raw)
    if not p.is_absolute():
        p = (Path.cwd() / p).resolve()
    p.mkdir(parents=True, exist_ok=True)
    return p


def run_scheduler_tick(*, data_dir: Path, reason: str) -> int:
    db_path = data_dir / "app.sqlite3"
    db = AppDB(db_path)
    db.ensure_bootstrap_admin()
    schedule_rows = db.list_users_for_schedule()
    db.close()

    now = int(time.time())
    ran_any = False
    for row in schedule_rows:
        user = row.user
        if not user.scheduler_enabled:
            continue
        interval = max(10, int(user.interval_seconds))
        last = row.last_run_at
        if last is None or now - int(last) >= interval:
            run_job_once(data_dir=data_dir, user_id=user.id, reason=reason)
            ran_any = True

    # Polling interval: keep small enough to respect per-user intervals without tight loop.
    return 10 if ran_any else 15


def loop(*, once: bool = False) -> None:
    data_dir = resolve_data_dir()
    db_path = data_dir / "app.sqlite3"
    db = AppDB(db_path)
    db.ensure_bootstrap_admin()
    db.close()

    while True:
        try:
            interval = run_scheduler_tick(data_dir=data_dir, reason="worker")
        except Exception:
            log.exception("Scheduler tick failed")
            interval = 30

        if once:
            return
        time.sleep(interval)
