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


def loop(*, once: bool = False) -> None:
    data_dir = resolve_data_dir()
    db_path = data_dir / "app.sqlite3"
    AppDB(db_path).close()

    while True:
        db = AppDB(db_path)
        interval = max(10, db.get_setting_int("interval_seconds", 300))
        enabled = db.get_setting_bool("scheduler_enabled", True)
        db.close()

        if enabled:
            run_job_once(data_dir=data_dir, reason="worker")
        else:
            log.info("Scheduler disabled (DB setting).")

        if once:
            return
        time.sleep(interval)

