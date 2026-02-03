from __future__ import annotations

import logging
from pathlib import Path
from typing import Literal

from sitewatcher.lock import FileLock
from sitewatcher.monitor import run_once
from sitewatcher.web.db import AppDB


log = logging.getLogger("sitewatcher.job")

RunStatus = Literal["ok", "error", "skipped"]


def run_job_once(*, data_dir: Path, user_id: int, reason: str) -> RunStatus:
    lock = FileLock(data_dir / f"run.{int(user_id)}.lock")
    with lock.acquired() as ok:
        if not ok:
            log.info("Skip run (%s): lock busy (user_id=%s)", reason, user_id)
            return "skipped"

        db_path = data_dir / "app.sqlite3"
        db = AppDB(db_path)
        run_id = db.insert_run(user_id)
        config = db.build_monitor_config(user_id)
        db.close()

        status: RunStatus = "ok"
        message = ""
        try:
            run_once(config, data_dir=data_dir)
        except Exception as e:
            status = "error"
            message = str(e)
            log.exception("Run failed (%s)", reason)

        db2 = AppDB(db_path)
        db2.finish_run(run_id, status=status, message=message)
        db2.close()

        return status
