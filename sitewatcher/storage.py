from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class TargetState:
    target_name: str
    signature: str
    content: str
    checked_at: int
    changed_at: int


class StateStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._db_path))
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._init_schema()

    def close(self) -> None:
        self._conn.close()

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS state (
              target_name TEXT PRIMARY KEY,
              signature TEXT NOT NULL,
              content TEXT NOT NULL,
              checked_at INTEGER NOT NULL,
              changed_at INTEGER NOT NULL
            )
            """
        )
        self._conn.commit()

    def get(self, target_name: str) -> Optional[TargetState]:
        row = self._conn.execute(
            "SELECT target_name, signature, content, checked_at, changed_at FROM state WHERE target_name = ?",
            (target_name,),
        ).fetchone()
        if row is None:
            return None
        return TargetState(
            target_name=row[0],
            signature=row[1],
            content=row[2],
            checked_at=int(row[3]),
            changed_at=int(row[4]),
        )

    def upsert(
        self,
        *,
        target_name: str,
        signature: str,
        content: str,
        checked_at: int | None = None,
        changed_at: int | None = None,
    ) -> None:
        now = int(time.time())
        checked_at = now if checked_at is None else int(checked_at)
        changed_at = checked_at if changed_at is None else int(changed_at)
        self._conn.execute(
            """
            INSERT INTO state (target_name, signature, content, checked_at, changed_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(target_name) DO UPDATE SET
              signature=excluded.signature,
              content=excluded.content,
              checked_at=excluded.checked_at,
              changed_at=excluded.changed_at
            """,
            (target_name, signature, content, checked_at, changed_at),
        )
        self._conn.commit()

    def touch_checked(self, *, target_name: str) -> None:
        now = int(time.time())
        self._conn.execute(
            "UPDATE state SET checked_at = ? WHERE target_name = ?",
            (now, target_name),
        )
        self._conn.commit()

