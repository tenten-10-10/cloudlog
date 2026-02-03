from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


def target_state_key(target_id: int) -> str:
    return f"target:{target_id}"


def _bool_int(value: bool) -> int:
    return 1 if value else 0


def _now_ts() -> int:
    return int(time.time())


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _json_loads(text: str, *, default: Any) -> Any:
    if not text:
        return default
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return default


@dataclass(frozen=True)
class NotifierRow:
    name: str
    enabled: bool
    config: dict[str, Any]


@dataclass(frozen=True)
class TargetRow:
    id: int
    name: str
    type: str
    url: str
    selector: Optional[str]
    extract: str
    render_js: bool
    timeout_seconds: int
    headers: dict[str, str]
    notify: list[str]
    enabled: bool
    created_at: int
    updated_at: int


@dataclass(frozen=True)
class RunRow:
    id: int
    started_at: int
    finished_at: Optional[int]
    status: str
    message: str


class AppDB:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False, timeout=30)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._init_schema()
        self._ensure_defaults()

    def close(self) -> None:
        self._conn.close()

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notifiers (
              name TEXT PRIMARY KEY,
              enabled INTEGER NOT NULL,
              config_json TEXT NOT NULL
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS targets (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              name TEXT NOT NULL,
              type TEXT NOT NULL,
              url TEXT NOT NULL,
              selector TEXT,
              extract TEXT NOT NULL DEFAULT 'text',
              render_js INTEGER NOT NULL DEFAULT 0,
              timeout_seconds INTEGER NOT NULL DEFAULT 20,
              headers_json TEXT NOT NULL DEFAULT '{}',
              notify_json TEXT NOT NULL DEFAULT '[]',
              enabled INTEGER NOT NULL DEFAULT 1,
              created_at INTEGER NOT NULL,
              updated_at INTEGER NOT NULL
            )
            """
        )
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_targets_enabled ON targets(enabled)")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              started_at INTEGER NOT NULL,
              finished_at INTEGER,
              status TEXT NOT NULL,
              message TEXT NOT NULL DEFAULT ''
            )
            """
        )
        self._conn.commit()

    def _ensure_defaults(self) -> None:
        self.set_setting_default("interval_seconds", "300")
        self.set_setting_default("notify_on_first", "0")
        self.set_setting_default("scheduler_enabled", "1")

        self._upsert_notifier_default("stdout", enabled=True, config={})
        self._upsert_notifier_default("macos", enabled=True, config={})
        self._upsert_notifier_default(
            "telegram",
            enabled=False,
            config={"bot_token": "", "chat_id": "", "bot_token_env": "TELEGRAM_BOT_TOKEN", "chat_id_env": "TELEGRAM_CHAT_ID"},
        )
        self._upsert_notifier_default(
            "pushover",
            enabled=False,
            config={"app_token": "", "user_key": "", "app_token_env": "PUSHOVER_APP_TOKEN", "user_key_env": "PUSHOVER_USER_KEY"},
        )

    def set_setting_default(self, key: str, value: str) -> None:
        self._conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO NOTHING",
            (key, value),
        )
        self._conn.commit()

    def get_setting(self, key: str, default: str = "") -> str:
        row = self._conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        if row is None:
            return default
        return str(row["value"])

    def get_setting_int(self, key: str, default: int) -> int:
        v = self.get_setting(key, str(default)).strip()
        try:
            return int(v)
        except ValueError:
            return default

    def get_setting_bool(self, key: str, default: bool) -> bool:
        v = self.get_setting(key, "1" if default else "0").strip().lower()
        return v in {"1", "true", "yes", "on"}

    def set_setting(self, key: str, value: str) -> None:
        self._conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self._conn.commit()

    def _upsert_notifier_default(self, name: str, *, enabled: bool, config: dict[str, Any]) -> None:
        row = self._conn.execute("SELECT name FROM notifiers WHERE name = ?", (name,)).fetchone()
        if row is not None:
            return
        self._conn.execute(
            "INSERT INTO notifiers(name, enabled, config_json) VALUES(?, ?, ?)",
            (name, _bool_int(enabled), _json_dumps(config)),
        )
        self._conn.commit()

    def list_notifiers(self) -> list[NotifierRow]:
        rows = self._conn.execute("SELECT name, enabled, config_json FROM notifiers ORDER BY name").fetchall()
        out: list[NotifierRow] = []
        for r in rows:
            out.append(
                NotifierRow(
                    name=str(r["name"]),
                    enabled=bool(int(r["enabled"])),
                    config=dict(_json_loads(str(r["config_json"]), default={}) or {}),
                )
            )
        return out

    def get_notifier(self, name: str) -> Optional[NotifierRow]:
        r = self._conn.execute(
            "SELECT name, enabled, config_json FROM notifiers WHERE name = ?",
            (name,),
        ).fetchone()
        if r is None:
            return None
        return NotifierRow(
            name=str(r["name"]),
            enabled=bool(int(r["enabled"])),
            config=dict(_json_loads(str(r["config_json"]), default={}) or {}),
        )

    def upsert_notifier(self, name: str, *, enabled: bool, config: dict[str, Any]) -> None:
        self._conn.execute(
            """
            INSERT INTO notifiers(name, enabled, config_json) VALUES(?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET enabled=excluded.enabled, config_json=excluded.config_json
            """,
            (name, _bool_int(enabled), _json_dumps(config)),
        )
        self._conn.commit()

    def list_targets(self, *, include_disabled: bool = True) -> list[TargetRow]:
        if include_disabled:
            rows = self._conn.execute("SELECT * FROM targets ORDER BY id DESC").fetchall()
        else:
            rows = self._conn.execute("SELECT * FROM targets WHERE enabled = 1 ORDER BY id DESC").fetchall()
        return [self._row_to_target(r) for r in rows]

    def get_target(self, target_id: int) -> Optional[TargetRow]:
        r = self._conn.execute("SELECT * FROM targets WHERE id = ?", (target_id,)).fetchone()
        if r is None:
            return None
        return self._row_to_target(r)

    def create_target(
        self,
        *,
        name: str,
        type: str,
        url: str,
        selector: Optional[str],
        extract: str,
        render_js: bool,
        timeout_seconds: int,
        headers: dict[str, str],
        notify: list[str],
        enabled: bool,
    ) -> int:
        now = _now_ts()
        cur = self._conn.execute(
            """
            INSERT INTO targets (
              name, type, url, selector, extract, render_js, timeout_seconds,
              headers_json, notify_json, enabled, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                name,
                type,
                url,
                selector,
                extract,
                _bool_int(render_js),
                int(timeout_seconds),
                _json_dumps(headers),
                _json_dumps(notify),
                _bool_int(enabled),
                now,
                now,
            ),
        )
        self._conn.commit()
        return int(cur.lastrowid)

    def update_target(
        self,
        target_id: int,
        *,
        name: str,
        type: str,
        url: str,
        selector: Optional[str],
        extract: str,
        render_js: bool,
        timeout_seconds: int,
        headers: dict[str, str],
        notify: list[str],
        enabled: bool,
    ) -> None:
        now = _now_ts()
        self._conn.execute(
            """
            UPDATE targets SET
              name=?,
              type=?,
              url=?,
              selector=?,
              extract=?,
              render_js=?,
              timeout_seconds=?,
              headers_json=?,
              notify_json=?,
              enabled=?,
              updated_at=?
            WHERE id=?
            """,
            (
                name,
                type,
                url,
                selector,
                extract,
                _bool_int(render_js),
                int(timeout_seconds),
                _json_dumps(headers),
                _json_dumps(notify),
                _bool_int(enabled),
                now,
                int(target_id),
            ),
        )
        self._conn.commit()

    def delete_target(self, target_id: int) -> None:
        self._conn.execute("DELETE FROM targets WHERE id = ?", (int(target_id),))
        self._conn.commit()

    def insert_run(self) -> int:
        now = _now_ts()
        cur = self._conn.execute(
            "INSERT INTO runs(started_at, status, message) VALUES(?, ?, ?)",
            (now, "running", ""),
        )
        self._conn.commit()
        return int(cur.lastrowid)

    def finish_run(self, run_id: int, *, status: str, message: str = "") -> None:
        now = _now_ts()
        self._conn.execute(
            "UPDATE runs SET finished_at=?, status=?, message=? WHERE id=?",
            (now, status, message, int(run_id)),
        )
        self._conn.commit()

    def get_last_run(self) -> Optional[RunRow]:
        r = self._conn.execute("SELECT * FROM runs ORDER BY id DESC LIMIT 1").fetchone()
        if r is None:
            return None
        return RunRow(
            id=int(r["id"]),
            started_at=int(r["started_at"]),
            finished_at=int(r["finished_at"]) if r["finished_at"] is not None else None,
            status=str(r["status"]),
            message=str(r["message"] or ""),
        )

    def build_monitor_config(self) -> dict[str, Any]:
        notifiers_cfg: dict[str, Any] = {}
        for n in self.list_notifiers():
            notifiers_cfg[n.name] = {"enabled": n.enabled, **n.config}

        targets_cfg: list[dict[str, Any]] = []
        for t in self.list_targets(include_disabled=False):
            targets_cfg.append(
                {
                    "state_key": target_state_key(t.id),
                    "name": t.name,
                    "type": t.type,
                    "url": t.url,
                    "selector": t.selector,
                    "extract": t.extract,
                    "render_js": t.render_js,
                    "timeout_seconds": t.timeout_seconds,
                    "headers": t.headers,
                    "notify": t.notify,
                }
            )

        return {
            "notify_on_first": self.get_setting_bool("notify_on_first", False),
            "interval_seconds": self.get_setting_int("interval_seconds", 300),
            "notifiers": notifiers_cfg,
            "targets": targets_cfg,
        }

    def _row_to_target(self, r: sqlite3.Row) -> TargetRow:
        headers = _json_loads(str(r["headers_json"]), default={}) or {}
        if not isinstance(headers, dict):
            headers = {}
        headers_norm = {str(k): str(v) for k, v in headers.items()}

        notify = _json_loads(str(r["notify_json"]), default=[]) or []
        if not isinstance(notify, list):
            notify = []
        notify_norm = [str(x) for x in notify]

        selector = r["selector"]
        selector_norm = str(selector).strip() if selector is not None else None
        if selector_norm == "":
            selector_norm = None

        return TargetRow(
            id=int(r["id"]),
            name=str(r["name"]),
            type=str(r["type"]).lower(),
            url=str(r["url"]),
            selector=selector_norm,
            extract=str(r["extract"] or "text").lower(),
            render_js=bool(int(r["render_js"])),
            timeout_seconds=int(r["timeout_seconds"] or 20),
            headers=headers_norm,
            notify=notify_norm,
            enabled=bool(int(r["enabled"])),
            created_at=int(r["created_at"]),
            updated_at=int(r["updated_at"]),
        )

