from __future__ import annotations

import json
import os
import secrets
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from sitewatcher.web.auth import hash_password


SCHEMA_VERSION = 2

BOOTSTRAP_USER_ENV = "SITEWATCHER_ADMIN_USER"
BOOTSTRAP_PASSWORD_ENV = "SITEWATCHER_ADMIN_PASSWORD"
BOOTSTRAP_PASSWORD_HASH_ENV = "SITEWATCHER_ADMIN_PASSWORD_HASH"


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
class UserRow:
    id: int
    username: str
    is_admin: bool
    interval_seconds: int
    scheduler_enabled: bool
    notify_on_first: bool
    created_at: int
    updated_at: int


@dataclass(frozen=True)
class UserAuthRow:
    id: int
    username: str
    password_hash: str
    is_admin: bool


@dataclass(frozen=True)
class NotifierRow:
    name: str
    enabled: bool
    config: dict[str, Any]


@dataclass(frozen=True)
class TargetRow:
    id: int
    user_id: int
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
    user_id: int
    started_at: int
    finished_at: Optional[int]
    status: str
    message: str


@dataclass(frozen=True)
class UserScheduleRow:
    user: UserRow
    last_run_at: Optional[int]


class ConfigError(RuntimeError):
    pass


class AppDB:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False, timeout=30)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._init_schema()

    def close(self) -> None:
        self._conn.close()

    def _table_exists(self, name: str) -> bool:
        row = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (name,),
        ).fetchone()
        return row is not None

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            )
            """
        )
        self._conn.commit()

        version = self.get_setting_int("schema_version", 0)
        if version == 0:
            if self._table_exists("users"):
                self.set_setting("schema_version", str(SCHEMA_VERSION))
                version = SCHEMA_VERSION
            elif self._table_exists("notifiers") or self._table_exists("targets") or self._table_exists("runs"):
                version = 1
            else:
                self.set_setting("schema_version", str(SCHEMA_VERSION))
                version = SCHEMA_VERSION

        if version == 1:
            self._migrate_v1_to_v2()
            version = SCHEMA_VERSION

        if version != SCHEMA_VERSION:
            raise ConfigError(f"Unsupported schema_version: {version}")

        self._ensure_schema_v2()

    def _ensure_schema_v2(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              username TEXT NOT NULL UNIQUE,
              password_hash TEXT NOT NULL,
              is_admin INTEGER NOT NULL DEFAULT 0,
              interval_seconds INTEGER NOT NULL DEFAULT 300,
              scheduler_enabled INTEGER NOT NULL DEFAULT 1,
              notify_on_first INTEGER NOT NULL DEFAULT 0,
              created_at INTEGER NOT NULL,
              updated_at INTEGER NOT NULL
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_notifiers (
              user_id INTEGER NOT NULL,
              name TEXT NOT NULL,
              enabled INTEGER NOT NULL,
              config_json TEXT NOT NULL,
              PRIMARY KEY (user_id, name),
              FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS targets (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              user_id INTEGER NOT NULL,
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
              updated_at INTEGER NOT NULL,
              FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_targets_user ON targets(user_id)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_targets_user_enabled ON targets(user_id, enabled)")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              user_id INTEGER NOT NULL,
              started_at INTEGER NOT NULL,
              finished_at INTEGER,
              status TEXT NOT NULL,
              message TEXT NOT NULL DEFAULT '',
              FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_user ON runs(user_id)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_user_started ON runs(user_id, started_at)")
        self._conn.commit()

    def _migrate_v1_to_v2(self) -> None:
        # v1 had: settings, notifiers, targets, runs (global)
        # Create v2 tables that do NOT conflict with v1.
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              username TEXT NOT NULL UNIQUE,
              password_hash TEXT NOT NULL,
              is_admin INTEGER NOT NULL DEFAULT 0,
              interval_seconds INTEGER NOT NULL DEFAULT 300,
              scheduler_enabled INTEGER NOT NULL DEFAULT 1,
              notify_on_first INTEGER NOT NULL DEFAULT 0,
              created_at INTEGER NOT NULL,
              updated_at INTEGER NOT NULL
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_notifiers (
              user_id INTEGER NOT NULL,
              name TEXT NOT NULL,
              enabled INTEGER NOT NULL,
              config_json TEXT NOT NULL,
              PRIMARY KEY (user_id, name),
              FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        self._conn.commit()

        old_interval = self.get_setting_int("interval_seconds", 300)
        old_scheduler_enabled = self.get_setting_bool("scheduler_enabled", True)
        old_notify_on_first = self.get_setting_bool("notify_on_first", False)

        username = (os.getenv(BOOTSTRAP_USER_ENV, "admin") or "admin").strip()
        password_hash = (os.getenv(BOOTSTRAP_PASSWORD_HASH_ENV, "") or "").strip()
        password_plain = (os.getenv(BOOTSTRAP_PASSWORD_ENV, "") or "").strip()
        if password_hash:
            ph = password_hash
        elif password_plain:
            ph = hash_password(password_plain)
        else:
            ph = hash_password(secrets.token_urlsafe(32))

        now = _now_ts()
        cur = self._conn.execute(
            """
            INSERT INTO users(username, password_hash, is_admin, interval_seconds, scheduler_enabled, notify_on_first, created_at, updated_at)
            VALUES(?, ?, 1, ?, ?, ?, ?, ?)
            """,
            (username, ph, int(old_interval), _bool_int(old_scheduler_enabled), _bool_int(old_notify_on_first), now, now),
        )
        user_id = int(cur.lastrowid)

        # Rename v1 tables out of the way.
        had_notifiers = self._table_exists("notifiers")
        had_targets = self._table_exists("targets")
        had_runs = self._table_exists("runs")
        if had_notifiers:
            self._conn.execute("ALTER TABLE notifiers RENAME TO notifiers_old")
        if had_targets:
            self._conn.execute("ALTER TABLE targets RENAME TO targets_old")
        if had_runs:
            self._conn.execute("ALTER TABLE runs RENAME TO runs_old")
        self._conn.commit()

        # Now create v2 schema fully (targets/runs tables now don't exist).
        self._ensure_schema_v2()

        if had_notifiers and self._table_exists("notifiers_old"):
            self._conn.execute(
                """
                INSERT INTO user_notifiers(user_id, name, enabled, config_json)
                SELECT ?, name, enabled, config_json FROM notifiers_old
                """,
                (user_id,),
            )
            self._conn.execute("DROP TABLE notifiers_old")

        if had_targets and self._table_exists("targets_old"):
            self._conn.execute(
                """
                INSERT INTO targets(
                  id, user_id, name, type, url, selector, extract, render_js, timeout_seconds,
                  headers_json, notify_json, enabled, created_at, updated_at
                )
                SELECT
                  id, ?, name, type, url, selector, extract, render_js, timeout_seconds,
                  headers_json, notify_json, enabled, created_at, updated_at
                FROM targets_old
                """,
                (user_id,),
            )
            self._conn.execute("DROP TABLE targets_old")

        if had_runs and self._table_exists("runs_old"):
            self._conn.execute(
                """
                INSERT INTO runs(id, user_id, started_at, finished_at, status, message)
                SELECT id, ?, started_at, finished_at, status, message FROM runs_old
                """,
                (user_id,),
            )
            self._conn.execute("DROP TABLE runs_old")

        # Ensure missing notifiers are present.
        self._ensure_user_notifier_defaults(user_id)

        self.set_setting("schema_version", str(SCHEMA_VERSION))
        self._conn.commit()

    # ----- Global settings (schema/migrations only) -----
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

    # ----- Users -----
    def count_users(self) -> int:
        r = self._conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()
        return int(r["c"]) if r else 0

    def list_users(self) -> list[UserRow]:
        rows = self._conn.execute("SELECT * FROM users ORDER BY id ASC").fetchall()
        return [self._row_to_user(r) for r in rows]

    def list_users_for_schedule(self) -> list[UserScheduleRow]:
        rows = self._conn.execute(
            """
            SELECT
              u.*,
              lr.last_run_at AS last_run_at
            FROM users u
            LEFT JOIN (
              SELECT user_id, MAX(COALESCE(finished_at, started_at)) AS last_run_at
              FROM runs
              GROUP BY user_id
            ) lr ON lr.user_id = u.id
            ORDER BY u.id ASC
            """
        ).fetchall()
        out: list[UserScheduleRow] = []
        for r in rows:
            user = self._row_to_user(r)
            last = r["last_run_at"]
            out.append(UserScheduleRow(user=user, last_run_at=int(last) if last is not None else None))
        return out

    def get_user(self, user_id: int) -> Optional[UserRow]:
        r = self._conn.execute("SELECT * FROM users WHERE id = ?", (int(user_id),)).fetchone()
        if r is None:
            return None
        return self._row_to_user(r)

    def get_user_auth_by_username(self, username: str) -> Optional[UserAuthRow]:
        r = self._conn.execute(
            "SELECT id, username, password_hash, is_admin FROM users WHERE username = ?",
            (username,),
        ).fetchone()
        if r is None:
            return None
        return UserAuthRow(
            id=int(r["id"]),
            username=str(r["username"]),
            password_hash=str(r["password_hash"]),
            is_admin=bool(int(r["is_admin"])),
        )

    def create_user(self, *, username: str, password_hash: str, is_admin: bool = False) -> int:
        now = _now_ts()
        cur = self._conn.execute(
            """
            INSERT INTO users(username, password_hash, is_admin, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?)
            """,
            (username, password_hash, _bool_int(is_admin), now, now),
        )
        self._conn.commit()
        user_id = int(cur.lastrowid)
        self._ensure_user_notifier_defaults(user_id)
        return user_id

    def ensure_bootstrap_admin(self) -> Optional[int]:
        username = (os.getenv(BOOTSTRAP_USER_ENV, "") or "").strip()
        password_hash = (os.getenv(BOOTSTRAP_PASSWORD_HASH_ENV, "") or "").strip()
        password_plain = (os.getenv(BOOTSTRAP_PASSWORD_ENV, "") or "").strip()

        if not username:
            return None
        if not password_hash and not password_plain:
            return None

        existing = self.get_user_auth_by_username(username)
        if existing is not None:
            if not existing.is_admin:
                self._conn.execute("UPDATE users SET is_admin = 1 WHERE id = ?", (existing.id,))
                self._conn.commit()
            self._ensure_user_notifier_defaults(existing.id)
            return existing.id

        ph = password_hash or hash_password(password_plain)
        return self.create_user(username=username, password_hash=ph, is_admin=True)

    def update_user_settings(
        self,
        user_id: int,
        *,
        interval_seconds: int,
        scheduler_enabled: bool,
        notify_on_first: bool,
    ) -> None:
        now = _now_ts()
        self._conn.execute(
            """
            UPDATE users SET
              interval_seconds=?,
              scheduler_enabled=?,
              notify_on_first=?,
              updated_at=?
            WHERE id=?
            """,
            (int(interval_seconds), _bool_int(scheduler_enabled), _bool_int(notify_on_first), now, int(user_id)),
        )
        self._conn.commit()

    def update_user_password(self, user_id: int, *, password_hash: str) -> None:
        now = _now_ts()
        self._conn.execute(
            "UPDATE users SET password_hash=?, updated_at=? WHERE id=?",
            (password_hash, now, int(user_id)),
        )
        self._conn.commit()

    def _ensure_user_notifier_defaults(self, user_id: int) -> None:
        self._upsert_user_notifier_default(user_id, "stdout", enabled=True, config={})
        self._upsert_user_notifier_default(user_id, "macos", enabled=False, config={})
        self._upsert_user_notifier_default(
            user_id,
            "telegram",
            enabled=False,
            config={"bot_token": "", "chat_id": "", "bot_token_env": "TELEGRAM_BOT_TOKEN", "chat_id_env": "TELEGRAM_CHAT_ID"},
        )
        self._upsert_user_notifier_default(
            user_id,
            "pushover",
            enabled=False,
            config={"app_token": "", "user_key": "", "app_token_env": "PUSHOVER_APP_TOKEN", "user_key_env": "PUSHOVER_USER_KEY"},
        )

    def _upsert_user_notifier_default(self, user_id: int, name: str, *, enabled: bool, config: dict[str, Any]) -> None:
        row = self._conn.execute(
            "SELECT name FROM user_notifiers WHERE user_id = ? AND name = ?",
            (int(user_id), name),
        ).fetchone()
        if row is not None:
            return
        self._conn.execute(
            "INSERT INTO user_notifiers(user_id, name, enabled, config_json) VALUES(?, ?, ?, ?)",
            (int(user_id), name, _bool_int(enabled), _json_dumps(config)),
        )
        self._conn.commit()

    # ----- User notifiers -----
    def list_notifiers(self, user_id: int) -> list[NotifierRow]:
        rows = self._conn.execute(
            "SELECT name, enabled, config_json FROM user_notifiers WHERE user_id = ? ORDER BY name",
            (int(user_id),),
        ).fetchall()
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

    def get_notifier(self, user_id: int, name: str) -> Optional[NotifierRow]:
        r = self._conn.execute(
            "SELECT name, enabled, config_json FROM user_notifiers WHERE user_id = ? AND name = ?",
            (int(user_id), name),
        ).fetchone()
        if r is None:
            return None
        return NotifierRow(
            name=str(r["name"]),
            enabled=bool(int(r["enabled"])),
            config=dict(_json_loads(str(r["config_json"]), default={}) or {}),
        )

    def upsert_notifier(self, user_id: int, name: str, *, enabled: bool, config: dict[str, Any]) -> None:
        self._conn.execute(
            """
            INSERT INTO user_notifiers(user_id, name, enabled, config_json) VALUES(?, ?, ?, ?)
            ON CONFLICT(user_id, name) DO UPDATE SET enabled=excluded.enabled, config_json=excluded.config_json
            """,
            (int(user_id), name, _bool_int(enabled), _json_dumps(config)),
        )
        self._conn.commit()

    # ----- Targets -----
    def list_targets(self, user_id: int, *, include_disabled: bool = True) -> list[TargetRow]:
        if include_disabled:
            rows = self._conn.execute(
                "SELECT * FROM targets WHERE user_id = ? ORDER BY id DESC",
                (int(user_id),),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM targets WHERE user_id = ? AND enabled = 1 ORDER BY id DESC",
                (int(user_id),),
            ).fetchall()
        return [self._row_to_target(r) for r in rows]

    def get_target(self, user_id: int, target_id: int) -> Optional[TargetRow]:
        r = self._conn.execute(
            "SELECT * FROM targets WHERE user_id = ? AND id = ?",
            (int(user_id), int(target_id)),
        ).fetchone()
        if r is None:
            return None
        return self._row_to_target(r)

    def create_target(
        self,
        user_id: int,
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
              user_id, name, type, url, selector, extract, render_js, timeout_seconds,
              headers_json, notify_json, enabled, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(user_id),
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
        user_id: int,
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
    ) -> bool:
        now = _now_ts()
        cur = self._conn.execute(
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
            WHERE user_id=? AND id=?
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
                int(user_id),
                int(target_id),
            ),
        )
        self._conn.commit()
        return cur.rowcount > 0

    def delete_target(self, user_id: int, target_id: int) -> bool:
        cur = self._conn.execute(
            "DELETE FROM targets WHERE user_id = ? AND id = ?",
            (int(user_id), int(target_id)),
        )
        self._conn.commit()
        return cur.rowcount > 0

    # ----- Runs -----
    def insert_run(self, user_id: int) -> int:
        now = _now_ts()
        cur = self._conn.execute(
            "INSERT INTO runs(user_id, started_at, status, message) VALUES(?, ?, ?, ?)",
            (int(user_id), now, "running", ""),
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

    def get_last_run(self, user_id: int) -> Optional[RunRow]:
        r = self._conn.execute(
            "SELECT * FROM runs WHERE user_id = ? ORDER BY id DESC LIMIT 1",
            (int(user_id),),
        ).fetchone()
        if r is None:
            return None
        return RunRow(
            id=int(r["id"]),
            user_id=int(r["user_id"]),
            started_at=int(r["started_at"]),
            finished_at=int(r["finished_at"]) if r["finished_at"] is not None else None,
            status=str(r["status"]),
            message=str(r["message"] or ""),
        )

    # ----- Monitor config per user -----
    def build_monitor_config(self, user_id: int) -> dict[str, Any]:
        user = self.get_user(user_id)
        if user is None:
            raise ConfigError(f"User not found: {user_id}")
        self._ensure_user_notifier_defaults(user_id)

        notifiers_cfg: dict[str, Any] = {}
        for n in self.list_notifiers(user_id):
            notifiers_cfg[n.name] = {"enabled": n.enabled, **n.config}

        targets_cfg: list[dict[str, Any]] = []
        for t in self.list_targets(user_id, include_disabled=False):
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
            "notify_on_first": bool(user.notify_on_first),
            "notifiers": notifiers_cfg,
            "targets": targets_cfg,
        }

    # ----- Row mappers -----
    def _row_to_user(self, r: sqlite3.Row) -> UserRow:
        return UserRow(
            id=int(r["id"]),
            username=str(r["username"]),
            is_admin=bool(int(r["is_admin"])),
            interval_seconds=int(r["interval_seconds"] or 300),
            scheduler_enabled=bool(int(r["scheduler_enabled"])),
            notify_on_first=bool(int(r["notify_on_first"])),
            created_at=int(r["created_at"]),
            updated_at=int(r["updated_at"]),
        )

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
            user_id=int(r["user_id"]),
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
