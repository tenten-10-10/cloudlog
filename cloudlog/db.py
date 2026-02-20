from __future__ import annotations

import json
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sitewatcher.web.auth import hash_password

SCHEMA_VERSION = 2


ROLE_ADMIN = "admin"
ROLE_MANAGER = "manager"
ROLE_MEMBER = "member"

ROLE_ORDER = {
    ROLE_MEMBER: 0,
    ROLE_MANAGER: 1,
    ROLE_ADMIN: 2,
}


STATUS_DRAFT = "draft"
STATUS_SUBMITTED = "submitted"
STATUS_APPROVED = "approved"
STATUS_REJECTED = "rejected"


def now_ts() -> int:
    return int(time.time())


@dataclass(frozen=True)
class UserRow:
    id: int
    username: str
    role: str
    hourly_cost: float
    active: bool
    created_at: int
    updated_at: int


@dataclass(frozen=True)
class UserAuthRow:
    id: int
    username: str
    role: str
    password_hash: str
    active: bool


@dataclass(frozen=True)
class ClientRow:
    id: int
    name: str
    created_at: int
    updated_at: int


@dataclass(frozen=True)
class ProjectRow:
    id: int
    client_id: int | None
    client_name: str | None
    name: str
    code: str
    description: str
    status: str
    budget_hours: float
    budget_cost: float
    bill_rate: float
    start_date: str | None
    end_date: str | None
    created_at: int
    updated_at: int


@dataclass(frozen=True)
class TaskRow:
    id: int
    project_id: int
    project_name: str
    project_code: str
    name: str
    status: str
    created_at: int
    updated_at: int


@dataclass(frozen=True)
class EntryRow:
    id: int
    user_id: int
    username: str
    project_id: int
    project_name: str
    project_code: str
    task_id: int | None
    task_name: str | None
    work_date: str
    minutes: int
    note: str
    status: str
    approver_id: int | None
    approver_name: str | None
    approved_at: int | None
    reject_reason: str | None
    created_at: int
    updated_at: int


@dataclass(frozen=True)
class TimerRow:
    user_id: int
    project_id: int
    task_id: int | None
    note: str
    started_at: int


@dataclass(frozen=True)
class AttendanceRow:
    id: int
    user_id: int
    username: str
    work_date: str
    clock_in_at: int | None
    clock_out_at: int | None
    note: str
    updated_by_user_id: int | None
    updated_by_username: str | None
    created_at: int
    updated_at: int


class CloudlogDB:
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

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            )
            """
        )
        version = self.get_setting_int("schema_version", 0)
        if version == 0:
            self.set_setting("schema_version", str(SCHEMA_VERSION))
            version = SCHEMA_VERSION
        if version > SCHEMA_VERSION:
            raise RuntimeError(f"Unsupported schema_version={version}")

        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              username TEXT NOT NULL UNIQUE,
              password_hash TEXT NOT NULL,
              role TEXT NOT NULL DEFAULT 'member',
              hourly_cost REAL NOT NULL DEFAULT 0,
              active INTEGER NOT NULL DEFAULT 1,
              created_at INTEGER NOT NULL,
              updated_at INTEGER NOT NULL
            )
            """
        )

        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS clients (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              name TEXT NOT NULL UNIQUE,
              created_at INTEGER NOT NULL,
              updated_at INTEGER NOT NULL
            )
            """
        )

        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS projects (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              client_id INTEGER,
              name TEXT NOT NULL,
              code TEXT NOT NULL UNIQUE,
              description TEXT NOT NULL DEFAULT '',
              status TEXT NOT NULL DEFAULT 'active',
              budget_hours REAL NOT NULL DEFAULT 0,
              budget_cost REAL NOT NULL DEFAULT 0,
              bill_rate REAL NOT NULL DEFAULT 0,
              start_date TEXT,
              end_date TEXT,
              created_at INTEGER NOT NULL,
              updated_at INTEGER NOT NULL,
              FOREIGN KEY(client_id) REFERENCES clients(id) ON DELETE SET NULL
            )
            """
        )

        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              project_id INTEGER NOT NULL,
              name TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'active',
              created_at INTEGER NOT NULL,
              updated_at INTEGER NOT NULL,
              UNIQUE(project_id, name),
              FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE
            )
            """
        )

        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS time_entries (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              user_id INTEGER NOT NULL,
              project_id INTEGER NOT NULL,
              task_id INTEGER,
              work_date TEXT NOT NULL,
              minutes INTEGER NOT NULL,
              note TEXT NOT NULL DEFAULT '',
              status TEXT NOT NULL DEFAULT 'draft',
              approver_id INTEGER,
              approved_at INTEGER,
              reject_reason TEXT,
              created_at INTEGER NOT NULL,
              updated_at INTEGER NOT NULL,
              FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
              FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE RESTRICT,
              FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE SET NULL,
              FOREIGN KEY(approver_id) REFERENCES users(id) ON DELETE SET NULL
            )
            """
        )

        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS entry_history (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              entry_id INTEGER NOT NULL,
              actor_id INTEGER NOT NULL,
              action TEXT NOT NULL,
              note TEXT NOT NULL DEFAULT '',
              created_at INTEGER NOT NULL,
              FOREIGN KEY(entry_id) REFERENCES time_entries(id) ON DELETE CASCADE,
              FOREIGN KEY(actor_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )

        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS timers (
              user_id INTEGER PRIMARY KEY,
              project_id INTEGER NOT NULL,
              task_id INTEGER,
              note TEXT NOT NULL DEFAULT '',
              started_at INTEGER NOT NULL,
              FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
              FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE RESTRICT,
              FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE SET NULL
            )
            """
        )

        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS attendance_logs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              user_id INTEGER NOT NULL,
              work_date TEXT NOT NULL,
              clock_in_at INTEGER,
              clock_out_at INTEGER,
              note TEXT NOT NULL DEFAULT '',
              created_at INTEGER NOT NULL,
              updated_at INTEGER NOT NULL,
              updated_by_user_id INTEGER,
              UNIQUE(user_id, work_date),
              FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
              FOREIGN KEY(updated_by_user_id) REFERENCES users(id) ON DELETE SET NULL
            )
            """
        )

        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS attendance_audit_logs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              attendance_id INTEGER NOT NULL,
              actor_user_id INTEGER NOT NULL,
              action TEXT NOT NULL,
              reason TEXT NOT NULL DEFAULT '',
              before_json TEXT NOT NULL DEFAULT '{}',
              after_json TEXT NOT NULL DEFAULT '{}',
              created_at INTEGER NOT NULL,
              FOREIGN KEY(attendance_id) REFERENCES attendance_logs(id) ON DELETE CASCADE,
              FOREIGN KEY(actor_user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )

        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_entries_user_date ON time_entries(user_id, work_date)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_entries_project_date ON time_entries(project_id, work_date)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_entries_status_date ON time_entries(status, work_date)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_project ON tasks(project_id)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_attendance_user_date ON attendance_logs(user_id, work_date)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_attendance_date ON attendance_logs(work_date)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_attendance_audit_attendance ON attendance_audit_logs(attendance_id)")
        if version < SCHEMA_VERSION:
            self.set_setting("schema_version", str(SCHEMA_VERSION))
        self._conn.commit()

        self.ensure_bootstrap_admin()
        self.ensure_seed_data()

    def set_setting(self, key: str, value: str) -> None:
        self._conn.execute(
            """
            INSERT INTO settings(key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (key, value),
        )
        self._conn.commit()

    def get_setting(self, key: str, default: str = "") -> str:
        row = self._conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        if not row:
            return default
        value = row["value"]
        if value is None:
            return default
        return str(value)

    def get_setting_int(self, key: str, default: int = 0) -> int:
        try:
            return int(self.get_setting(key, str(default)))
        except ValueError:
            return default

    def ensure_bootstrap_admin(self) -> None:
        user = (os.getenv("CLOUDLOG_ADMIN_USER", "admin") or "admin").strip()
        password = (os.getenv("CLOUDLOG_ADMIN_PASSWORD", "admin1234") or "admin1234").strip()

        row = self._conn.execute("SELECT id FROM users WHERE username=?", (user,)).fetchone()
        if row:
            return

        ts = now_ts()
        self._conn.execute(
            """
            INSERT INTO users(username, password_hash, role, hourly_cost, active, created_at, updated_at)
            VALUES (?, ?, ?, ?, 1, ?, ?)
            """,
            (user, hash_password(password), ROLE_ADMIN, 0.0, ts, ts),
        )
        self._conn.commit()

    def ensure_seed_data(self) -> None:
        c = self._conn.execute("SELECT COUNT(*) AS c FROM clients").fetchone()
        p = self._conn.execute("SELECT COUNT(*) AS c FROM projects").fetchone()
        if int(c["c"] or 0) > 0 or int(p["c"] or 0) > 0:
            return

        ts = now_ts()
        self._conn.execute(
            "INSERT INTO clients(name, created_at, updated_at) VALUES (?, ?, ?)",
            ("サンプル顧客", ts, ts),
        )
        client_id = int(self._conn.execute("SELECT id FROM clients WHERE name='サンプル顧客'").fetchone()["id"])
        self._conn.execute(
            """
            INSERT INTO projects(client_id, name, code, description, status, budget_hours, budget_cost, bill_rate, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'active', ?, ?, ?, ?, ?)
            """,
            (client_id, "導入プロジェクト", "DEMO-001", "初期セットアップ", 120.0, 600000.0, 12000.0, ts, ts),
        )
        project_id = int(self._conn.execute("SELECT id FROM projects WHERE code='DEMO-001'").fetchone()["id"])
        self._conn.execute(
            "INSERT INTO tasks(project_id, name, status, created_at, updated_at) VALUES (?, ?, 'active', ?, ?)",
            (project_id, "要件整理", ts, ts),
        )
        self._conn.execute(
            "INSERT INTO tasks(project_id, name, status, created_at, updated_at) VALUES (?, ?, 'active', ?, ?)",
            (project_id, "実装", ts, ts),
        )
        self._conn.commit()

    def _to_user(self, row: sqlite3.Row) -> UserRow:
        return UserRow(
            id=int(row["id"]),
            username=str(row["username"]),
            role=str(row["role"]),
            hourly_cost=float(row["hourly_cost"] or 0),
            active=bool(row["active"]),
            created_at=int(row["created_at"]),
            updated_at=int(row["updated_at"]),
        )

    def _to_user_auth(self, row: sqlite3.Row) -> UserAuthRow:
        return UserAuthRow(
            id=int(row["id"]),
            username=str(row["username"]),
            role=str(row["role"]),
            password_hash=str(row["password_hash"]),
            active=bool(row["active"]),
        )

    def _to_attendance(self, row: sqlite3.Row) -> AttendanceRow:
        return AttendanceRow(
            id=int(row["id"]),
            user_id=int(row["user_id"]),
            username=str(row["username"]),
            work_date=str(row["work_date"]),
            clock_in_at=int(row["clock_in_at"]) if row["clock_in_at"] is not None else None,
            clock_out_at=int(row["clock_out_at"]) if row["clock_out_at"] is not None else None,
            note=str(row["note"] or ""),
            updated_by_user_id=int(row["updated_by_user_id"]) if row["updated_by_user_id"] is not None else None,
            updated_by_username=str(row["updated_by_username"]) if row["updated_by_username"] is not None else None,
            created_at=int(row["created_at"]),
            updated_at=int(row["updated_at"]),
        )

    def get_user(self, user_id: int) -> UserRow | None:
        row = self._conn.execute("SELECT * FROM users WHERE id=?", (int(user_id),)).fetchone()
        if not row:
            return None
        return self._to_user(row)

    def get_user_by_name(self, username: str) -> UserRow | None:
        row = self._conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
        if not row:
            return None
        return self._to_user(row)

    def get_user_auth(self, username: str) -> UserAuthRow | None:
        row = self._conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
        if not row:
            return None
        return self._to_user_auth(row)

    def list_users(self, *, active_only: bool = True) -> list[UserRow]:
        if active_only:
            rows = self._conn.execute("SELECT * FROM users WHERE active=1 ORDER BY username").fetchall()
        else:
            rows = self._conn.execute("SELECT * FROM users ORDER BY username").fetchall()
        return [self._to_user(row) for row in rows]

    def create_user(self, *, username: str, password_hash: str, role: str = ROLE_MEMBER, hourly_cost: float = 0.0) -> int:
        ts = now_ts()
        cur = self._conn.execute(
            """
            INSERT INTO users(username, password_hash, role, hourly_cost, active, created_at, updated_at)
            VALUES (?, ?, ?, ?, 1, ?, ?)
            """,
            (username, password_hash, role, float(hourly_cost), ts, ts),
        )
        self._conn.commit()
        return int(cur.lastrowid)

    def update_user_role_and_cost(self, user_id: int, *, role: str, hourly_cost: float) -> None:
        ts = now_ts()
        self._conn.execute(
            "UPDATE users SET role=?, hourly_cost=?, updated_at=? WHERE id=?",
            (role, float(hourly_cost), ts, int(user_id)),
        )
        self._conn.commit()

    def set_user_password(self, *, username: str, new_password: str) -> bool:
        ts = now_ts()
        cur = self._conn.execute(
            "UPDATE users SET password_hash=?, updated_at=? WHERE username=?",
            (hash_password(new_password), ts, username),
        )
        self._conn.commit()
        return int(cur.rowcount or 0) > 0

    def list_clients(self) -> list[ClientRow]:
        rows = self._conn.execute("SELECT * FROM clients ORDER BY name").fetchall()
        out: list[ClientRow] = []
        for row in rows:
            out.append(
                ClientRow(
                    id=int(row["id"]),
                    name=str(row["name"]),
                    created_at=int(row["created_at"]),
                    updated_at=int(row["updated_at"]),
                )
            )
        return out

    def create_client(self, name: str) -> int:
        ts = now_ts()
        cur = self._conn.execute(
            "INSERT INTO clients(name, created_at, updated_at) VALUES (?, ?, ?)",
            (name, ts, ts),
        )
        self._conn.commit()
        return int(cur.lastrowid)

    def list_projects(self, *, include_archived: bool = True) -> list[ProjectRow]:
        sql = """
            SELECT p.*, c.name AS client_name
            FROM projects p
            LEFT JOIN clients c ON c.id=p.client_id
        """
        args: tuple[Any, ...] = ()
        if not include_archived:
            sql += " WHERE p.status='active'"
        sql += " ORDER BY p.code"
        rows = self._conn.execute(sql, args).fetchall()
        out: list[ProjectRow] = []
        for row in rows:
            out.append(
                ProjectRow(
                    id=int(row["id"]),
                    client_id=int(row["client_id"]) if row["client_id"] is not None else None,
                    client_name=str(row["client_name"]) if row["client_name"] is not None else None,
                    name=str(row["name"]),
                    code=str(row["code"]),
                    description=str(row["description"] or ""),
                    status=str(row["status"]),
                    budget_hours=float(row["budget_hours"] or 0),
                    budget_cost=float(row["budget_cost"] or 0),
                    bill_rate=float(row["bill_rate"] or 0),
                    start_date=str(row["start_date"]) if row["start_date"] else None,
                    end_date=str(row["end_date"]) if row["end_date"] else None,
                    created_at=int(row["created_at"]),
                    updated_at=int(row["updated_at"]),
                )
            )
        return out

    def get_project(self, project_id: int) -> ProjectRow | None:
        rows = self.list_projects(include_archived=True)
        for p in rows:
            if p.id == int(project_id):
                return p
        return None

    def get_project_by_code_or_name(self, key: str) -> ProjectRow | None:
        row = self._conn.execute(
            """
            SELECT p.*, c.name AS client_name
            FROM projects p
            LEFT JOIN clients c ON c.id=p.client_id
            WHERE p.code=? OR p.name=?
            LIMIT 1
            """,
            (key, key),
        ).fetchone()
        if not row:
            return None
        return ProjectRow(
            id=int(row["id"]),
            client_id=int(row["client_id"]) if row["client_id"] is not None else None,
            client_name=str(row["client_name"]) if row["client_name"] is not None else None,
            name=str(row["name"]),
            code=str(row["code"]),
            description=str(row["description"] or ""),
            status=str(row["status"]),
            budget_hours=float(row["budget_hours"] or 0),
            budget_cost=float(row["budget_cost"] or 0),
            bill_rate=float(row["bill_rate"] or 0),
            start_date=str(row["start_date"]) if row["start_date"] else None,
            end_date=str(row["end_date"]) if row["end_date"] else None,
            created_at=int(row["created_at"]),
            updated_at=int(row["updated_at"]),
        )

    def create_project(
        self,
        *,
        client_id: int | None,
        name: str,
        code: str,
        description: str,
        budget_hours: float,
        budget_cost: float,
        bill_rate: float,
        start_date: str | None,
        end_date: str | None,
    ) -> int:
        ts = now_ts()
        cur = self._conn.execute(
            """
            INSERT INTO projects(
              client_id, name, code, description, status,
              budget_hours, budget_cost, bill_rate, start_date, end_date,
              created_at, updated_at
            ) VALUES (?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(client_id) if client_id else None,
                name,
                code,
                description,
                float(budget_hours),
                float(budget_cost),
                float(bill_rate),
                start_date,
                end_date,
                ts,
                ts,
            ),
        )
        self._conn.commit()
        return int(cur.lastrowid)

    def update_project_status(self, project_id: int, status: str) -> None:
        ts = now_ts()
        self._conn.execute(
            "UPDATE projects SET status=?, updated_at=? WHERE id=?",
            (status, ts, int(project_id)),
        )
        self._conn.commit()

    def list_tasks(self, *, project_id: int | None = None, active_only: bool = True) -> list[TaskRow]:
        sql = """
            SELECT t.*, p.name AS project_name, p.code AS project_code
            FROM tasks t
            JOIN projects p ON p.id=t.project_id
        """
        args: list[Any] = []
        clauses: list[str] = []
        if project_id is not None:
            clauses.append("t.project_id=?")
            args.append(int(project_id))
        if active_only:
            clauses.append("t.status='active'")
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY p.code, t.name"
        rows = self._conn.execute(sql, tuple(args)).fetchall()
        out: list[TaskRow] = []
        for row in rows:
            out.append(
                TaskRow(
                    id=int(row["id"]),
                    project_id=int(row["project_id"]),
                    project_name=str(row["project_name"]),
                    project_code=str(row["project_code"]),
                    name=str(row["name"]),
                    status=str(row["status"]),
                    created_at=int(row["created_at"]),
                    updated_at=int(row["updated_at"]),
                )
            )
        return out

    def create_task(self, *, project_id: int, name: str) -> int:
        ts = now_ts()
        cur = self._conn.execute(
            "INSERT INTO tasks(project_id, name, status, created_at, updated_at) VALUES (?, ?, 'active', ?, ?)",
            (int(project_id), name, ts, ts),
        )
        self._conn.commit()
        return int(cur.lastrowid)

    def get_entry(self, entry_id: int) -> EntryRow | None:
        rows = self.list_entries(entry_id=int(entry_id))
        if rows:
            return rows[0]
        return None

    def list_entries(
        self,
        *,
        entry_id: int | None = None,
        user_id: int | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
        status: str | None = None,
        include_rejected: bool = True,
    ) -> list[EntryRow]:
        sql = """
            SELECT e.*,
                   u.username,
                   p.name AS project_name,
                   p.code AS project_code,
                   t.name AS task_name,
                   au.username AS approver_name
            FROM time_entries e
            JOIN users u ON u.id=e.user_id
            JOIN projects p ON p.id=e.project_id
            LEFT JOIN tasks t ON t.id=e.task_id
            LEFT JOIN users au ON au.id=e.approver_id
        """
        args: list[Any] = []
        clauses: list[str] = []
        if entry_id is not None:
            clauses.append("e.id=?")
            args.append(int(entry_id))
        if user_id is not None:
            clauses.append("e.user_id=?")
            args.append(int(user_id))
        if from_date:
            clauses.append("e.work_date>=?")
            args.append(from_date)
        if to_date:
            clauses.append("e.work_date<=?")
            args.append(to_date)
        if status:
            clauses.append("e.status=?")
            args.append(status)
        if not include_rejected:
            clauses.append("e.status!='rejected'")
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY e.work_date DESC, e.id DESC"

        rows = self._conn.execute(sql, tuple(args)).fetchall()
        out: list[EntryRow] = []
        for row in rows:
            out.append(
                EntryRow(
                    id=int(row["id"]),
                    user_id=int(row["user_id"]),
                    username=str(row["username"]),
                    project_id=int(row["project_id"]),
                    project_name=str(row["project_name"]),
                    project_code=str(row["project_code"]),
                    task_id=int(row["task_id"]) if row["task_id"] is not None else None,
                    task_name=str(row["task_name"]) if row["task_name"] is not None else None,
                    work_date=str(row["work_date"]),
                    minutes=int(row["minutes"]),
                    note=str(row["note"] or ""),
                    status=str(row["status"]),
                    approver_id=int(row["approver_id"]) if row["approver_id"] is not None else None,
                    approver_name=str(row["approver_name"]) if row["approver_name"] is not None else None,
                    approved_at=int(row["approved_at"]) if row["approved_at"] is not None else None,
                    reject_reason=str(row["reject_reason"]) if row["reject_reason"] else None,
                    created_at=int(row["created_at"]),
                    updated_at=int(row["updated_at"]),
                )
            )
        return out

    def create_entry(
        self,
        *,
        user_id: int,
        project_id: int,
        task_id: int | None,
        work_date: str,
        minutes: int,
        note: str,
        status: str = STATUS_DRAFT,
    ) -> int:
        ts = now_ts()
        cur = self._conn.execute(
            """
            INSERT INTO time_entries(
              user_id, project_id, task_id, work_date, minutes, note,
              status, approver_id, approved_at, reject_reason, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?)
            """,
            (int(user_id), int(project_id), int(task_id) if task_id else None, work_date, int(minutes), note, status, ts, ts),
        )
        entry_id = int(cur.lastrowid)
        self._add_history(entry_id, int(user_id), "create", note="")
        self._conn.commit()
        return entry_id

    def update_entry(
        self,
        *,
        entry_id: int,
        project_id: int,
        task_id: int | None,
        work_date: str,
        minutes: int,
        note: str,
    ) -> None:
        ts = now_ts()
        self._conn.execute(
            """
            UPDATE time_entries
            SET project_id=?, task_id=?, work_date=?, minutes=?, note=?,
                status=CASE WHEN status='rejected' THEN 'draft' ELSE status END,
                approver_id=CASE WHEN status='rejected' THEN NULL ELSE approver_id END,
                approved_at=CASE WHEN status='rejected' THEN NULL ELSE approved_at END,
                reject_reason=CASE WHEN status='rejected' THEN NULL ELSE reject_reason END,
                updated_at=?
            WHERE id=?
            """,
            (int(project_id), int(task_id) if task_id else None, work_date, int(minutes), note, ts, int(entry_id)),
        )
        self._conn.commit()

    def delete_entry(self, entry_id: int) -> None:
        self._conn.execute("DELETE FROM time_entries WHERE id=?", (int(entry_id),))
        self._conn.commit()

    def submit_entries(self, *, user_id: int, from_date: str, to_date: str) -> int:
        ts = now_ts()
        cur = self._conn.execute(
            """
            UPDATE time_entries
            SET status='submitted', updated_at=?, reject_reason=NULL, approver_id=NULL, approved_at=NULL
            WHERE user_id=? AND work_date>=? AND work_date<=? AND status IN ('draft', 'rejected')
            """,
            (ts, int(user_id), from_date, to_date),
        )
        self._conn.commit()
        return int(cur.rowcount or 0)

    def approve_entry(self, *, entry_id: int, approver_id: int) -> None:
        ts = now_ts()
        self._conn.execute(
            """
            UPDATE time_entries
            SET status='approved', approver_id=?, approved_at=?, reject_reason=NULL, updated_at=?
            WHERE id=? AND status='submitted'
            """,
            (int(approver_id), ts, ts, int(entry_id)),
        )
        self._add_history(entry_id, approver_id, "approve", note="")
        self._conn.commit()

    def reject_entry(self, *, entry_id: int, approver_id: int, reason: str) -> None:
        ts = now_ts()
        self._conn.execute(
            """
            UPDATE time_entries
            SET status='rejected', approver_id=?, approved_at=NULL, reject_reason=?, updated_at=?
            WHERE id=? AND status='submitted'
            """,
            (int(approver_id), reason, ts, int(entry_id)),
        )
        self._add_history(entry_id, approver_id, "reject", note=reason)
        self._conn.commit()

    def _add_history(self, entry_id: int, actor_id: int, action: str, note: str) -> None:
        self._conn.execute(
            "INSERT INTO entry_history(entry_id, actor_id, action, note, created_at) VALUES (?, ?, ?, ?, ?)",
            (int(entry_id), int(actor_id), action, note, now_ts()),
        )

    def start_timer(self, *, user_id: int, project_id: int, task_id: int | None, note: str) -> None:
        self._conn.execute(
            """
            INSERT INTO timers(user_id, project_id, task_id, note, started_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(user_id)
            DO UPDATE SET project_id=excluded.project_id, task_id=excluded.task_id, note=excluded.note, started_at=excluded.started_at
            """,
            (int(user_id), int(project_id), int(task_id) if task_id else None, note, now_ts()),
        )
        self._conn.commit()

    def stop_timer(self, *, user_id: int) -> TimerRow | None:
        row = self._conn.execute("SELECT * FROM timers WHERE user_id=?", (int(user_id),)).fetchone()
        if not row:
            return None
        self._conn.execute("DELETE FROM timers WHERE user_id=?", (int(user_id),))
        self._conn.commit()
        return TimerRow(
            user_id=int(row["user_id"]),
            project_id=int(row["project_id"]),
            task_id=int(row["task_id"]) if row["task_id"] is not None else None,
            note=str(row["note"] or ""),
            started_at=int(row["started_at"]),
        )

    def get_timer(self, *, user_id: int) -> TimerRow | None:
        row = self._conn.execute("SELECT * FROM timers WHERE user_id=?", (int(user_id),)).fetchone()
        if not row:
            return None
        return TimerRow(
            user_id=int(row["user_id"]),
            project_id=int(row["project_id"]),
            task_id=int(row["task_id"]) if row["task_id"] is not None else None,
            note=str(row["note"] or ""),
            started_at=int(row["started_at"]),
        )

    def _attendance_snapshot(self, row: AttendanceRow | sqlite3.Row | None) -> dict[str, Any]:
        if row is None:
            return {}
        if isinstance(row, AttendanceRow):
            return {
                "id": row.id,
                "user_id": row.user_id,
                "work_date": row.work_date,
                "clock_in_at": row.clock_in_at,
                "clock_out_at": row.clock_out_at,
                "note": row.note,
                "updated_by_user_id": row.updated_by_user_id,
            }
        return {
            "id": int(row["id"]),
            "user_id": int(row["user_id"]),
            "work_date": str(row["work_date"]),
            "clock_in_at": int(row["clock_in_at"]) if row["clock_in_at"] is not None else None,
            "clock_out_at": int(row["clock_out_at"]) if row["clock_out_at"] is not None else None,
            "note": str(row["note"] or ""),
            "updated_by_user_id": int(row["updated_by_user_id"]) if row["updated_by_user_id"] is not None else None,
        }

    def _add_attendance_audit(
        self,
        *,
        attendance_id: int,
        actor_user_id: int,
        action: str,
        reason: str,
        before: dict[str, Any],
        after: dict[str, Any],
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO attendance_audit_logs(
              attendance_id, actor_user_id, action, reason, before_json, after_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(attendance_id),
                int(actor_user_id),
                action,
                reason,
                json.dumps(before, ensure_ascii=False, separators=(",", ":")),
                json.dumps(after, ensure_ascii=False, separators=(",", ":")),
                now_ts(),
            ),
        )

    def get_attendance(self, *, attendance_id: int) -> AttendanceRow | None:
        row = self._conn.execute(
            """
            SELECT a.*, u.username, uu.username AS updated_by_username
            FROM attendance_logs a
            JOIN users u ON u.id=a.user_id
            LEFT JOIN users uu ON uu.id=a.updated_by_user_id
            WHERE a.id=?
            LIMIT 1
            """,
            (int(attendance_id),),
        ).fetchone()
        if not row:
            return None
        return self._to_attendance(row)

    def get_attendance_by_user_date(self, *, user_id: int, work_date: str) -> AttendanceRow | None:
        row = self._conn.execute(
            """
            SELECT a.*, u.username, uu.username AS updated_by_username
            FROM attendance_logs a
            JOIN users u ON u.id=a.user_id
            LEFT JOIN users uu ON uu.id=a.updated_by_user_id
            WHERE a.user_id=? AND a.work_date=?
            LIMIT 1
            """,
            (int(user_id), work_date),
        ).fetchone()
        if not row:
            return None
        return self._to_attendance(row)

    def clock_in(self, *, user_id: int, work_date: str, at_ts: int) -> AttendanceRow:
        row = self._conn.execute(
            "SELECT * FROM attendance_logs WHERE user_id=? AND work_date=? LIMIT 1",
            (int(user_id), work_date),
        ).fetchone()
        if row and row["clock_in_at"] is not None:
            raise ValueError("already_clocked_in")
        if row and row["clock_out_at"] is not None:
            raise ValueError("already_clocked_out")

        before = self._attendance_snapshot(row)
        ts = now_ts()
        if row is None:
            cur = self._conn.execute(
                """
                INSERT INTO attendance_logs(
                  user_id, work_date, clock_in_at, clock_out_at, note,
                  created_at, updated_at, updated_by_user_id
                ) VALUES (?, ?, ?, NULL, '', ?, ?, NULL)
                """,
                (int(user_id), work_date, int(at_ts), ts, ts),
            )
            attendance_id = int(cur.lastrowid)
        else:
            attendance_id = int(row["id"])
            self._conn.execute(
                """
                UPDATE attendance_logs
                SET clock_in_at=?, updated_at=?, updated_by_user_id=NULL
                WHERE id=?
                """,
                (int(at_ts), ts, attendance_id),
            )

        attendance = self.get_attendance(attendance_id=attendance_id)
        if attendance is None:
            raise RuntimeError("attendance_not_found_after_clock_in")
        self._add_attendance_audit(
            attendance_id=attendance.id,
            actor_user_id=int(user_id),
            action="clock_in",
            reason="",
            before=before,
            after=self._attendance_snapshot(attendance),
        )
        self._conn.commit()
        return attendance

    def clock_out(self, *, user_id: int, work_date: str, at_ts: int) -> AttendanceRow:
        row = self._conn.execute(
            "SELECT * FROM attendance_logs WHERE user_id=? AND work_date=? LIMIT 1",
            (int(user_id), work_date),
        ).fetchone()
        if row is None or row["clock_in_at"] is None:
            raise ValueError("clock_in_required")
        if row["clock_out_at"] is not None:
            raise ValueError("already_clocked_out")

        before = self._attendance_snapshot(row)
        attendance_id = int(row["id"])
        ts = now_ts()
        self._conn.execute(
            """
            UPDATE attendance_logs
            SET clock_out_at=?, updated_at=?, updated_by_user_id=NULL
            WHERE id=?
            """,
            (int(at_ts), ts, attendance_id),
        )
        attendance = self.get_attendance(attendance_id=attendance_id)
        if attendance is None:
            raise RuntimeError("attendance_not_found_after_clock_out")
        self._add_attendance_audit(
            attendance_id=attendance.id,
            actor_user_id=int(user_id),
            action="clock_out",
            reason="",
            before=before,
            after=self._attendance_snapshot(attendance),
        )
        self._conn.commit()
        return attendance

    def list_attendance(
        self,
        *,
        user_id: int | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> list[AttendanceRow]:
        sql = """
            SELECT a.*, u.username, uu.username AS updated_by_username
            FROM attendance_logs a
            JOIN users u ON u.id=a.user_id
            LEFT JOIN users uu ON uu.id=a.updated_by_user_id
        """
        clauses: list[str] = []
        args: list[Any] = []
        if user_id is not None:
            clauses.append("a.user_id=?")
            args.append(int(user_id))
        if from_date:
            clauses.append("a.work_date>=?")
            args.append(from_date)
        if to_date:
            clauses.append("a.work_date<=?")
            args.append(to_date)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY a.work_date DESC, u.username ASC, a.id DESC"
        rows = self._conn.execute(sql, tuple(args)).fetchall()
        return [self._to_attendance(row) for row in rows]

    def attendance_summary(self, *, user_id: int, from_date: str, to_date: str) -> dict[str, Any]:
        row = self._conn.execute(
            """
            SELECT
              COALESCE(SUM(
                CASE
                  WHEN clock_in_at IS NOT NULL AND clock_out_at IS NOT NULL AND clock_out_at >= clock_in_at
                  THEN (clock_out_at - clock_in_at)
                  ELSE 0
                END
              ), 0) AS total_seconds,
              COALESCE(SUM(CASE WHEN clock_in_at IS NOT NULL THEN 1 ELSE 0 END), 0) AS worked_days
            FROM attendance_logs
            WHERE user_id=? AND work_date>=? AND work_date<=?
            """,
            (int(user_id), from_date, to_date),
        ).fetchone()
        return {
            "total_seconds": int(row["total_seconds"] or 0),
            "worked_days": int(row["worked_days"] or 0),
        }

    def admin_update_attendance(
        self,
        *,
        attendance_id: int,
        actor_user_id: int,
        clock_in_at: int | None,
        clock_out_at: int | None,
        note: str,
        reason: str,
    ) -> AttendanceRow:
        if not str(reason or "").strip():
            raise ValueError("reason_required")
        if clock_in_at is None and clock_out_at is not None:
            raise ValueError("clock_in_required")
        if clock_in_at is not None and clock_out_at is not None and int(clock_out_at) < int(clock_in_at):
            raise ValueError("clock_out_must_be_after_clock_in")

        row = self._conn.execute("SELECT * FROM attendance_logs WHERE id=? LIMIT 1", (int(attendance_id),)).fetchone()
        if row is None:
            raise ValueError("attendance_not_found")

        before = self._attendance_snapshot(row)
        ts = now_ts()
        self._conn.execute(
            """
            UPDATE attendance_logs
            SET clock_in_at=?, clock_out_at=?, note=?, updated_by_user_id=?, updated_at=?
            WHERE id=?
            """,
            (
                int(clock_in_at) if clock_in_at is not None else None,
                int(clock_out_at) if clock_out_at is not None else None,
                note,
                int(actor_user_id),
                ts,
                int(attendance_id),
            ),
        )
        attendance = self.get_attendance(attendance_id=int(attendance_id))
        if attendance is None:
            raise RuntimeError("attendance_not_found_after_update")
        self._add_attendance_audit(
            attendance_id=attendance.id,
            actor_user_id=int(actor_user_id),
            action="admin_update",
            reason=reason,
            before=before,
            after=self._attendance_snapshot(attendance),
        )
        self._conn.commit()
        return attendance

    def project_report(self, *, from_date: str, to_date: str, user_id: int | None = None) -> list[dict[str, Any]]:
        sql = """
            SELECT
              p.id AS project_id,
              p.code AS project_code,
              p.name AS project_name,
              p.budget_hours,
              p.budget_cost,
              p.bill_rate,
              COALESCE(SUM(e.minutes), 0) AS total_minutes,
              COALESCE(SUM((e.minutes / 60.0) * u.hourly_cost), 0) AS total_cost
            FROM projects p
            LEFT JOIN time_entries e
              ON e.project_id=p.id
             AND e.work_date>=?
             AND e.work_date<=?
             AND e.status IN ('submitted','approved')
            LEFT JOIN users u ON u.id=e.user_id
        """
        args: list[Any] = [from_date, to_date]
        if user_id is not None:
            sql += " AND e.user_id=?"
            args.append(int(user_id))
        sql += " GROUP BY p.id ORDER BY p.code"

        rows = self._conn.execute(sql, tuple(args)).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            hours = float(row["total_minutes"] or 0) / 60.0
            cost = float(row["total_cost"] or 0)
            revenue = hours * float(row["bill_rate"] or 0)
            out.append(
                {
                    "project_id": int(row["project_id"]),
                    "project_code": str(row["project_code"]),
                    "project_name": str(row["project_name"]),
                    "budget_hours": float(row["budget_hours"] or 0),
                    "budget_cost": float(row["budget_cost"] or 0),
                    "bill_rate": float(row["bill_rate"] or 0),
                    "actual_hours": hours,
                    "actual_cost": cost,
                    "actual_revenue": revenue,
                    "profit": revenue - cost,
                }
            )
        return out

    def user_report(self, *, from_date: str, to_date: str) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT
              u.id AS user_id,
              u.username,
              u.role,
              u.hourly_cost,
              COALESCE(SUM(e.minutes), 0) AS total_minutes,
              COALESCE(SUM(CASE WHEN e.status='approved' THEN e.minutes ELSE 0 END), 0) AS approved_minutes,
              COALESCE(SUM(CASE WHEN e.status='submitted' THEN e.minutes ELSE 0 END), 0) AS submitted_minutes,
              COALESCE(SUM(CASE WHEN e.status='draft' THEN e.minutes ELSE 0 END), 0) AS draft_minutes,
              COALESCE(SUM(CASE WHEN e.status='rejected' THEN e.minutes ELSE 0 END), 0) AS rejected_minutes
            FROM users u
            LEFT JOIN time_entries e
              ON e.user_id=u.id
             AND e.work_date>=?
             AND e.work_date<=?
            WHERE u.active=1
            GROUP BY u.id
            ORDER BY u.username
            """,
            (from_date, to_date),
        ).fetchall()

        out: list[dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "user_id": int(row["user_id"]),
                    "username": str(row["username"]),
                    "role": str(row["role"]),
                    "hourly_cost": float(row["hourly_cost"] or 0),
                    "total_hours": float(row["total_minutes"] or 0) / 60.0,
                    "approved_hours": float(row["approved_minutes"] or 0) / 60.0,
                    "submitted_hours": float(row["submitted_minutes"] or 0) / 60.0,
                    "draft_hours": float(row["draft_minutes"] or 0) / 60.0,
                    "rejected_hours": float(row["rejected_minutes"] or 0) / 60.0,
                }
            )
        return out

    def dashboard_totals(self, *, from_date: str, to_date: str, user_id: int | None = None) -> dict[str, Any]:
        sql = """
            SELECT
              COALESCE(SUM(minutes), 0) AS all_minutes,
              COALESCE(SUM(CASE WHEN status='draft' THEN minutes ELSE 0 END), 0) AS draft_minutes,
              COALESCE(SUM(CASE WHEN status='submitted' THEN minutes ELSE 0 END), 0) AS submitted_minutes,
              COALESCE(SUM(CASE WHEN status='approved' THEN minutes ELSE 0 END), 0) AS approved_minutes,
              COALESCE(SUM(CASE WHEN status='rejected' THEN minutes ELSE 0 END), 0) AS rejected_minutes
            FROM time_entries
            WHERE work_date>=? AND work_date<=?
        """
        args: list[Any] = [from_date, to_date]
        if user_id is not None:
            sql += " AND user_id=?"
            args.append(int(user_id))
        row = self._conn.execute(sql, tuple(args)).fetchone()
        pending_approvals = self._conn.execute("SELECT COUNT(*) AS c FROM time_entries WHERE status='submitted'").fetchone()

        def to_hours(v: Any) -> float:
            return float(v or 0) / 60.0

        return {
            "total_hours": to_hours(row["all_minutes"]),
            "draft_hours": to_hours(row["draft_minutes"]),
            "submitted_hours": to_hours(row["submitted_minutes"]),
            "approved_hours": to_hours(row["approved_minutes"]),
            "rejected_hours": to_hours(row["rejected_minutes"]),
            "pending_approvals": int(pending_approvals["c"] or 0),
        }

    def submission_status_list(self, *, from_date: str, to_date: str) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT
              u.id,
              u.username,
              COALESCE(SUM(CASE WHEN e.status='draft' THEN e.minutes ELSE 0 END), 0) AS draft_minutes,
              COALESCE(SUM(CASE WHEN e.status='submitted' THEN e.minutes ELSE 0 END), 0) AS submitted_minutes,
              COALESCE(SUM(CASE WHEN e.status='approved' THEN e.minutes ELSE 0 END), 0) AS approved_minutes,
              COALESCE(SUM(CASE WHEN e.status='rejected' THEN e.minutes ELSE 0 END), 0) AS rejected_minutes
            FROM users u
            LEFT JOIN time_entries e
              ON e.user_id=u.id
             AND e.work_date>=?
             AND e.work_date<=?
            WHERE u.active=1
            GROUP BY u.id, u.username
            ORDER BY u.username
            """,
            (from_date, to_date),
        ).fetchall()

        out: list[dict[str, Any]] = []
        for row in rows:
            draft_h = float(row["draft_minutes"] or 0) / 60.0
            submitted_h = float(row["submitted_minutes"] or 0) / 60.0
            approved_h = float(row["approved_minutes"] or 0) / 60.0
            rejected_h = float(row["rejected_minutes"] or 0) / 60.0
            out.append(
                {
                    "user_id": int(row["id"]),
                    "username": str(row["username"]),
                    "draft_hours": draft_h,
                    "submitted_hours": submitted_h,
                    "approved_hours": approved_h,
                    "rejected_hours": rejected_h,
                    "ready_for_approval": submitted_h > 0 and draft_h == 0,
                }
            )
        return out

    def copy_entries(self, *, user_id: int, source_date: str, target_date: str) -> int:
        rows = self.list_entries(user_id=user_id, from_date=source_date, to_date=source_date)
        copied = 0
        for row in rows:
            self.create_entry(
                user_id=user_id,
                project_id=row.project_id,
                task_id=row.task_id,
                work_date=target_date,
                minutes=row.minutes,
                note=row.note,
                status=STATUS_DRAFT,
            )
            copied += 1
        return copied

    def export_entries(self, *, from_date: str, to_date: str, user_id: int | None = None) -> list[dict[str, Any]]:
        entries = self.list_entries(user_id=user_id, from_date=from_date, to_date=to_date)
        out: list[dict[str, Any]] = []
        for e in entries:
            out.append(
                {
                    "id": e.id,
                    "username": e.username,
                    "date": e.work_date,
                    "project_code": e.project_code,
                    "project": e.project_name,
                    "task": e.task_name or "",
                    "hours": round(e.minutes / 60.0, 2),
                    "status": e.status,
                    "note": e.note,
                    "approver": e.approver_name or "",
                    "reject_reason": e.reject_reason or "",
                }
            )
        return out

    def entries_for_calendar(self, *, user_id: int, from_date: str, to_date: str) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT
              work_date,
              p.code AS project_code,
              p.name AS project_name,
              COALESCE(SUM(e.minutes), 0) AS total_minutes
            FROM time_entries e
            JOIN projects p ON p.id=e.project_id
            WHERE e.user_id=?
              AND e.work_date>=?
              AND e.work_date<=?
              AND e.status IN ('submitted','approved')
            GROUP BY e.work_date, e.project_id
            ORDER BY e.work_date, p.code
            """,
            (int(user_id), from_date, to_date),
        ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "work_date": str(row["work_date"]),
                    "project_code": str(row["project_code"]),
                    "project_name": str(row["project_name"]),
                    "hours": float(row["total_minutes"] or 0) / 60.0,
                }
            )
        return out

    def dump_json(self) -> str:
        data = {
            "users": [u.__dict__ for u in self.list_users(active_only=False)],
            "clients": [c.__dict__ for c in self.list_clients()],
            "projects": [p.__dict__ for p in self.list_projects(include_archived=True)],
            "tasks": [t.__dict__ for t in self.list_tasks(active_only=False)],
            "entries": [e.__dict__ for e in self.list_entries()],
            "attendance": [a.__dict__ for a in self.list_attendance()],
        }
        return json.dumps(data, ensure_ascii=False, separators=(",", ":"))
