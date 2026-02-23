from __future__ import annotations

import base64
import calendar
import json
import logging
import os
import tempfile
import threading
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from sitewatcher.web.auth import hash_password, verify_password

try:
    import gspread  # type: ignore
except Exception:  # pragma: no cover - optional for local tests
    gspread = None

try:
    import holidays as holidays_lib  # type: ignore
except Exception:  # pragma: no cover - optional for local tests
    holidays_lib = None


ROLE_ADMIN = "admin"
ROLE_USER = "user"
ROLE_ORDER = {ROLE_USER: 0, ROLE_ADMIN: 1}

EVENT_IN = "IN"
EVENT_OUT = "OUT"
EVENT_OUTING = "OUTING"
EVENT_RETURN = "RETURN"
EVENT_TYPES = {EVENT_IN, EVENT_OUT, EVENT_OUTING, EVENT_RETURN}

LEAVE_PAID = "PAID"
LEAVE_SPECIAL = "SPECIAL"
LEAVE_COMPANY_DESIGNATED = "COMPANY_DESIGNATED_PAID"
LEAVE_OTHER = "OTHER"
LEAVE_TYPES = {LEAVE_PAID, LEAVE_SPECIAL, LEAVE_COMPANY_DESIGNATED, LEAVE_OTHER}

LEAVE_PENDING = "PENDING"
LEAVE_APPROVED = "APPROVED"
LEAVE_REJECTED = "REJECTED"
LEAVE_STATUSES = {LEAVE_PENDING, LEAVE_APPROVED, LEAVE_REJECTED}

HOLIDAY_PUBLIC = "PUBLIC"
HOLIDAY_COMPANY_CUSTOM = "COMPANY_CUSTOM"

SETTINGS_SINGLETON = "singleton"

JST = timezone(timedelta(hours=9))
JP_WEEKDAYS = ["月", "火", "水", "木", "金", "土", "日"]
DEFAULT_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/11X9JjTC6ntkFb71SCs64pB4vQEh7HtA5gFwbdMbDM4g/edit"

SHEETS_SCHEMA: dict[str, list[str]] = {
    "Users": [
        "user_id",
        "email",
        "name",
        "role",
        "closing_day",
        "is_active",
        "password_hash",
        "created_at",
        "updated_at",
        "last_login_at",
    ],
    "Settings": [
        "settings_id",
        "payroll_cutoff_day",
        "scheduled_start_time",
        "scheduled_end_time",
        "scheduled_work_minutes",
        "grace_minutes",
        "break_policy_type",
        "break_fixed_minutes",
        "break_tier_json",
        "working_weekdays_json",
        "weekend_work_counts_as_holiday_work",
        "paid_leave_approval_mode",
        "special_leave_types_json",
        "company_designated_paid_leave_dates_json",
        "company_custom_holidays_json",
        "holiday_source",
        "holiday_cache_updated_at",
        "updated_by_user_id",
        "updated_at",
    ],
    "Events": [
        "event_id",
        "user_id",
        "event_type",
        "event_time",
        "client_time",
        "source",
        "ip",
        "user_agent",
        "note",
        "is_edited",
        "edited_from_event_id",
        "edited_by_user_id",
        "edited_at",
    ],
    "LeaveRequests": [
        "leave_id",
        "user_id",
        "leave_date",
        "leave_type",
        "leave_name",
        "note",
        "status",
        "requested_at",
        "decided_at",
        "decided_by_user_id",
    ],
    "Holidays": [
        "date",
        "name",
        "kind",
        "source",
        "year",
        "fetched_at",
    ],
    "SummaryCache": [
        "cache_id",
        "period_start",
        "period_end",
        "user_id",
        "summary_json",
        "created_at",
    ],
}

OPS_SHEETS_SCHEMA: dict[str, list[str]] = {
    "_meta": [
        "key",
        "value",
        "updated_at",
    ],
    "users": [
        "user_id",
        "name",
        "email",
        "role",
        "closing_day",
        "is_active",
        "created_at",
    ],
    "events": [
        "event_id",
        "user_id",
        "event_type",
        "event_at",
        "source",
        "note",
        "created_at",
        "created_by",
        "is_deleted",
    ],
    "edits": [
        "edit_id",
        "event_id",
        "edited_at",
        "edited_by",
        "before_json",
        "after_json",
        "reason",
    ],
    "daily": [
        "user_id",
        "date",
        "clock_in_at",
        "clock_out_at",
        "break_minutes",
        "work_minutes",
        "overtime_minutes",
        "incomplete_flag",
        "updated_at",
    ],
    "monthly": [
        "user_id",
        "period_start",
        "period_end",
        "work_minutes",
        "overtime_minutes",
        "updated_at",
    ],
    "time_events": [
        "eventId",
        "userId",
        "eventType",
        "timestamp",
        "note",
        "source",
        "createdAt",
        "updatedAt",
    ],
    "time_edits": [
        "editId",
        "eventId",
        "userId",
        "beforeJson",
        "afterJson",
        "reason",
        "editedAt",
    ],
    "settings": [
        "key",
        "value",
        "updatedAt",
    ],
}

ALL_SHEETS_SCHEMA: dict[str, list[str]] = {
    **SHEETS_SCHEMA,
    **OPS_SHEETS_SCHEMA,
}

DEFAULT_SETTINGS: dict[str, Any] = {
    "settings_id": SETTINGS_SINGLETON,
    "payroll_cutoff_day": 20,
    "scheduled_start_time": "08:55",
    "scheduled_end_time": "17:55",
    "scheduled_work_minutes": 480,
    "grace_minutes": 5,
    "break_policy_type": "fixed",
    "break_fixed_minutes": 60,
    "break_tier_json": [
        {"min_work_minutes": 360, "break_minutes": 45},
        {"min_work_minutes": 480, "break_minutes": 60},
    ],
    "working_weekdays_json": [0, 1, 2, 3, 4],
    "weekend_work_counts_as_holiday_work": True,
    "paid_leave_approval_mode": "require_admin",
    "special_leave_types_json": ["慶弔", "特別休暇"],
    "company_designated_paid_leave_dates_json": [],
    "company_custom_holidays_json": [],
    "holiday_source": "holidays-lib-jp",
    "holiday_cache_updated_at": "",
    "updated_by_user_id": "",
    "updated_at": "",
}

DEFAULT_RUNTIME_SETTINGS: dict[str, str] = {
    "timezone": "Asia/Tokyo",
    "closingDay": "20",
    "requiredWorkMinutes": "480",
    "nightStart": "22:00",
    "nightEnd": "05:00",
    "allowMultipleClockInSameDay": "false",
}


def _jst_now() -> datetime:
    return datetime.now(tz=JST)


def _iso_now() -> str:
    return _jst_now().isoformat()


def _as_bool(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "on"}


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return int(default)


def _parse_json_list(value: Any, default: list[Any]) -> list[Any]:
    text = str(value or "").strip()
    if not text:
        return list(default)
    try:
        loaded = json.loads(text)
        if isinstance(loaded, list):
            return loaded
        return list(default)
    except Exception:
        return list(default)


def _parse_date_iso(raw: str) -> date:
    return datetime.fromisoformat(raw).astimezone(JST).date() if "T" in raw else date.fromisoformat(raw)


def _parse_dt_iso(raw: str) -> datetime:
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=JST)
    return dt.astimezone(JST)


def _format_hhmm(minutes: int) -> str:
    m = max(0, int(minutes))
    h, mm = divmod(m, 60)
    return f"{h:02d}:{mm:02d}"


def _time_to_minutes(raw_hhmm: str, default: int) -> int:
    text = str(raw_hhmm or "").strip()
    if ":" not in text:
        return default
    h_s, m_s = text.split(":", 1)
    try:
        return int(h_s) * 60 + int(m_s)
    except Exception:
        return default


def _month_last_day(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def _clamp_day(year: int, month: int, day: int) -> int:
    return min(max(1, day), _month_last_day(year, month))


def _shift_month(year: int, month: int, delta: int) -> tuple[int, int]:
    m = month + delta
    y = year
    while m < 1:
        m += 12
        y -= 1
    while m > 12:
        m -= 12
        y += 1
    return y, m


def _period_for_anchor(anchor: date, cutoff_day: int) -> tuple[date, date]:
    cutoff = min(max(1, int(cutoff_day)), 31)
    if anchor.day <= cutoff:
        end = date(anchor.year, anchor.month, _clamp_day(anchor.year, anchor.month, cutoff))
        py, pm = _shift_month(anchor.year, anchor.month, -1)
        start = date(py, pm, _clamp_day(py, pm, cutoff + 1))
        return start, end

    start = date(anchor.year, anchor.month, _clamp_day(anchor.year, anchor.month, cutoff + 1))
    ny, nm = _shift_month(anchor.year, anchor.month, 1)
    end = date(ny, nm, _clamp_day(ny, nm, cutoff))
    return start, end


def _date_range(start: date, end: date) -> list[date]:
    if end < start:
        return []
    days = (end - start).days
    return [start + timedelta(days=i) for i in range(days + 1)]


def _to_csv_bool(value: bool) -> str:
    return "true" if value else "false"


def _parse_sheet_id(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if "/" not in raw:
        return raw
    try:
        parsed = urlparse(raw)
        parts = [p for p in parsed.path.split("/") if p]
        if "d" in parts:
            idx = parts.index("d")
            if idx + 1 < len(parts):
                return parts[idx + 1]
    except Exception:
        return ""
    return ""


def _normalize_user_closing_day(value: Any, default: int = 20) -> int:
    day = _as_int(value, default)
    return min(max(1, day), 28)


@dataclass(frozen=True)
class AuthUser:
    user_id: str
    email: str
    name: str
    role: str
    is_active: bool
    password_hash: str


class StorageError(RuntimeError):
    pass


class BaseTableGateway:
    def read_rows(self, tab: str) -> list[dict[str, str]]:  # pragma: no cover - interface
        raise NotImplementedError

    def append_row(self, tab: str, row: dict[str, str]) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    def replace_rows(self, tab: str, rows: list[dict[str, str]]) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    def read_tail_rows(self, tab: str, limit: int) -> list[dict[str, str]]:  # pragma: no cover - interface
        raise NotImplementedError


class MemoryTableGateway(BaseTableGateway):
    def __init__(self) -> None:
        self._rows: dict[str, list[dict[str, str]]] = {tab: [] for tab in ALL_SHEETS_SCHEMA}

    def read_rows(self, tab: str) -> list[dict[str, str]]:
        return [dict(row) for row in self._rows[tab]]

    def append_row(self, tab: str, row: dict[str, str]) -> None:
        self._rows[tab].append(dict(row))

    def replace_rows(self, tab: str, rows: list[dict[str, str]]) -> None:
        self._rows[tab] = [dict(r) for r in rows]

    def read_tail_rows(self, tab: str, limit: int) -> list[dict[str, str]]:
        n = max(0, int(limit))
        if n <= 0:
            return []
        return [dict(row) for row in self._rows[tab][-n:]]


class GoogleSheetsGateway(BaseTableGateway):
    def __init__(self, spreadsheet_id: str, credentials_path: str) -> None:
        if gspread is None:
            raise StorageError("gspread is not installed")

        self._client = gspread.service_account(filename=credentials_path)
        self._book = self._client.open_by_key(spreadsheet_id)
        self._ensure_tabs()

    def _ensure_tabs(self) -> None:
        sheets = {ws.title: ws for ws in self._book.worksheets()}
        for tab, headers in ALL_SHEETS_SCHEMA.items():
            ws = sheets.get(tab)
            if ws is None:
                ws = self._book.add_worksheet(title=tab, rows=2000, cols=max(8, len(headers)))
            final_headers = self._ensure_headers(ws, headers)
            self._apply_validations(ws, tab, final_headers)

    def _ensure_headers(self, ws, expected_headers: list[str]) -> list[str]:
        current = [str(h).strip() for h in ws.row_values(1) if str(h).strip()]
        if not current:
            ws.update("A1", [expected_headers])
            return list(expected_headers)

        merged = list(current)
        changed = False
        for h in expected_headers:
            if h not in merged:
                merged.append(h)
                changed = True
        if changed:
            ws.update("A1", [merged])
        return merged

    def _apply_validations(self, ws, tab: str, headers: list[str]) -> None:
        rules: dict[str, list[str]] = {}
        if tab in {"events", "time_events"}:
            rules["event_type"] = [EVENT_IN, EVENT_OUT, EVENT_OUTING, EVENT_RETURN]
            rules["eventType"] = [EVENT_IN, EVENT_OUT, EVENT_OUTING, EVENT_RETURN]
        elif tab == "users":
            rules["role"] = [ROLE_ADMIN, ROLE_USER]

        if not rules:
            return

        requests: list[dict[str, Any]] = []
        for col_name, values in rules.items():
            if col_name not in headers:
                continue
            col = headers.index(col_name)
            requests.append(
                {
                    "setDataValidation": {
                        "range": {
                            "sheetId": ws.id,
                            "startRowIndex": 1,
                            "startColumnIndex": col,
                            "endColumnIndex": col + 1,
                        },
                        "rule": {
                            "condition": {
                                "type": "ONE_OF_LIST",
                                "values": [{"userEnteredValue": value} for value in values],
                            },
                            "strict": True,
                            "showCustomUi": True,
                        },
                    }
                }
            )
        if not requests:
            return
        try:
            self._book.batch_update({"requests": requests})
        except Exception:
            pass

    def _ws(self, tab: str):
        return self._book.worksheet(tab)

    def _column_letter(self, number: int) -> str:
        n = max(1, int(number))
        out = ""
        while n > 0:
            n, rem = divmod(n - 1, 26)
            out = chr(65 + rem) + out
        return out

    def read_rows(self, tab: str) -> list[dict[str, str]]:
        ws = self._ws(tab)
        headers = ALL_SHEETS_SCHEMA[tab]
        values = ws.get_all_values()
        if not values:
            ws.update("A1", [headers])
            return []
        start = 1
        if values[0] == headers:
            start = 1
        rows: list[dict[str, str]] = []
        for line in values[start:]:
            if not any(str(c).strip() for c in line):
                continue
            normalized = list(line) + [""] * (len(headers) - len(line))
            row = {headers[i]: str(normalized[i]) for i in range(len(headers))}
            rows.append(row)
        return rows

    def append_row(self, tab: str, row: dict[str, str]) -> None:
        ws = self._ws(tab)
        headers = ALL_SHEETS_SCHEMA[tab]
        ws.append_row([str(row.get(h, "")) for h in headers], value_input_option="USER_ENTERED")

    def replace_rows(self, tab: str, rows: list[dict[str, str]]) -> None:
        ws = self._ws(tab)
        headers = ALL_SHEETS_SCHEMA[tab]
        payload = [headers] + [[str(r.get(h, "")) for h in headers] for r in rows]
        ws.clear()
        ws.update("A1", payload)

    def read_tail_rows(self, tab: str, limit: int) -> list[dict[str, str]]:
        ws = self._ws(tab)
        headers = ALL_SHEETS_SCHEMA[tab]
        n = max(0, int(limit))
        if n <= 0:
            return []
        start_row = max(2, ws.row_count - n + 1)
        end_row = max(start_row, ws.row_count)
        end_col = self._column_letter(len(headers))
        values = ws.get(f"A{start_row}:{end_col}{end_row}")
        rows: list[dict[str, str]] = []
        for line in values:
            if not any(str(c).strip() for c in line):
                continue
            normalized = list(line) + [""] * (len(headers) - len(line))
            rows.append({headers[i]: str(normalized[i]) for i in range(len(headers))})
        if len(rows) > n:
            rows = rows[-n:]
        return rows


class TimeclockStore:
    def __init__(self, gateway: BaseTableGateway) -> None:
        self._gw = gateway
        self._lock = threading.RLock()
        self._init_defaults()

    @classmethod
    def from_env(cls) -> "TimeclockStore":
        backend = (os.getenv("CLOUDLOG_STORAGE", "google_sheets") or "google_sheets").strip().lower()
        if backend in {"memory", "inmemory"}:
            return cls(MemoryTableGateway())

        spreadsheet_id = _parse_sheet_id(os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID"))
        if not spreadsheet_id:
            spreadsheet_id = _parse_sheet_id(os.getenv("GOOGLE_SHEETS_SPREADSHEET_URL"))
        if not spreadsheet_id:
            spreadsheet_id = _parse_sheet_id(os.getenv("CLOUDLOG_SPREADSHEET_URL"))
        if not spreadsheet_id:
            spreadsheet_id = _parse_sheet_id(DEFAULT_SPREADSHEET_URL)
        if not spreadsheet_id:
            return cls(MemoryTableGateway())
        try:
            credentials_path = _resolve_google_credentials_path()
            gateway = GoogleSheetsGateway(spreadsheet_id=spreadsheet_id, credentials_path=credentials_path)
            return cls(gateway)
        except StorageError as exc:
            logging.getLogger("cloudlog").warning("Google Sheets unavailable (%s). Falling back to memory backend.", exc)
            return cls(MemoryTableGateway())

    def _init_defaults(self) -> None:
        with self._lock:
            settings_rows = self._gw.read_rows("Settings")
            if not settings_rows:
                self._gw.append_row("Settings", self._settings_to_row(DEFAULT_SETTINGS))
            self._ensure_meta_defaults()
            self._ensure_runtime_settings_defaults()
            users = self._gw.read_rows("Users")
            if not users:
                admin_email = (os.getenv("CLOUDLOG_BOOTSTRAP_ADMIN_EMAIL", "admin@example.com") or "admin@example.com").strip().lower()
                admin_name = (os.getenv("CLOUDLOG_BOOTSTRAP_ADMIN_NAME", "Admin") or "Admin").strip()
                admin_password = (os.getenv("CLOUDLOG_BOOTSTRAP_ADMIN_PASSWORD", "ChangeMe123!") or "ChangeMe123!").strip()
                self.create_user(
                    email=admin_email,
                    name=admin_name,
                    password=admin_password,
                    role=ROLE_ADMIN,
                    is_active=True,
                )
            else:
                self._sync_all_users_shadow()
            self.refresh_holidays_cache(force=False)

    def _settings_to_row(self, settings: dict[str, Any]) -> dict[str, str]:
        return {
            "settings_id": SETTINGS_SINGLETON,
            "payroll_cutoff_day": str(settings["payroll_cutoff_day"]),
            "scheduled_start_time": str(settings["scheduled_start_time"]),
            "scheduled_end_time": str(settings["scheduled_end_time"]),
            "scheduled_work_minutes": str(settings["scheduled_work_minutes"]),
            "grace_minutes": str(settings["grace_minutes"]),
            "break_policy_type": str(settings["break_policy_type"]),
            "break_fixed_minutes": str(settings["break_fixed_minutes"]),
            "break_tier_json": json.dumps(settings["break_tier_json"], ensure_ascii=False),
            "working_weekdays_json": json.dumps(settings["working_weekdays_json"], ensure_ascii=False),
            "weekend_work_counts_as_holiday_work": _to_csv_bool(bool(settings["weekend_work_counts_as_holiday_work"])),
            "paid_leave_approval_mode": str(settings["paid_leave_approval_mode"]),
            "special_leave_types_json": json.dumps(settings["special_leave_types_json"], ensure_ascii=False),
            "company_designated_paid_leave_dates_json": json.dumps(settings["company_designated_paid_leave_dates_json"], ensure_ascii=False),
            "company_custom_holidays_json": json.dumps(settings["company_custom_holidays_json"], ensure_ascii=False),
            "holiday_source": str(settings["holiday_source"]),
            "holiday_cache_updated_at": str(settings.get("holiday_cache_updated_at", "")),
            "updated_by_user_id": str(settings.get("updated_by_user_id", "")),
            "updated_at": str(settings.get("updated_at", "")),
        }

    def get_settings(self) -> dict[str, Any]:
        with self._lock:
            rows = self._gw.read_rows("Settings")
            row = rows[0] if rows else self._settings_to_row(DEFAULT_SETTINGS)
            out = dict(DEFAULT_SETTINGS)
            out.update(
                {
                    "payroll_cutoff_day": _as_int(row.get("payroll_cutoff_day"), 20),
                    "scheduled_start_time": str(row.get("scheduled_start_time") or DEFAULT_SETTINGS["scheduled_start_time"]),
                    "scheduled_end_time": str(row.get("scheduled_end_time") or DEFAULT_SETTINGS["scheduled_end_time"]),
                    "scheduled_work_minutes": _as_int(row.get("scheduled_work_minutes"), 480),
                    "grace_minutes": _as_int(row.get("grace_minutes"), 5),
                    "break_policy_type": str(row.get("break_policy_type") or "fixed"),
                    "break_fixed_minutes": _as_int(row.get("break_fixed_minutes"), 60),
                    "break_tier_json": _parse_json_list(row.get("break_tier_json", ""), DEFAULT_SETTINGS["break_tier_json"]),
                    "working_weekdays_json": [int(x) for x in _parse_json_list(row.get("working_weekdays_json", ""), [0, 1, 2, 3, 4])],
                    "weekend_work_counts_as_holiday_work": _as_bool(row.get("weekend_work_counts_as_holiday_work", "true")),
                    "paid_leave_approval_mode": str(row.get("paid_leave_approval_mode") or "require_admin"),
                    "special_leave_types_json": [str(x) for x in _parse_json_list(row.get("special_leave_types_json", ""), ["慶弔", "特別休暇"])],
                    "company_designated_paid_leave_dates_json": [str(x) for x in _parse_json_list(row.get("company_designated_paid_leave_dates_json", ""), [])],
                    "company_custom_holidays_json": [str(x) for x in _parse_json_list(row.get("company_custom_holidays_json", ""), [])],
                    "holiday_source": str(row.get("holiday_source") or "holidays-lib-jp"),
                    "holiday_cache_updated_at": str(row.get("holiday_cache_updated_at") or ""),
                    "updated_by_user_id": str(row.get("updated_by_user_id") or ""),
                    "updated_at": str(row.get("updated_at") or ""),
                }
            )
            cutoff = min(max(1, int(out["payroll_cutoff_day"])), 31)
            out["payroll_cutoff_day"] = cutoff
            out["working_weekdays_json"] = [x for x in out["working_weekdays_json"] if isinstance(x, int) and 0 <= x <= 6]
            if not out["working_weekdays_json"]:
                out["working_weekdays_json"] = [0, 1, 2, 3, 4]
            return out

    def update_settings(self, actor_user_id: str, changes: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            current = self.get_settings()
            merged = dict(current)
            for key in DEFAULT_SETTINGS:
                if key in changes:
                    merged[key] = changes[key]
            merged["updated_by_user_id"] = actor_user_id
            merged["updated_at"] = _iso_now()
            self._gw.replace_rows("Settings", [self._settings_to_row(merged)])
            self._upsert_runtime_setting("closingDay", str(_normalize_user_closing_day(merged.get("payroll_cutoff_day"), 20)))
            self._upsert_runtime_setting("requiredWorkMinutes", str(max(0, _as_int(merged.get("scheduled_work_minutes"), 480))))
            return self.get_settings()

    def _meta_map(self) -> dict[str, str]:
        rows = self._gw.read_rows("_meta")
        out: dict[str, str] = {}
        for row in rows:
            key = str(row.get("key") or "").strip()
            if key:
                out[key] = str(row.get("value") or "")
        return out

    def _ensure_meta_defaults(self) -> None:
        rows = self._gw.read_rows("_meta")
        existing = {str(r.get("key") or "").strip() for r in rows}
        now = _iso_now()
        defaults = {
            "timezone": "Asia/Tokyo",
            "closing_day_default": str(_normalize_user_closing_day(DEFAULT_SETTINGS.get("payroll_cutoff_day", 20), 20)),
        }
        changed = False
        for key, value in defaults.items():
            if key not in existing:
                rows.append({"key": key, "value": value, "updated_at": now})
                changed = True
        if changed:
            self._gw.replace_rows("_meta", rows)

    def _runtime_settings_map(self) -> dict[str, str]:
        rows = self._gw.read_rows("settings")
        out: dict[str, str] = {}
        for row in rows:
            key = str(row.get("key") or "").strip()
            if key:
                out[key] = str(row.get("value") or "")
        return out

    def _ensure_runtime_settings_defaults(self) -> None:
        rows = self._gw.read_rows("settings")
        existing = {str(r.get("key") or "").strip() for r in rows}
        now = _iso_now()

        settings = self.get_settings()
        defaults = dict(DEFAULT_RUNTIME_SETTINGS)
        defaults["closingDay"] = str(_normalize_user_closing_day(settings.get("payroll_cutoff_day"), 20))
        defaults["requiredWorkMinutes"] = str(_as_int(settings.get("scheduled_work_minutes"), 480))

        changed = False
        for key, value in defaults.items():
            if key in existing:
                continue
            rows.append({"key": key, "value": str(value), "updatedAt": now})
            changed = True

        if changed:
            self._gw.replace_rows("settings", rows)

    def _upsert_runtime_setting(self, key: str, value: str) -> None:
        if not key:
            return
        rows = self._gw.read_rows("settings")
        now = _iso_now()
        updated = False
        for row in rows:
            if str(row.get("key") or "").strip() != key:
                continue
            row["value"] = str(value)
            row["updatedAt"] = now
            updated = True
            break
        if not updated:
            rows.append({"key": key, "value": str(value), "updatedAt": now})
        self._gw.replace_rows("settings", rows)

    def get_runtime_policy(self) -> dict[str, Any]:
        defaults = dict(DEFAULT_RUNTIME_SETTINGS)
        settings = self.get_settings()
        defaults["closingDay"] = str(_normalize_user_closing_day(settings.get("payroll_cutoff_day"), 20))
        defaults["requiredWorkMinutes"] = str(_as_int(settings.get("scheduled_work_minutes"), 480))

        kv = self._runtime_settings_map()
        merged = dict(defaults)
        merged.update({k: str(v) for k, v in kv.items() if str(k).strip()})

        return {
            "timezone": str(merged.get("timezone") or "Asia/Tokyo"),
            "closing_day": _normalize_user_closing_day(merged.get("closingDay"), _normalize_user_closing_day(settings.get("payroll_cutoff_day"), 20)),
            "required_work_minutes": max(0, _as_int(merged.get("requiredWorkMinutes"), _as_int(settings.get("scheduled_work_minutes"), 480))),
            "night_start": str(merged.get("nightStart") or "22:00"),
            "night_end": str(merged.get("nightEnd") or "05:00"),
            "allow_multiple_clock_in_same_day": _as_bool(merged.get("allowMultipleClockInSameDay", "false")),
        }

    def update_runtime_policy(self, *, changes: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            current = self.get_runtime_policy()
            if "closing_day" in changes:
                self._upsert_runtime_setting("closingDay", str(_normalize_user_closing_day(changes.get("closing_day"), current["closing_day"])))
            if "required_work_minutes" in changes:
                self._upsert_runtime_setting("requiredWorkMinutes", str(max(0, _as_int(changes.get("required_work_minutes"), current["required_work_minutes"]))))
            if "night_start" in changes:
                self._upsert_runtime_setting("nightStart", str(changes.get("night_start") or current["night_start"]))
            if "night_end" in changes:
                self._upsert_runtime_setting("nightEnd", str(changes.get("night_end") or current["night_end"]))
            if "allow_multiple_clock_in_same_day" in changes:
                self._upsert_runtime_setting(
                    "allowMultipleClockInSameDay",
                    _to_csv_bool(bool(changes.get("allow_multiple_clock_in_same_day"))),
                )
            return self.get_runtime_policy()

    def _default_user_closing_day(self) -> int:
        settings_day = _normalize_user_closing_day(self.get_runtime_policy().get("closing_day"), 20)
        meta_day = _normalize_user_closing_day(self._meta_map().get("closing_day_default"), settings_day)
        return meta_day

    def get_user_closing_day(self, user_id: str) -> int:
        user = self.get_user_by_id(user_id)
        if user is not None:
            return _normalize_user_closing_day(user.get("closing_day"), self._default_user_closing_day())
        return self._default_user_closing_day()

    def get_payroll_period_for_user(self, *, user_id: str, anchor: date | None = None) -> tuple[date, date]:
        ref = anchor or _jst_now().date()
        cutoff = self.get_user_closing_day(user_id)
        return _period_for_anchor(ref, cutoff)

    def _sync_user_shadow(self, user: dict[str, Any]) -> None:
        rows = self._gw.read_rows("users")
        target_id = str(user.get("user_id") or "")
        if not target_id:
            return
        closing_day = _normalize_user_closing_day(user.get("closing_day"), self._default_user_closing_day())
        row = {
            "user_id": target_id,
            "name": str(user.get("name") or ""),
            "email": str(user.get("email") or "").strip().lower(),
            "role": str(user.get("role") or ROLE_USER),
            "closing_day": str(closing_day),
            "is_active": _to_csv_bool(bool(user.get("is_active"))),
            "created_at": str(user.get("created_at") or _iso_now()),
        }
        updated = False
        for idx, existing in enumerate(rows):
            if str(existing.get("user_id") or "") == target_id:
                rows[idx] = row
                updated = True
                break
        if not updated:
            rows.append(row)
        self._gw.replace_rows("users", rows)

    def _sync_all_users_shadow(self) -> None:
        for user in self.list_users():
            self._sync_user_shadow(user)

    def _row_to_auth_user(self, row: dict[str, str]) -> AuthUser:
        return AuthUser(
            user_id=str(row["user_id"]),
            email=str(row["email"]).strip().lower(),
            name=str(row["name"]),
            role=str(row.get("role") or ROLE_USER),
            is_active=_as_bool(row.get("is_active", "true")),
            password_hash=str(row.get("password_hash") or ""),
        )

    def list_users(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._gw.read_rows("Users")
            users: list[dict[str, Any]] = []
            for row in rows:
                au = self._row_to_auth_user(row)
                users.append(
                    {
                        "user_id": au.user_id,
                        "email": au.email,
                        "name": au.name,
                        "role": au.role,
                        "closing_day": _normalize_user_closing_day(row.get("closing_day"), self._default_user_closing_day()),
                        "is_active": au.is_active,
                        "created_at": str(row.get("created_at") or ""),
                        "updated_at": str(row.get("updated_at") or ""),
                        "last_login_at": str(row.get("last_login_at") or ""),
                    }
                )
            users.sort(key=lambda x: x["email"])
            return users

    def get_user_by_id(self, user_id: str) -> dict[str, Any] | None:
        with self._lock:
            for row in self._gw.read_rows("Users"):
                if str(row.get("user_id")) == str(user_id):
                    au = self._row_to_auth_user(row)
                    return {
                        "user_id": au.user_id,
                        "email": au.email,
                        "name": au.name,
                        "role": au.role,
                        "closing_day": _normalize_user_closing_day(row.get("closing_day"), self._default_user_closing_day()),
                        "is_active": au.is_active,
                        "password_hash": au.password_hash,
                        "created_at": str(row.get("created_at") or ""),
                        "updated_at": str(row.get("updated_at") or ""),
                        "last_login_at": str(row.get("last_login_at") or ""),
                    }
            return None

    def get_user_by_email(self, email: str) -> dict[str, Any] | None:
        needle = str(email or "").strip().lower()
        with self._lock:
            for row in self._gw.read_rows("Users"):
                if str(row.get("email") or "").strip().lower() == needle:
                    au = self._row_to_auth_user(row)
                    return {
                        "user_id": au.user_id,
                        "email": au.email,
                        "name": au.name,
                        "role": au.role,
                        "closing_day": _normalize_user_closing_day(row.get("closing_day"), self._default_user_closing_day()),
                        "is_active": au.is_active,
                        "password_hash": au.password_hash,
                        "created_at": str(row.get("created_at") or ""),
                        "updated_at": str(row.get("updated_at") or ""),
                        "last_login_at": str(row.get("last_login_at") or ""),
                    }
            return None

    def create_user(
        self,
        *,
        email: str,
        name: str,
        password: str,
        role: str = ROLE_USER,
        closing_day: int | None = None,
        is_active: bool = True,
    ) -> dict[str, Any]:
        with self._lock:
            lowered = str(email or "").strip().lower()
            if not lowered:
                raise ValueError("email_required")
            if self.get_user_by_email(lowered) is not None:
                raise ValueError("email_already_exists")
            if role not in ROLE_ORDER:
                raise ValueError("invalid_role")
            now = _iso_now()
            row = {
                "user_id": str(uuid.uuid4()),
                "email": lowered,
                "name": str(name or lowered.split("@")[0]),
                "role": role,
                "closing_day": str(_normalize_user_closing_day(closing_day, self._default_user_closing_day())),
                "is_active": _to_csv_bool(is_active),
                "password_hash": hash_password(password),
                "created_at": now,
                "updated_at": now,
                "last_login_at": "",
            }
            self._gw.append_row("Users", row)
            created = self.get_user_by_email(lowered)
            if created is None:
                raise StorageError("failed_to_create_user")
            self._sync_user_shadow(created)
            return created

    def update_user(
        self,
        *,
        user_id: str,
        name: str | None = None,
        role: str | None = None,
        closing_day: int | None = None,
        is_active: bool | None = None,
        password: str | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            rows = self._gw.read_rows("Users")
            found = False
            changed = False
            for row in rows:
                if str(row.get("user_id")) != str(user_id):
                    continue
                found = True
                if name is not None:
                    row["name"] = str(name)
                    changed = True
                if role is not None:
                    if role not in ROLE_ORDER:
                        raise ValueError("invalid_role")
                    row["role"] = role
                    changed = True
                if closing_day is not None:
                    row["closing_day"] = str(_normalize_user_closing_day(closing_day, self._default_user_closing_day()))
                    changed = True
                if is_active is not None:
                    row["is_active"] = _to_csv_bool(bool(is_active))
                    changed = True
                if password is not None and str(password).strip():
                    row["password_hash"] = hash_password(password)
                    changed = True
                if changed:
                    row["updated_at"] = _iso_now()
                break
            if not found:
                raise ValueError("user_not_found")
            if changed:
                self._gw.replace_rows("Users", rows)
            updated = self.get_user_by_id(user_id)
            if updated is None:
                raise StorageError("failed_to_update_user")
            self._sync_user_shadow(updated)
            return updated

    def authenticate_user(self, *, email: str, password: str) -> dict[str, Any] | None:
        user = self.get_user_by_email(email)
        if user is None:
            return None
        if not user["is_active"]:
            return None
        if not verify_password(password, user["password_hash"]):
            return None
        self.touch_last_login(user["user_id"])
        return self.get_user_by_id(user["user_id"])

    def touch_last_login(self, user_id: str) -> None:
        with self._lock:
            rows = self._gw.read_rows("Users")
            changed = False
            for row in rows:
                if str(row.get("user_id")) == str(user_id):
                    row["last_login_at"] = _iso_now()
                    row["updated_at"] = row["last_login_at"]
                    changed = True
                    break
            if changed:
                self._gw.replace_rows("Users", rows)

    def _parse_event(self, row: dict[str, str]) -> dict[str, Any]:
        event_time = str(row.get("event_time") or "")
        event_dt = _parse_dt_iso(event_time) if event_time else None
        return {
            "event_id": str(row.get("event_id") or ""),
            "user_id": str(row.get("user_id") or ""),
            "event_type": str(row.get("event_type") or ""),
            "event_time": event_time,
            "event_dt": event_dt,
            "client_time": str(row.get("client_time") or ""),
            "source": str(row.get("source") or "web"),
            "ip": str(row.get("ip") or ""),
            "user_agent": str(row.get("user_agent") or ""),
            "note": str(row.get("note") or ""),
            "is_edited": _as_bool(row.get("is_edited", "false")),
            "edited_from_event_id": str(row.get("edited_from_event_id") or ""),
            "edited_by_user_id": str(row.get("edited_by_user_id") or ""),
            "edited_at": str(row.get("edited_at") or ""),
        }

    def list_events(self, *, user_id: str | None = None, start_date: date | None = None, end_date: date | None = None) -> list[dict[str, Any]]:
        with self._lock:
            rows = [self._parse_event(r) for r in self._gw.read_rows("Events")]
        out: list[dict[str, Any]] = []
        for row in rows:
            if user_id and str(row["user_id"]) != str(user_id):
                continue
            dt: datetime | None = row["event_dt"]
            if dt is None:
                continue
            d = dt.date()
            if start_date and d < start_date:
                continue
            if end_date and d > end_date:
                continue
            out.append(row)
        return out

    def list_recent_events(self, *, user_id: str, limit: int = 10) -> list[dict[str, Any]]:
        target = max(1, int(limit))
        batch = max(80, target * 8)
        collected: list[dict[str, Any]] = []

        with self._lock:
            for _ in range(5):
                tail_rows = [self._parse_event(r) for r in self._gw.read_tail_rows("Events", batch)]
                items = [row for row in tail_rows if str(row.get("user_id") or "") == str(user_id) and row.get("event_dt")]
                items.sort(key=lambda ev: ev.get("event_dt") or datetime.min.replace(tzinfo=JST), reverse=True)
                collected = items[:target]
                if len(collected) >= target or len(tail_rows) < batch:
                    break
                batch *= 2

            if len(collected) < target:
                all_rows = [self._parse_event(r) for r in self._gw.read_rows("Events")]
                items = [row for row in all_rows if str(row.get("user_id") or "") == str(user_id) and row.get("event_dt")]
                items.sort(key=lambda ev: ev.get("event_dt") or datetime.min.replace(tzinfo=JST), reverse=True)
                collected = items[:target]

        return collected

    def get_event_by_id(self, event_id: str) -> dict[str, Any] | None:
        needle = str(event_id or "").strip()
        if not needle:
            return None
        with self._lock:
            for row in self._gw.read_rows("Events"):
                if str(row.get("event_id") or "") == needle:
                    return self._parse_event(row)
        return None

    def _events_by_day(self, *, user_id: str, start_date: date, end_date: date) -> dict[str, list[dict[str, Any]]]:
        out: dict[str, list[dict[str, Any]]] = {}
        for ev in self.list_events(user_id=user_id, start_date=start_date, end_date=end_date):
            key = ev["event_dt"].date().isoformat()
            out.setdefault(key, []).append(ev)
        return out

    def _day_events_latest(self, events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        latest: dict[str, dict[str, Any]] = {}
        for ev in events:
            typ = str(ev["event_type"])
            if typ in EVENT_TYPES:
                latest[typ] = ev
        return latest

    def get_day_record(self, *, user_id: str, target_date: date) -> dict[str, Any]:
        key = target_date.isoformat()
        events = self._events_by_day(user_id=user_id, start_date=target_date, end_date=target_date).get(key, [])
        latest = self._day_events_latest(events)
        in_dt = latest.get(EVENT_IN, {}).get("event_dt") if latest.get(EVENT_IN) else None
        out_dt = latest.get(EVENT_OUT, {}).get("event_dt") if latest.get(EVENT_OUT) else None
        outing_dt = latest.get(EVENT_OUTING, {}).get("event_dt") if latest.get(EVENT_OUTING) else None
        return_dt = latest.get(EVENT_RETURN, {}).get("event_dt") if latest.get(EVENT_RETURN) else None
        is_edited = any(bool(ev.get("is_edited")) for ev in events)
        note = ""
        for ev in reversed(events):
            n = str(ev.get("note") or "").strip()
            if n:
                note = n
                break
        return {
            "date": key,
            "clock_in_at": in_dt.isoformat() if in_dt else "",
            "clock_out_at": out_dt.isoformat() if out_dt else "",
            "outing_at": outing_dt.isoformat() if outing_dt else "",
            "return_at": return_dt.isoformat() if return_dt else "",
            "clock_in_label": in_dt.strftime("%H:%M") if in_dt else "",
            "clock_out_label": out_dt.strftime("%H:%M") if out_dt else "",
            "outing_label": outing_dt.strftime("%H:%M") if outing_dt else "",
            "return_label": return_dt.strftime("%H:%M") if return_dt else "",
            "note": note,
            "is_edited": is_edited,
            "events": events,
        }

    def _attendance_state_from_events(self, events: list[dict[str, Any]]) -> str:
        timeline = sorted(
            [ev for ev in events if str(ev.get("event_type") or "") in EVENT_TYPES],
            key=lambda ev: str(ev.get("event_time") or ""),
        )
        state = "NOT_STARTED"
        for ev in timeline:
            event_type = str(ev.get("event_type") or "")
            if event_type == EVENT_IN:
                state = "WORKING"
            elif event_type == EVENT_OUTING and state == "WORKING":
                state = "OUTING"
            elif event_type == EVENT_RETURN and state == "OUTING":
                state = "WORKING"
            elif event_type == EVENT_OUT and state in {"WORKING", "OUTING"}:
                state = "DONE"
        return state

    def attendance_state(self, *, user_id: str, target_date: date) -> dict[str, Any]:
        record = self.get_day_record(user_id=user_id, target_date=target_date)
        state = self._attendance_state_from_events(record.get("events") or [])
        policy = self.get_runtime_policy()
        allow_multi = bool(policy.get("allow_multiple_clock_in_same_day"))

        status = "未出勤"
        if state in {"WORKING", "OUTING"}:
            status = "出勤済"
        elif state == "DONE":
            status = "退勤済"

        primary_action = "clock-in"
        primary_label = "出勤"
        primary_endpoint = "/events/clock-in"
        primary_disabled = False

        if state in {"WORKING", "OUTING"}:
            primary_action = "clock-out"
            primary_label = "退勤"
            primary_endpoint = "/events/clock-out"
        elif state == "DONE" and not allow_multi:
            primary_action = "done"
            primary_label = "退勤済"
            primary_endpoint = "/events/clock-out"
            primary_disabled = True

        secondary_action = "disabled"
        secondary_label = "外出/戻り"
        secondary_endpoint = "/events/outing"
        secondary_disabled = True
        if state == "WORKING":
            secondary_action = "outing"
            secondary_label = "外出"
            secondary_endpoint = "/events/outing"
            secondary_disabled = False
        elif state == "OUTING":
            secondary_action = "return"
            secondary_label = "戻り"
            secondary_endpoint = "/events/return"
            secondary_disabled = False

        return {
            "record": record,
            "event_state": state,
            "status": status,
            "is_outing_now": state == "OUTING",
            "primary_action": primary_action,
            "primary_label": primary_label,
            "primary_endpoint": primary_endpoint,
            "primary_disabled": primary_disabled,
            "secondary_action": secondary_action,
            "secondary_label": secondary_label,
            "secondary_endpoint": secondary_endpoint,
            "secondary_disabled": secondary_disabled,
        }

    def _can_clock(self, *, user_id: str, today: date, action: str) -> tuple[bool, str]:
        state = self.attendance_state(user_id=user_id, target_date=today)
        event_state = str(state.get("event_state") or "NOT_STARTED")

        if action == EVENT_IN:
            if event_state in {"WORKING", "OUTING"}:
                return False, "already_clocked_in"
            if event_state == "DONE" and bool(state.get("primary_disabled")):
                return False, "already_clocked_out"
            return True, ""
        if action == EVENT_OUT:
            if event_state == "NOT_STARTED":
                return False, "clock_in_required"
            if event_state == "DONE":
                return False, "already_clocked_out"
            return True, ""
        if action == EVENT_OUTING:
            if event_state == "NOT_STARTED":
                return False, "clock_in_required"
            if event_state == "DONE":
                return False, "already_clocked_out"
            if event_state == "OUTING":
                return False, "already_outing"
            return True, ""
        if action == EVENT_RETURN:
            if event_state == "NOT_STARTED":
                return False, "clock_in_required"
            if event_state == "DONE":
                return False, "already_clocked_out"
            if event_state != "OUTING":
                return False, "outing_required"
            return True, ""
        return False, "invalid_action"

    def append_event(
        self,
        *,
        user_id: str,
        event_type: str,
        event_dt: datetime,
        client_dt: datetime | None,
        note: str,
        source: str,
        ip: str,
        user_agent: str,
        is_edited: bool,
        edited_from_event_id: str,
        edited_by_user_id: str,
    ) -> dict[str, Any]:
        if event_type not in EVENT_TYPES:
            raise ValueError("invalid_event_type")
        row = {
            "event_id": str(uuid.uuid4()),
            "user_id": str(user_id),
            "event_type": event_type,
            "event_time": event_dt.astimezone(JST).isoformat(),
            "client_time": (client_dt.astimezone(JST).isoformat() if client_dt else ""),
            "source": source,
            "ip": ip,
            "user_agent": user_agent,
            "note": str(note or ""),
            "is_edited": _to_csv_bool(bool(is_edited)),
            "edited_from_event_id": str(edited_from_event_id or ""),
            "edited_by_user_id": str(edited_by_user_id or ""),
            "edited_at": _iso_now() if is_edited else "",
        }
        shadow_row = {
            "event_id": row["event_id"],
            "user_id": str(user_id),
            "event_type": event_type,
            "event_at": row["event_time"],
            "source": source,
            "note": str(note or ""),
            "created_at": _iso_now(),
            "created_by": str(edited_by_user_id or user_id),
            "is_deleted": "false",
        }
        feed_row = {
            "eventId": row["event_id"],
            "userId": str(user_id),
            "eventType": event_type,
            "timestamp": row["event_time"],
            "note": str(note or ""),
            "source": source or "web",
            "createdAt": _iso_now(),
            "updatedAt": _iso_now(),
        }
        with self._lock:
            self._gw.append_row("Events", row)
            self._gw.append_row("events", shadow_row)
            self._gw.append_row("time_events", feed_row)
        return self._parse_event(row)

    def _sync_daily_summary_row(self, *, user_id: str, target_date: date) -> None:
        rows = self._gw.read_rows("daily")
        target_key = (str(user_id), target_date.isoformat())
        rec = self.get_day_record(user_id=user_id, target_date=target_date)
        day_rows, _ = self.daily_records(user_id=user_id, period_start=target_date, period_end=target_date)
        daily = day_rows[0] if day_rows else {}
        row = {
            "user_id": str(user_id),
            "date": target_date.isoformat(),
            "clock_in_at": str(rec.get("clock_in_at") or ""),
            "clock_out_at": str(rec.get("clock_out_at") or ""),
            "break_minutes": str(daily.get("break_minutes", 0)),
            "work_minutes": str(daily.get("work_minutes", 0)),
            "overtime_minutes": str(daily.get("overtime_minutes", 0)),
            "incomplete_flag": _to_csv_bool(bool(daily.get("incomplete"))),
            "updated_at": _iso_now(),
        }
        updated = False
        for idx, existing in enumerate(rows):
            if (str(existing.get("user_id") or ""), str(existing.get("date") or "")) == target_key:
                rows[idx] = row
                updated = True
                break
        if not updated:
            rows.append(row)
        self._gw.replace_rows("daily", rows)

    def _sync_monthly_summary_row(self, *, user_id: str, anchor: date) -> None:
        rows = self._gw.read_rows("monthly")
        period_start, period_end = self.get_payroll_period_for_user(user_id=user_id, anchor=anchor)
        _, summary = self.daily_records(user_id=user_id, period_start=period_start, period_end=period_end)
        row = {
            "user_id": str(user_id),
            "period_start": period_start.isoformat(),
            "period_end": period_end.isoformat(),
            "work_minutes": str(summary.get("work_minutes", 0)),
            "overtime_minutes": str(summary.get("overtime_minutes", 0)),
            "updated_at": _iso_now(),
        }
        updated = False
        for idx, existing in enumerate(rows):
            if str(existing.get("user_id") or "") != str(user_id):
                continue
            if str(existing.get("period_start") or "") == row["period_start"] and str(existing.get("period_end") or "") == row["period_end"]:
                rows[idx] = row
                updated = True
                break
        if not updated:
            rows.append(row)
        self._gw.replace_rows("monthly", rows)

    def _sync_cached_summaries(self, *, user_id: str, anchor: date) -> None:
        self._sync_daily_summary_row(user_id=user_id, target_date=anchor)
        self._sync_monthly_summary_row(user_id=user_id, anchor=anchor)

    def clock_action(self, *, user_id: str, action: str, note: str, ip: str, user_agent: str) -> dict[str, Any]:
        now = _jst_now()
        can, code = self._can_clock(user_id=user_id, today=now.date(), action=action)
        if not can:
            raise ValueError(code)
        appended = self.append_event(
            user_id=user_id,
            event_type=action,
            event_dt=now,
            client_dt=now,
            note=note,
            source="web",
            ip=ip,
            user_agent=user_agent,
            is_edited=False,
            edited_from_event_id="",
            edited_by_user_id="",
        )
        self._sync_cached_summaries(user_id=user_id, anchor=now.date())
        return appended

    def edit_event(
        self,
        *,
        actor_user_id: str,
        target_event_id: str,
        event_type: str,
        event_dt: datetime,
        note: str,
        ip: str,
        user_agent: str,
    ) -> dict[str, Any]:
        normalized_type = str(event_type or "").strip().upper()
        if normalized_type not in EVENT_TYPES:
            raise ValueError("invalid_event_type")
        original = self.get_event_by_id(target_event_id)
        if original is None:
            raise ValueError("event_not_found")
        before = {
            "event_id": str(original.get("event_id") or ""),
            "user_id": str(original.get("user_id") or ""),
            "event_type": str(original.get("event_type") or ""),
            "event_time": str(original.get("event_time") or ""),
            "note": str(original.get("note") or ""),
        }
        after = {
            "event_type": normalized_type,
            "event_time": event_dt.astimezone(JST).isoformat(),
            "note": str(note or ""),
        }
        appended = self.append_event(
            user_id=str(original["user_id"]),
            event_type=normalized_type,
            event_dt=event_dt,
            client_dt=event_dt,
            note=str(note or ""),
            source="web",
            ip=str(ip or ""),
            user_agent=str(user_agent or ""),
            is_edited=True,
            edited_from_event_id=str(original["event_id"]),
            edited_by_user_id=str(actor_user_id or ""),
        )
        self._gw.append_row(
            "edits",
            {
                "edit_id": str(uuid.uuid4()),
                "event_id": str(original.get("event_id") or ""),
                "edited_at": _iso_now(),
                "edited_by": str(actor_user_id or ""),
                "before_json": json.dumps(before, ensure_ascii=False),
                "after_json": json.dumps(after, ensure_ascii=False),
                "reason": str(note or ""),
            },
        )
        self._gw.append_row(
            "time_edits",
            {
                "editId": str(uuid.uuid4()),
                "eventId": str(original.get("event_id") or ""),
                "userId": str(actor_user_id or ""),
                "beforeJson": json.dumps(before, ensure_ascii=False),
                "afterJson": json.dumps(after, ensure_ascii=False),
                "reason": str(note or ""),
                "editedAt": _iso_now(),
            },
        )
        event_day = event_dt.astimezone(JST).date()
        self._sync_cached_summaries(user_id=str(original["user_id"]), anchor=event_day)
        return appended

    def edit_day_record(
        self,
        *,
        actor_user_id: str,
        target_user_id: str,
        target_date: date,
        clock_in_at: datetime | None,
        clock_out_at: datetime | None,
        outing_at: datetime | None,
        return_at: datetime | None,
        note: str,
    ) -> None:
        if clock_in_at and clock_out_at and clock_out_at < clock_in_at:
            raise ValueError("clock_out_before_clock_in")
        if outing_at and return_at and return_at < outing_at:
            raise ValueError("return_before_outing")

        existing = self.get_day_record(user_id=target_user_id, target_date=target_date)
        current_latest = self._day_events_latest(existing["events"])

        patch_items: list[tuple[str, datetime | None]] = [
            (EVENT_IN, clock_in_at),
            (EVENT_OUT, clock_out_at),
            (EVENT_OUTING, outing_at),
            (EVENT_RETURN, return_at),
        ]
        for ev_type, new_dt in patch_items:
            if new_dt is None:
                continue
            old_ev = current_latest.get(ev_type)
            old_iso = old_ev["event_time"] if old_ev else ""
            if old_iso and _parse_dt_iso(old_iso) == new_dt:
                continue
            self.append_event(
                user_id=target_user_id,
                event_type=ev_type,
                event_dt=new_dt,
                client_dt=new_dt,
                note=note,
                source="web",
                ip="",
                user_agent="",
                is_edited=True,
                edited_from_event_id=(old_ev["event_id"] if old_ev else ""),
                edited_by_user_id=actor_user_id,
            )

        has_current_events = any(current_latest.get(k) is not None for k in EVENT_TYPES)
        if note and (any(v is not None for _, v in patch_items) or has_current_events):
            marker_dt = (
                clock_out_at
                or clock_in_at
                or outing_at
                or return_at
                or (current_latest.get(EVENT_OUT, {}) or {}).get("event_dt")
                or (current_latest.get(EVENT_IN, {}) or {}).get("event_dt")
                or datetime.combine(target_date, time(9, 0), tzinfo=JST)
            )
            self.append_event(
                user_id=target_user_id,
                event_type=EVENT_IN,
                event_dt=marker_dt,
                client_dt=marker_dt,
                note=note,
                source="web",
                ip="",
                user_agent="",
                is_edited=True,
                edited_from_event_id="",
                edited_by_user_id=actor_user_id,
            )
        self._sync_cached_summaries(user_id=target_user_id, anchor=target_date)

    def create_leave_request(self, *, user_id: str, leave_date: str, leave_type: str, leave_name: str, note: str) -> dict[str, Any]:
        if leave_type not in LEAVE_TYPES:
            raise ValueError("invalid_leave_type")
        settings = self.get_settings()
        status = LEAVE_APPROVED if settings["paid_leave_approval_mode"] == "auto_approve" else LEAVE_PENDING
        row = {
            "leave_id": str(uuid.uuid4()),
            "user_id": user_id,
            "leave_date": leave_date,
            "leave_type": leave_type,
            "leave_name": leave_name,
            "note": note,
            "status": status,
            "requested_at": _iso_now(),
            "decided_at": _iso_now() if status == LEAVE_APPROVED else "",
            "decided_by_user_id": user_id if status == LEAVE_APPROVED else "",
        }
        with self._lock:
            self._gw.append_row("LeaveRequests", row)
        return dict(row)

    def list_leave_requests(self, *, user_id: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._gw.read_rows("LeaveRequests")
        out: list[dict[str, Any]] = []
        for row in rows:
            if user_id and str(row.get("user_id")) != str(user_id):
                continue
            if str(row.get("status") or "") not in LEAVE_STATUSES:
                row["status"] = LEAVE_PENDING
            out.append({k: str(v or "") for k, v in row.items()})
        out.sort(key=lambda x: (x["leave_date"], x["requested_at"]))
        return out

    def decide_leave_request(self, *, leave_id: str, actor_user_id: str, approve: bool) -> dict[str, Any]:
        with self._lock:
            rows = self._gw.read_rows("LeaveRequests")
            updated: dict[str, Any] | None = None
            for row in rows:
                if str(row.get("leave_id")) != str(leave_id):
                    continue
                row["status"] = LEAVE_APPROVED if approve else LEAVE_REJECTED
                row["decided_at"] = _iso_now()
                row["decided_by_user_id"] = actor_user_id
                updated = dict(row)
                break
            if updated is None:
                raise ValueError("leave_not_found")
            self._gw.replace_rows("LeaveRequests", rows)
            return updated

    def get_payroll_period(self, *, anchor: date | None = None) -> tuple[date, date]:
        ref = anchor or _jst_now().date()
        runtime = self.get_runtime_policy()
        return _period_for_anchor(ref, int(runtime.get("closing_day") or 20))

    def get_payroll_period_by_month(self, year_month: str) -> tuple[date, date]:
        base = datetime.strptime(str(year_month) + "-01", "%Y-%m-%d").date()
        runtime = self.get_runtime_policy()
        cutoff = int(runtime.get("closing_day") or 20)
        end = date(base.year, base.month, _clamp_day(base.year, base.month, cutoff))
        py, pm = _shift_month(base.year, base.month, -1)
        start = date(py, pm, _clamp_day(py, pm, cutoff + 1))
        return start, end

    def _approved_leave_map(self, *, user_id: str | None, start: date, end: date) -> dict[tuple[str, str], dict[str, Any]]:
        rows = self.list_leave_requests(user_id=user_id)
        out: dict[tuple[str, str], dict[str, Any]] = {}
        for row in rows:
            if row["status"] != LEAVE_APPROVED:
                continue
            d = date.fromisoformat(row["leave_date"])
            if d < start or d > end:
                continue
            out[(row["user_id"], row["leave_date"])] = row
        return out

    def refresh_holidays_cache(self, *, force: bool = False) -> None:
        settings = self.get_settings()
        updated_at = str(settings.get("holiday_cache_updated_at") or "")
        should_refresh = force or not updated_at
        if updated_at and not force:
            try:
                last = _parse_dt_iso(updated_at)
                should_refresh = (_jst_now() - last) >= timedelta(hours=24)
            except Exception:
                should_refresh = True

        if not should_refresh:
            return

        years = [_jst_now().year - 1, _jst_now().year, _jst_now().year + 1]
        fetched_at = _iso_now()
        rows: dict[str, dict[str, str]] = {}

        if holidays_lib is not None:
            jp = holidays_lib.country_holidays("JP", years=years)
            for dt, name in jp.items():
                key = dt.isoformat()
                rows[key] = {
                    "date": key,
                    "name": str(name),
                    "kind": HOLIDAY_PUBLIC,
                    "source": "holidays-lib-jp",
                    "year": str(dt.year),
                    "fetched_at": fetched_at,
                }

        for day in settings["company_custom_holidays_json"]:
            try:
                d = date.fromisoformat(day)
            except Exception:
                continue
            rows[d.isoformat()] = {
                "date": d.isoformat(),
                "name": "Company Holiday",
                "kind": HOLIDAY_COMPANY_CUSTOM,
                "source": "settings",
                "year": str(d.year),
                "fetched_at": fetched_at,
            }

        merged = sorted(rows.values(), key=lambda x: x["date"])
        with self._lock:
            self._gw.replace_rows("Holidays", merged)
            self.update_settings(actor_user_id=str(settings.get("updated_by_user_id") or ""), changes={"holiday_cache_updated_at": fetched_at})

    def holiday_map(self, *, start: date, end: date) -> dict[str, dict[str, str]]:
        with self._lock:
            rows = self._gw.read_rows("Holidays")
        out: dict[str, dict[str, str]] = {}
        for row in rows:
            try:
                d = date.fromisoformat(str(row.get("date") or ""))
            except Exception:
                continue
            if d < start or d > end:
                continue
            out[d.isoformat()] = {
                "name": str(row.get("name") or ""),
                "kind": str(row.get("kind") or HOLIDAY_PUBLIC),
            }
        return out

    def daily_records(self, *, user_id: str, period_start: date, period_end: date) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        self.refresh_holidays_cache(force=False)
        settings = self.get_settings()
        holidays_map = self.holiday_map(start=period_start, end=period_end)
        company_designated = set(settings["company_designated_paid_leave_dates_json"])
        working_weekdays = set(int(x) for x in settings["working_weekdays_json"])

        leaves = self._approved_leave_map(user_id=user_id, start=period_start, end=period_end)
        events_by_day = self._events_by_day(user_id=user_id, start_date=period_start, end_date=period_end)

        rows: list[dict[str, Any]] = []
        summary = {
            "work_days": 0,
            "work_minutes": 0,
            "overtime_days": 0,
            "overtime_minutes": 0,
            "holiday_work_days": 0,
            "holiday_work_minutes": 0,
            "lateness_count": 0,
            "early_leave_count": 0,
            "absence_count": 0,
            "special_leave_count": 0,
            "paid_leave_count": 0,
            "company_designated_paid_leave_count": 0,
            "comp_leave_count": 0,
            "other_count": 0,
            "missing_count": 0,
        }

        sched_start_m = _time_to_minutes(settings["scheduled_start_time"], default=8 * 60 + 55)
        sched_end_m = _time_to_minutes(settings["scheduled_end_time"], default=17 * 60 + 55)
        grace = int(settings["grace_minutes"])
        scheduled_work_minutes = int(settings["scheduled_work_minutes"])

        for d in _date_range(period_start, period_end):
            key = d.isoformat()
            day_events = events_by_day.get(key, [])
            latest = self._day_events_latest(day_events)

            ev_in = latest.get(EVENT_IN)
            ev_out = latest.get(EVENT_OUT)
            ev_outing = latest.get(EVENT_OUTING)
            ev_return = latest.get(EVENT_RETURN)

            in_dt: datetime | None = ev_in["event_dt"] if ev_in else None
            out_dt: datetime | None = ev_out["event_dt"] if ev_out else None
            outing_dt: datetime | None = ev_outing["event_dt"] if ev_outing else None
            return_dt: datetime | None = ev_return["event_dt"] if ev_return else None

            note = ""
            for ev in reversed(day_events):
                msg = str(ev.get("note") or "").strip()
                if msg:
                    note = msg
                    break

            leave = leaves.get((user_id, key))
            is_public_or_company_holiday = key in holidays_map
            is_company_designated = key in company_designated
            is_working_weekday = d.weekday() in working_weekdays

            has_punch = in_dt is not None or out_dt is not None
            incomplete = (in_dt is None) != (out_dt is None)
            worked_minutes = 0
            overtime_minutes = 0
            is_late = False
            is_early = False

            if in_dt and out_dt and out_dt >= in_dt:
                raw_minutes = int((out_dt - in_dt).total_seconds() // 60)
                break_minutes = self._break_minutes(settings=settings, raw_work_minutes=raw_minutes)
                worked_minutes = max(0, raw_minutes - break_minutes)
                overtime_minutes = max(0, worked_minutes - scheduled_work_minutes)

                in_m = in_dt.hour * 60 + in_dt.minute
                out_m = out_dt.hour * 60 + out_dt.minute
                is_late = in_m > (sched_start_m + grace)
                is_early = out_m < (sched_end_m - grace)

            flags: list[str] = []
            classification = "WORK"
            if any(bool(ev.get("is_edited")) for ev in day_events):
                flags.append("edited")
            if incomplete:
                flags.append("incomplete")

            if leave:
                classification = f"LEAVE_{leave['leave_type']}"
                if leave["leave_type"] == LEAVE_PAID:
                    summary["paid_leave_count"] += 1
                elif leave["leave_type"] == LEAVE_SPECIAL:
                    summary["special_leave_count"] += 1
                elif leave["leave_type"] == LEAVE_COMPANY_DESIGNATED:
                    summary["company_designated_paid_leave_count"] += 1
                else:
                    summary["other_count"] += 1
            elif is_company_designated and not has_punch:
                classification = "COMPANY_DESIGNATED_PAID"
                summary["company_designated_paid_leave_count"] += 1
            elif is_company_designated and has_punch:
                classification = "HOLIDAY_WORK"
            elif is_public_or_company_holiday:
                classification = "HOLIDAY_WORK" if has_punch else "HOLIDAY_OFF"
            elif not is_working_weekday:
                classification = "HOLIDAY_WORK" if has_punch else "WEEKEND_OFF"
            else:
                if not has_punch or incomplete:
                    classification = "MISSING"
                    summary["missing_count"] += 1
                    if d <= _jst_now().date():
                        summary["absence_count"] += 1
                else:
                    classification = "WORK"

            if has_punch and classification in {"WORK", "HOLIDAY_WORK"}:
                summary["work_days"] += 1
                summary["work_minutes"] += worked_minutes
                if overtime_minutes > 0:
                    summary["overtime_days"] += 1
                    summary["overtime_minutes"] += overtime_minutes
                if is_late:
                    summary["lateness_count"] += 1
                if is_early:
                    summary["early_leave_count"] += 1

            if has_punch and classification == "HOLIDAY_WORK":
                summary["holiday_work_days"] += 1
                summary["holiday_work_minutes"] += worked_minutes

            if is_public_or_company_holiday:
                flags.append("holiday")
            elif not is_working_weekday:
                flags.append("weekend")
            if is_company_designated:
                flags.append("company_designated")
            if classification == "MISSING":
                flags.append("missing")

            rows.append(
                {
                    "date": key,
                    "weekday": JP_WEEKDAYS[d.weekday()],
                    "clock_in": in_dt.strftime("%H:%M") if in_dt else "",
                    "clock_out": out_dt.strftime("%H:%M") if out_dt else "",
                    "outing": outing_dt.strftime("%H:%M") if outing_dt else "",
                    "return": return_dt.strftime("%H:%M") if return_dt else "",
                    "worked_minutes": worked_minutes,
                    "worked_hhmm": _format_hhmm(worked_minutes),
                    "overtime_minutes": overtime_minutes,
                    "note": note,
                    "classification": classification,
                    "flags": flags,
                    "is_edited": "edited" in flags,
                    "leave": leave,
                }
            )

        return rows, summary

    def _break_minutes(self, *, settings: dict[str, Any], raw_work_minutes: int) -> int:
        policy = str(settings.get("break_policy_type") or "fixed")
        if policy == "tiered":
            break_minutes = 0
            for tier in settings.get("break_tier_json", []):
                min_work = _as_int(tier.get("min_work_minutes"), 0)
                mins = _as_int(tier.get("break_minutes"), 0)
                if raw_work_minutes >= min_work:
                    break_minutes = max(break_minutes, mins)
            return max(0, break_minutes)
        return max(0, _as_int(settings.get("break_fixed_minutes"), 60))

    def summary_for_period(self, *, user_id: str | None, period_start: date, period_end: date) -> list[dict[str, Any]]:
        users = self.list_users()
        out: list[dict[str, Any]] = []
        for user in users:
            if not user["is_active"]:
                continue
            if user_id and user["user_id"] != user_id:
                continue
            _, summary = self.daily_records(user_id=user["user_id"], period_start=period_start, period_end=period_end)
            out.append({"user": user, "summary": summary})
        return out

    def export_csv_rows(self, *, user_id: str, period_start: date, period_end: date) -> list[dict[str, str]]:
        user = self.get_user_by_id(user_id)
        if user is None:
            return []
        records, _ = self.daily_records(user_id=user_id, period_start=period_start, period_end=period_end)
        rows: list[dict[str, str]] = []
        for rec in records:
            note = rec["note"]
            if rec["leave"]:
                leave = rec["leave"]
                if leave["leave_type"] == LEAVE_PAID:
                    note = "有給"
                elif leave["leave_type"] == LEAVE_SPECIAL:
                    note = leave["leave_name"] or "特別休暇"
                elif leave["leave_type"] == LEAVE_COMPANY_DESIGNATED:
                    note = "会社指定有給"
            elif rec["classification"] == "COMPANY_DESIGNATED_PAID":
                note = "会社指定有給"
            elif rec["classification"] == "MISSING":
                note = "未入力"

            rows.append(
                {
                    "組織": "",
                    "関連エリア": "",
                    "氏名": user["name"],
                    "日付": rec["date"],
                    "曜日": rec["weekday"],
                    "始業時刻": rec["clock_in"],
                    "遅刻事由": "",
                    "外出": rec["outing"],
                    "戻り": rec["return"],
                    "終業時刻": rec["clock_out"],
                    "早退事由": "",
                    "欠勤事由": "",
                    "備考": note,
                    "修正区分": "修正" if rec["is_edited"] else "",
                }
            )
        return rows


def _resolve_google_credentials_path() -> str:
    existing = (os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "") or "").strip()
    if existing:
        p = Path(existing)
        if p.exists():
            return str(p)

    b64 = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_BASE64", "") or "").strip()
    if not b64:
        raise StorageError("Google credentials are missing. Set GOOGLE_APPLICATION_CREDENTIALS or GOOGLE_SERVICE_ACCOUNT_JSON_BASE64")

    target = Path(tempfile.gettempdir()) / "cloudlog-google-service-account.json"
    target.write_bytes(base64.b64decode(b64))
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(target)
    return str(target)
