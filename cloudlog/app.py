from __future__ import annotations

import csv
import hmac
import io
import json
import logging
import os
import secrets
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from itsdangerous import BadData, URLSafeTimedSerializer
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

from cloudlog.timeclock_store import (
    EVENT_IN,
    EVENT_OUT,
    EVENT_OUTING,
    EVENT_RETURN,
    LEAVE_COMPANY_DESIGNATED,
    LEAVE_OTHER,
    LEAVE_PAID,
    LEAVE_SPECIAL,
    ROLE_ADMIN,
    ROLE_ORDER,
    TimeclockStore,
)


log = logging.getLogger("cloudlog")
JST = timezone(timedelta(hours=9))

SESSION_USER_ID_KEY = "cloudlog_user_id"
SESSION_USER_EMAIL_KEY = "cloudlog_user_email"
SESSION_CSRF_KEY = "cloudlog_csrf"
EVENT_TYPE_LABELS = {
    EVENT_IN: "出勤",
    EVENT_OUT: "退勤",
    EVENT_OUTING: "外出",
    EVENT_RETURN: "戻り",
}


def _jst_now() -> datetime:
    return datetime.now(tz=JST)


def _jst_today() -> date:
    return _jst_now().date()


def _resolve_templates() -> Jinja2Templates:
    return Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


def _parse_bool(raw: str | None, default: bool = False) -> bool:
    text = str(raw or "").strip().lower()
    if not text:
        return default
    return text in {"1", "true", "yes", "on"}


def _parse_allowed_hosts(raw: str) -> list[str]:
    parts = [p.strip() for p in str(raw or "").split(",") if p.strip()]
    out: list[str] = []
    for part in parts:
        if part == "*":
            continue
        out.append(part)
    return out or ["localhost", "127.0.0.1"]


def _parse_trusted_proxies(raw: str) -> list[str] | str:
    text = str(raw or "").strip()
    if text == "*":
        return "*"
    out = [x.strip() for x in text.split(",") if x.strip()]
    return out if out else ["127.0.0.1"]


def _parse_hidden_user_emails(raw: str | None) -> set[str]:
    return {part.strip().lower() for part in str(raw or "").split(",") if part.strip()}


def _safe_next(next_path: str | None) -> str:
    if not next_path:
        return "/today"
    p = str(next_path).strip()
    if not p.startswith("/"):
        return "/today"
    if p.startswith("//"):
        return "/today"
    if "://" in p:
        return "/today"
    return p


def _session_user_id(session: dict[str, Any]) -> str | None:
    raw = session.get(SESSION_USER_ID_KEY)
    if raw is None:
        return None
    value = str(raw).strip()
    return value or None


def _session_user_email(session: dict[str, Any]) -> str | None:
    raw = session.get(SESSION_USER_EMAIL_KEY)
    if raw is None:
        return None
    value = str(raw).strip()
    return value or None


def _login_session(session: dict[str, Any], *, user_id: str, email: str) -> None:
    session[SESSION_USER_ID_KEY] = user_id
    session[SESSION_USER_EMAIL_KEY] = email
    _ensure_csrf_token(session)


def _logout_session(session: dict[str, Any]) -> None:
    session.pop(SESSION_USER_ID_KEY, None)
    session.pop(SESSION_USER_EMAIL_KEY, None)


def _ensure_csrf_token(session: dict[str, Any]) -> str:
    token = session.get(SESSION_CSRF_KEY)
    if isinstance(token, str) and token:
        return token
    token = secrets.token_urlsafe(32)
    session[SESSION_CSRF_KEY] = token
    return token


def _validate_csrf(session: dict[str, Any], token: str | None) -> bool:
    expected = session.get(SESSION_CSRF_KEY)
    if not isinstance(expected, str) or not expected:
        return False
    if not token:
        return False
    return hmac.compare_digest(expected, str(token))


def _fmt_hhmm(dt_iso: str) -> str:
    if not dt_iso:
        return ""
    try:
        dt = datetime.fromisoformat(dt_iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=JST)
        return dt.astimezone(JST).strftime("%H:%M")
    except Exception:
        return ""


def _fmt_hhmm_dt(dt: datetime | None) -> str:
    if dt is None:
        return ""
    return dt.astimezone(JST).strftime("%H:%M")


def _normalize_day_record_for_ui(record: dict[str, Any]) -> dict[str, Any]:
    rec = dict(record)
    rec["clock_in"] = (
        str(rec.get("clock_in") or "")
        or str(rec.get("clock_in_label") or "")
        or _fmt_hhmm(str(rec.get("clock_in_at") or ""))
    )
    rec["clock_out"] = (
        str(rec.get("clock_out") or "")
        or str(rec.get("clock_out_label") or "")
        or _fmt_hhmm(str(rec.get("clock_out_at") or ""))
    )
    rec["outing"] = (
        str(rec.get("outing") or "")
        or str(rec.get("outing_label") or "")
        or _fmt_hhmm(str(rec.get("outing_at") or ""))
    )
    rec["return"] = (
        str(rec.get("return") or "")
        or str(rec.get("return_label") or "")
        or _fmt_hhmm(str(rec.get("return_at") or ""))
    )
    return rec


def _is_currently_outing(events: list[dict[str, Any]] | None) -> bool:
    timeline = sorted(
        [ev for ev in (events or []) if str(ev.get("event_type") or "") in {EVENT_OUTING, EVENT_RETURN}],
        key=lambda ev: str(ev.get("event_time") or ""),
    )
    if not timeline:
        return False
    return str(timeline[-1].get("event_type") or "") == EVENT_OUTING


def _event_type_label(event_type: str) -> str:
    return EVENT_TYPE_LABELS.get(str(event_type or "").upper(), str(event_type or ""))


def _recent_events_for_user(user_id: str, *, limit: int = 10) -> list[dict[str, Any]]:
    rows = store.list_recent_events(user_id=user_id, limit=limit)
    rows = sorted(rows, key=lambda ev: ev.get("event_dt") or datetime.min.replace(tzinfo=JST), reverse=True)
    out: list[dict[str, Any]] = []
    for ev in rows[: max(1, int(limit))]:
        dt: datetime | None = ev.get("event_dt")
        if dt is None:
            continue
        jst_dt = dt.astimezone(JST)
        out.append(
            {
                "event_id": str(ev.get("event_id") or ""),
                "event_type": str(ev.get("event_type") or ""),
                "event_type_label": _event_type_label(str(ev.get("event_type") or "")),
                "event_time": str(ev.get("event_time") or ""),
                "event_time_label": jst_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "event_time_input": jst_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                "note": str(ev.get("note") or ""),
                "source": str(ev.get("source") or "web"),
                "is_edited": bool(ev.get("is_edited")),
                "edited_at": str(ev.get("edited_at") or ""),
            }
        )
    return out


def _work_minutes_mom(user_id: str, *, anchor: date | None = None) -> dict[str, Any]:
    ref = anchor or _jst_today()
    current_start, current_end = store.get_payroll_period_for_user(user_id=user_id, anchor=ref)
    _, current_summary = store.daily_records(user_id=user_id, period_start=current_start, period_end=current_end)

    previous_anchor = current_start - timedelta(days=1)
    previous_start, previous_end = store.get_payroll_period_for_user(user_id=user_id, anchor=previous_anchor)
    _, previous_summary = store.daily_records(user_id=user_id, period_start=previous_start, period_end=previous_end)

    this_total = int(current_summary.get("work_minutes") or 0)
    prev_total = int(previous_summary.get("work_minutes") or 0)
    pct: int | None = None
    if prev_total > 0:
        pct = int(round(((this_total - prev_total) / prev_total) * 100))

    return {
        "period_start": current_start.isoformat(),
        "period_end": current_end.isoformat(),
        "previous_period_start": previous_start.isoformat(),
        "previous_period_end": previous_end.isoformat(),
        "this_total_minutes": this_total,
        "previous_total_minutes": prev_total,
        "percent_change": pct,
    }


def _time_text_to_minutes(raw: str, default: int) -> int:
    text = str(raw or "").strip()
    if ":" not in text:
        return default
    hh, mm = text.split(":", 1)
    try:
        return max(0, min(1439, int(hh) * 60 + int(mm)))
    except Exception:
        return default


def _minutes_of_day(dt: datetime) -> int:
    jst_dt = dt.astimezone(JST)
    return max(0, min(1439, jst_dt.hour * 60 + jst_dt.minute))


def _merge_intervals(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    if not intervals:
        return []
    normalized = sorted((max(0, s), min(1440, e)) for s, e in intervals if e > s)
    if not normalized:
        return []
    merged: list[list[int]] = [[normalized[0][0], normalized[0][1]]]
    for start, end in normalized[1:]:
        tail = merged[-1]
        if start <= tail[1]:
            tail[1] = max(tail[1], end)
        else:
            merged.append([start, end])
    return [(s, e) for s, e in merged]


def _minutes_label(minutes_of_day: int) -> str:
    m = max(0, min(1440, int(minutes_of_day)))
    hh, mm = divmod(m, 60)
    return f"{hh:02d}:{mm:02d}"


def _split_segment_by_night(
    *,
    start: int,
    end: int,
    base_kind: str,
    night_ranges: list[tuple[int, int]],
) -> list[tuple[int, int, str]]:
    if end <= start:
        return []
    points = {start, end}
    for ns, ne in night_ranges:
        if start < ns < end:
            points.add(ns)
        if start < ne < end:
            points.add(ne)
    ordered = sorted(points)
    out: list[tuple[int, int, str]] = []
    for idx in range(len(ordered) - 1):
        seg_s = ordered[idx]
        seg_e = ordered[idx + 1]
        if seg_e <= seg_s:
            continue
        mid = seg_s + ((seg_e - seg_s) / 2.0)
        in_night = any(ns <= mid < ne for ns, ne in night_ranges)
        if base_kind == "break":
            kind = "break"
        elif in_night and base_kind == "overtime":
            kind = "night-overtime"
        elif in_night:
            kind = "night"
        elif base_kind == "overtime":
            kind = "overtime"
        else:
            kind = "work"
        out.append((seg_s, seg_e, kind))
    return out


def _build_today_progress(record: dict[str, Any], *, now_dt: datetime | None = None) -> dict[str, Any]:
    now = (now_dt or _jst_now()).astimezone(JST)
    runtime = store.get_runtime_policy()
    required = max(0, int(runtime.get("required_work_minutes") or 480))
    night_start = _time_text_to_minutes(str(runtime.get("night_start") or "22:00"), 22 * 60)
    night_end = _time_text_to_minutes(str(runtime.get("night_end") or "05:00"), 5 * 60)
    if night_start < night_end:
        night_ranges = [(night_start, night_end)]
    else:
        night_ranges = [(0, night_end), (night_start, 1440)]

    event_rows = sorted(
        [ev for ev in (record.get("events") or []) if str(ev.get("event_type") or "") in {EVENT_IN, EVENT_OUT, EVENT_OUTING, EVENT_RETURN}],
        key=lambda ev: str(ev.get("event_time") or ""),
    )
    target_day_text = str(record.get("date") or "")
    try:
        target_day = date.fromisoformat(target_day_text) if target_day_text else now.date()
    except ValueError:
        target_day = now.date()

    work_intervals: list[tuple[int, int]] = []
    break_intervals: list[tuple[int, int]] = []
    state = "NOT_STARTED"
    active_work_start: int | None = None
    active_break_start: int | None = None

    for ev in event_rows:
        event_dt = ev.get("event_dt")
        if not isinstance(event_dt, datetime):
            event_raw = str(ev.get("event_time") or "")
            if not event_raw:
                continue
            try:
                event_dt = datetime.fromisoformat(event_raw)
                if event_dt.tzinfo is None:
                    event_dt = event_dt.replace(tzinfo=JST)
            except Exception:
                continue
        event_dt = event_dt.astimezone(JST)
        minute = _minutes_of_day(event_dt)
        event_type = str(ev.get("event_type") or "")
        if event_type == EVENT_IN:
            state = "WORKING"
            active_work_start = minute
            active_break_start = None
        elif event_type == EVENT_OUTING and state == "WORKING":
            if active_work_start is not None and minute > active_work_start:
                work_intervals.append((active_work_start, minute))
            state = "OUTING"
            active_work_start = None
            active_break_start = minute
        elif event_type == EVENT_RETURN and state == "OUTING":
            if active_break_start is not None and minute > active_break_start:
                break_intervals.append((active_break_start, minute))
            state = "WORKING"
            active_break_start = None
            active_work_start = minute
        elif event_type == EVENT_OUT and state in {"WORKING", "OUTING"}:
            if state == "WORKING" and active_work_start is not None and minute > active_work_start:
                work_intervals.append((active_work_start, minute))
            elif state == "OUTING" and active_break_start is not None and minute > active_break_start:
                break_intervals.append((active_break_start, minute))
            state = "DONE"
            active_work_start = None
            active_break_start = None

    if target_day == now.date():
        now_min = _minutes_of_day(now)
        if state == "WORKING" and active_work_start is not None and now_min > active_work_start:
            work_intervals.append((active_work_start, now_min))
        elif state == "OUTING" and active_break_start is not None and now_min > active_break_start:
            break_intervals.append((active_break_start, now_min))
    else:
        now_min = 0

    work_intervals = _merge_intervals(work_intervals)
    break_intervals = _merge_intervals(break_intervals)

    worked_total = sum(max(0, end - start) for start, end in work_intervals)
    overtime_total = max(0, worked_total - required)

    timeline: list[tuple[int, int, str]] = []
    regular_remaining = required
    for start, end in work_intervals:
        duration = end - start
        if duration <= 0:
            continue
        regular_chunk = min(duration, max(0, regular_remaining))
        if regular_chunk > 0:
            timeline.extend(_split_segment_by_night(start=start, end=start + regular_chunk, base_kind="work", night_ranges=night_ranges))
        if duration > regular_chunk:
            timeline.extend(_split_segment_by_night(start=start + regular_chunk, end=end, base_kind="overtime", night_ranges=night_ranges))
        regular_remaining = max(0, regular_remaining - regular_chunk)

    for start, end in break_intervals:
        timeline.extend(_split_segment_by_night(start=start, end=end, base_kind="break", night_ranges=night_ranges))

    timeline.sort(key=lambda item: (item[0], item[1]))
    segments: list[dict[str, Any]] = []
    for start, end, kind in timeline:
        width = end - start
        if width <= 0:
            continue
        segments.append(
            {
                "start_minute": start,
                "end_minute": end,
                "start_pct": round((start / 1440.0) * 100.0, 4),
                "width_pct": round((width / 1440.0) * 100.0, 4),
                "kind": kind,
                "minutes": width,
                "start_label": _minutes_label(start),
                "end_label": _minutes_label(end),
            }
        )

    return {
        "segments": segments,
        "now_pct": round((now_min / 1440.0) * 100.0, 4),
        "worked_minutes": worked_total,
        "overtime_minutes": overtime_total,
        "required_work_minutes": required,
        "night_start": str(runtime.get("night_start") or "22:00"),
        "night_end": str(runtime.get("night_end") or "05:00"),
        "has_segments": bool(segments),
    }


def _today_context(*, user_id: str, target: date) -> dict[str, Any]:
    attendance = store.attendance_state(user_id=user_id, target_date=target)
    rec = _normalize_day_record_for_ui(attendance["record"])
    progress = _build_today_progress(rec)
    return {
        "record": rec,
        "status": attendance["status"],
        "is_outing_now": attendance["is_outing_now"],
        "primary_action": attendance["primary_action"],
        "primary_label": attendance["primary_label"],
        "primary_endpoint": attendance["primary_endpoint"],
        "primary_disabled": attendance["primary_disabled"],
        "secondary_action": attendance["secondary_action"],
        "secondary_label": attendance["secondary_label"],
        "secondary_endpoint": attendance["secondary_endpoint"],
        "secondary_disabled": attendance["secondary_disabled"],
        "progress": progress,
    }


def _parse_datetime_local(raw: str | None) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.replace(tzinfo=JST)
        except ValueError:
            continue
    return None


def _parse_date(raw: str | None, default: date) -> date:
    text = str(raw or "").strip()
    if not text:
        return default
    try:
        return date.fromisoformat(text)
    except ValueError:
        return default


def _parse_month(raw: str | None, default: date) -> str:
    text = str(raw or "").strip()
    if not text:
        return default.strftime("%Y-%m")
    try:
        datetime.strptime(text + "-01", "%Y-%m-%d")
        return text
    except ValueError:
        return default.strftime("%Y-%m")


def _minutes_to_hhmm(minutes: int) -> str:
    m = max(0, int(minutes))
    hh, mm = divmod(m, 60)
    return f"{hh:02d}:{mm:02d}"


def _wants_json(request: Request) -> bool:
    accept = (request.headers.get("accept") or "").lower()
    content_type = (request.headers.get("content-type") or "").lower()
    return "application/json" in accept or "application/json" in content_type


app = FastAPI(title="SHOWA TIME CARD")
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")
templates = _resolve_templates()
store = TimeclockStore.from_env()

_https_only = _parse_bool(os.getenv("CLOUDLOG_HTTPS_ONLY", "1"), default=True)
_allowed_hosts = _parse_allowed_hosts(os.getenv("CLOUDLOG_ALLOWED_HOSTS", "localhost,127.0.0.1"))
_trusted_proxies = _parse_trusted_proxies(os.getenv("CLOUDLOG_TRUSTED_PROXIES", "*"))
_session_secret = (os.getenv("CLOUDLOG_SECRET_KEY", "") or "").strip() or secrets.token_urlsafe(48)
_session_max_age = int((os.getenv("CLOUDLOG_SESSION_MAX_AGE_SECONDS", "43200") or "43200").strip())
_remember_days = int((os.getenv("CLOUDLOG_REMEMBER_MAX_AGE_DAYS", "30") or "30").strip())
_remember_cookie = "cloudlog_remember"
_saved_email_cookie = "cloudlog_saved_email"
_serializer = URLSafeTimedSerializer(_session_secret, salt="cloudlog-remember")
_hidden_user_emails = _parse_hidden_user_emails(os.getenv("CLOUDLOG_HIDDEN_USER_EMAILS", ""))
_always_remember_emails = _parse_hidden_user_emails(os.getenv("CLOUDLOG_ALWAYS_REMEMBER_EMAILS", ""))
_effective_hidden_user_emails = set(_hidden_user_emails) | set(_always_remember_emails)

app.add_middleware(ProxyHeadersMiddleware, trusted_hosts=_trusted_proxies)
app.add_middleware(TrustedHostMiddleware, allowed_hosts=_allowed_hosts)


@app.on_event("startup")
def _startup() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        store.refresh_holidays_cache(force=False)
    except Exception:
        log.exception("holiday refresh failed")


@app.middleware("http")
async def _auth_middleware(request: Request, call_next):  # type: ignore
    path = request.url.path
    public_paths = {
        "/health",
        "/login",
        "/auth/login",
        "/register",
    }

    if path.startswith("/static") or path in public_paths:
        return await call_next(request)

    uid = _session_user_id(request.session)
    if uid is None:
        remember_token = request.cookies.get(_remember_cookie)
        if remember_token:
            try:
                payload = _serializer.loads(remember_token, max_age=_remember_days * 86400)
                remember_user_id = str(payload.get("uid") or "")
                remember_sig = str(payload.get("sig") or "")
                user = store.get_user_by_id(remember_user_id)
                if user and user["is_active"]:
                    expected = secrets.compare_digest(remember_sig, _password_signature(user["password_hash"]))
                    if expected:
                        _login_session(request.session, user_id=user["user_id"], email=user["email"])
                        uid = user["user_id"]
            except BadData:
                pass

    if uid is None:
        if path.startswith("/api/"):
            return JSONResponse(status_code=401, content={"ok": False, "error": "authentication required"})
        return RedirectResponse(url=f"/login?next={quote(path)}", status_code=303)

    user = store.get_user_by_id(uid)
    if user is None or not user["is_active"]:
        _logout_session(request.session)
        if path.startswith("/api/"):
            return JSONResponse(status_code=401, content={"ok": False, "error": "invalid session"})
        return RedirectResponse(url="/login", status_code=303)

    request.state.current_user = user
    return await call_next(request)


app.add_middleware(
    SessionMiddleware,
    secret_key=_session_secret,
    session_cookie="cloudlog_session",
    https_only=_https_only,
    same_site="lax",
    max_age=_session_max_age,
)


def _password_signature(password_hash: str) -> str:
    import hashlib

    return hashlib.sha256(password_hash.encode("utf-8")).hexdigest()[:24]


def _require_user(request: Request) -> dict[str, Any]:
    user = getattr(request.state, "current_user", None)
    if not user:
        uid = _session_user_id(request.session)
        if not uid:
            raise HTTPException(status_code=401)
        row = store.get_user_by_id(uid)
        if row is None:
            raise HTTPException(status_code=401)
        request.state.current_user = row
        return row
    return user


def _require_admin(request: Request) -> dict[str, Any]:
    user = _require_user(request)
    if ROLE_ORDER.get(user["role"], 0) < ROLE_ORDER[ROLE_ADMIN]:
        raise HTTPException(status_code=403)
    return user


def _is_hidden_user(user: dict[str, Any]) -> bool:
    if not _effective_hidden_user_emails:
        return False
    email = str(user.get("email") or "").strip().lower()
    return email in _effective_hidden_user_emails


def _visible_users(users: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not _effective_hidden_user_emails:
        return users
    return [u for u in users if not _is_hidden_user(u)]


def _render(request: Request, template_name: str, context: dict[str, Any], *, status_code: int = 200) -> HTMLResponse:
    user = getattr(request.state, "current_user", None)
    merged = {
        "request": request,
        "csrf_token": _ensure_csrf_token(request.session),
        "current_user": user,
        "auth_email": _session_user_email(request.session),
        "ROLE_ADMIN": ROLE_ADMIN,
        "today": _jst_today().isoformat(),
        "minutes_to_hhmm": _minutes_to_hhmm,
        "fmt_hhmm": _fmt_hhmm,
        "fmt_hhmm_dt": _fmt_hhmm_dt,
        **context,
    }
    return templates.TemplateResponse(template_name, merged, status_code=status_code)


def _error_or_redirect(request: Request, *, redirect_to: str, message: str, code: int = 400) -> JSONResponse | RedirectResponse:
    if _wants_json(request):
        return JSONResponse(status_code=code, content={"ok": False, "error": message})
    return RedirectResponse(url=f"{redirect_to}{'&' if '?' in redirect_to else '?'}error={quote(message)}", status_code=303)


def _success_or_redirect(request: Request, *, redirect_to: str, payload: dict[str, Any], flash: str) -> JSONResponse | RedirectResponse:
    if _wants_json(request):
        body = {"ok": True}
        body.update(payload)
        return JSONResponse(content=body)
    return RedirectResponse(url=f"{redirect_to}{'&' if '?' in redirect_to else '?'}msg={quote(flash)}", status_code=303)


@app.get("/health", response_class=PlainTextResponse)
def health() -> str:
    return "ok"


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request, next: str = "/today"):  # noqa: A002
    if _session_user_id(request.session):
        return RedirectResponse(url="/today", status_code=303)
    return _render(
        request,
        "login.html",
        {
            "title": "ログイン",
            "next": _safe_next(next),
            "error": str(request.query_params.get("error") or ""),
            "saved_email": str(request.cookies.get(_saved_email_cookie) or ""),
        },
    )


@app.post("/auth/login")
async def auth_login(request: Request):
    form = await request.form()
    email = str(form.get("email") or "").strip().lower()
    password = str(form.get("password") or "")
    remember = str(form.get("remember") or "") == "1"
    if email in _always_remember_emails:
        remember = True
    next_path = _safe_next(str(form.get("next") or "/today"))

    user = store.authenticate_user(email=email, password=password)
    if user is None:
        return _render(
            request,
            "login.html",
            {
                "title": "ログイン",
                "next": next_path,
                "error": "メールアドレスまたはパスワードが違います",
                "saved_email": email,
            },
            status_code=401,
        )

    _login_session(request.session, user_id=user["user_id"], email=user["email"])
    resp = RedirectResponse(url=next_path, status_code=303)
    resp.set_cookie(_saved_email_cookie, user["email"], max_age=31536000, secure=_https_only, samesite="lax")
    if remember:
        token = _serializer.dumps({"uid": user["user_id"], "sig": _password_signature(user["password_hash"])})
        resp.set_cookie(
            _remember_cookie,
            token,
            httponly=True,
            secure=_https_only,
            samesite="lax",
            max_age=_remember_days * 86400,
        )
    else:
        resp.delete_cookie(_remember_cookie)
    return resp


@app.post("/auth/logout")
async def auth_logout(request: Request):
    form = await request.form()
    csrf_token = str(form.get("csrf_token") or "")
    if not _validate_csrf(request.session, csrf_token):
        return RedirectResponse(url="/today", status_code=303)

    _logout_session(request.session)
    resp = RedirectResponse(url="/login", status_code=303)
    resp.delete_cookie(_remember_cookie)
    return resp


@app.post("/logout")
async def logout_legacy(request: Request):
    return await auth_logout(request)


@app.get("/me")
def me(request: Request):
    user = _require_user(request)
    return {
        "ok": True,
        "user": {
            "user_id": user["user_id"],
            "email": user["email"],
            "name": user["name"],
            "role": user["role"],
            "is_active": user["is_active"],
        },
    }


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    _require_user(request)
    return RedirectResponse(url="/today", status_code=303)


@app.get("/today", response_class=HTMLResponse)
def today_page(request: Request):
    user = _require_user(request)
    target = _jst_today()
    today_ctx = _today_context(user_id=user["user_id"], target=target)
    rec = today_ctx["record"]
    month_start, month_end = store.get_payroll_period_for_user(user_id=user["user_id"], anchor=target)
    _, summary = store.daily_records(user_id=user["user_id"], period_start=month_start, period_end=month_end)
    mom = _work_minutes_mom(user["user_id"], anchor=target)
    closing_day = store.get_user_closing_day(user["user_id"])
    recent_events = _recent_events_for_user(user["user_id"], limit=10)

    return _render(
        request,
        "today.html",
        {
            "title": "本日の打刻",
            "status": today_ctx["status"],
            "record": rec,
            "month_start": month_start.isoformat(),
            "month_end": month_end.isoformat(),
            "summary": summary,
            "closing_day": closing_day,
            "mom": mom,
            "recent_events": recent_events,
            "is_outing_now": today_ctx["is_outing_now"],
            "primary_action": today_ctx["primary_action"],
            "primary_label": today_ctx["primary_label"],
            "primary_endpoint": today_ctx["primary_endpoint"],
            "primary_disabled": today_ctx["primary_disabled"],
            "secondary_action": today_ctx["secondary_action"],
            "secondary_label": today_ctx["secondary_label"],
            "secondary_endpoint": today_ctx["secondary_endpoint"],
            "secondary_disabled": today_ctx["secondary_disabled"],
            "progress": today_ctx["progress"],
            "flash_error": str(request.query_params.get("error") or ""),
            "flash_message": str(request.query_params.get("msg") or ""),
        },
    )


@app.get("/attendance/today")
def attendance_today(request: Request):
    user = _require_user(request)
    target = _jst_today()
    today_ctx = _today_context(user_id=user["user_id"], target=target)
    rec = today_ctx["record"]
    period_start, period_end = store.get_payroll_period_for_user(user_id=user["user_id"], anchor=target)
    _, summary = store.daily_records(user_id=user["user_id"], period_start=period_start, period_end=period_end)
    mom = _work_minutes_mom(user["user_id"], anchor=target)
    recent_events = _recent_events_for_user(user["user_id"], limit=10)
    return {
        "ok": True,
        "date": target.isoformat(),
        "status": today_ctx["status"],
        "is_outing_now": today_ctx["is_outing_now"],
        "record": rec,
        "summary": summary,
        "mom": mom,
        "recent_events": recent_events,
        "primary_action": today_ctx["primary_action"],
        "primary_label": today_ctx["primary_label"],
        "primary_endpoint": today_ctx["primary_endpoint"],
        "primary_disabled": today_ctx["primary_disabled"],
        "secondary_action": today_ctx["secondary_action"],
        "secondary_label": today_ctx["secondary_label"],
        "secondary_endpoint": today_ctx["secondary_endpoint"],
        "secondary_disabled": today_ctx["secondary_disabled"],
        "progress": today_ctx["progress"],
    }


@app.get("/attendance", response_class=HTMLResponse)
def attendance_alias(request: Request):
    _require_user(request)
    return RedirectResponse(url="/today", status_code=303)


async def _clock_action(request: Request, event_type: str, ok_message: str):
    user = _require_user(request)
    form = await request.form()
    csrf_token = str(form.get("csrf_token") or "")
    if not _validate_csrf(request.session, csrf_token):
        return _error_or_redirect(request, redirect_to="/today", message="CSRF tokenが不正です", code=400)
    note = str(form.get("note") or "").strip()
    try:
        event = store.clock_action(
            user_id=user["user_id"],
            action=event_type,
            note=note,
            ip=request.client.host if request.client else "",
            user_agent=str(request.headers.get("user-agent") or ""),
        )
    except ValueError as exc:
        code = str(exc)
        msg_map = {
            "already_clocked_in": "既に出勤済みです",
            "already_clocked_out": "既に退勤済みです",
            "clock_in_required": "先に出勤打刻が必要です",
            "already_outing": "すでに外出中です",
            "outing_required": "先に外出打刻が必要です",
            "already_returned": "既に戻り打刻済みです",
            "invalid_action": "不正な打刻操作です",
        }
        return _error_or_redirect(request, redirect_to="/today", message=msg_map.get(code, "打刻に失敗しました"), code=400)

    return _success_or_redirect(
        request,
        redirect_to="/today",
        payload={"event": {k: v for k, v in event.items() if k != "event_dt"}},
        flash=ok_message,
    )


@app.post("/events/clock-in")
async def clock_in(request: Request):
    return await _clock_action(request, EVENT_IN, "出勤しました")


@app.post("/events/clock-out")
async def clock_out(request: Request):
    return await _clock_action(request, EVENT_OUT, "退勤しました")


@app.post("/events/outing")
async def clock_outing(request: Request):
    return await _clock_action(request, EVENT_OUTING, "外出しました")


@app.post("/events/return")
async def clock_return(request: Request):
    return await _clock_action(request, EVENT_RETURN, "戻りました")


@app.post("/events/{event_id}/edit")
async def edit_event(request: Request, event_id: str):
    user = _require_user(request)
    form = await request.form()
    csrf_token = str(form.get("csrf_token") or "")
    if not _validate_csrf(request.session, csrf_token):
        return _error_or_redirect(request, redirect_to="/today", message="CSRF tokenが不正です", code=400)

    target = store.get_event_by_id(event_id)
    if target is None:
        return _error_or_redirect(request, redirect_to="/today", message="対象の打刻が見つかりません", code=404)

    is_admin = ROLE_ORDER.get(str(user.get("role") or ""), 0) >= ROLE_ORDER[ROLE_ADMIN]
    if str(target.get("user_id") or "") != str(user.get("user_id") or "") and not is_admin:
        return _error_or_redirect(request, redirect_to="/today", message="この打刻は修正できません", code=403)

    event_type = str(form.get("event_type") or "").strip().upper()
    if event_type not in {EVENT_IN, EVENT_OUT, EVENT_OUTING, EVENT_RETURN}:
        return _error_or_redirect(request, redirect_to="/today", message="打刻種別が不正です", code=400)

    event_time = _parse_datetime_local(form.get("event_time"))
    if event_time is None:
        return _error_or_redirect(request, redirect_to="/today", message="打刻日時が不正です", code=400)

    note = str(form.get("note") or "").strip()

    try:
        updated = store.edit_event(
            actor_user_id=str(user.get("user_id") or ""),
            target_event_id=event_id,
            event_type=event_type,
            event_dt=event_time,
            note=note,
            ip=request.client.host if request.client else "",
            user_agent=str(request.headers.get("user-agent") or ""),
        )
    except ValueError as exc:
        code = str(exc)
        msg_map = {
            "event_not_found": "対象の打刻が見つかりません",
            "invalid_event_type": "打刻種別が不正です",
        }
        return _error_or_redirect(request, redirect_to="/today", message=msg_map.get(code, "打刻修正に失敗しました"), code=400)

    return _success_or_redirect(
        request,
        redirect_to="/today",
        payload={"event": {k: v for k, v in updated.items() if k != "event_dt"}},
        flash="打刻修正を保存しました",
    )


@app.get("/records", response_class=HTMLResponse)
def records_page(request: Request, period: str | None = None, from_date: str | None = None, to_date: str | None = None):
    user = _require_user(request)

    today = _jst_today()
    month_key = _parse_month(period, today)
    period_start, period_end = store.get_payroll_period_by_month(month_key)

    if from_date and to_date:
        period_start = _parse_date(from_date, period_start)
        period_end = _parse_date(to_date, period_end)

    rows, summary = store.daily_records(user_id=user["user_id"], period_start=period_start, period_end=period_end)
    return _render(
        request,
        "records.html",
        {
            "title": "打刻履歴",
            "period": month_key,
            "period_start": period_start.isoformat(),
            "period_end": period_end.isoformat(),
            "rows": rows,
            "summary": summary,
            "flash_error": str(request.query_params.get("error") or ""),
            "flash_message": str(request.query_params.get("msg") or ""),
        },
    )


@app.put("/records/{work_date}")
async def edit_record_api(
    request: Request,
    work_date: str,
):
    user = _require_user(request)
    payload = await request.json()
    try:
        d = date.fromisoformat(work_date)
        store.edit_day_record(
            actor_user_id=user["user_id"],
            target_user_id=user["user_id"],
            target_date=d,
            clock_in_at=_parse_datetime_local(payload.get("clock_in_at")),
            clock_out_at=_parse_datetime_local(payload.get("clock_out_at")),
            outing_at=_parse_datetime_local(payload.get("outing_at")),
            return_at=_parse_datetime_local(payload.get("return_at")),
            note=str(payload.get("note") or ""),
        )
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})
    return {"ok": True}


@app.post("/records/{work_date}")
async def edit_record_form(request: Request, work_date: str):
    user = _require_user(request)
    form = await request.form()
    csrf_token = str(form.get("csrf_token") or "")
    if not _validate_csrf(request.session, csrf_token):
        return RedirectResponse(url="/records?error=CSRF%20tokenが不正です", status_code=303)

    try:
        d = date.fromisoformat(work_date)
        store.edit_day_record(
            actor_user_id=user["user_id"],
            target_user_id=user["user_id"],
            target_date=d,
            clock_in_at=_parse_datetime_local(form.get("clock_in_at")),
            clock_out_at=_parse_datetime_local(form.get("clock_out_at")),
            outing_at=_parse_datetime_local(form.get("outing_at")),
            return_at=_parse_datetime_local(form.get("return_at")),
            note=str(form.get("note") or ""),
        )
    except ValueError:
        return RedirectResponse(url="/records?error=入力値が不正です", status_code=303)

    return RedirectResponse(url="/records?msg=修正を保存しました", status_code=303)


@app.get("/leave", response_class=HTMLResponse)
def leave_page(request: Request):
    user = _require_user(request)
    mine = store.list_leave_requests(user_id=user["user_id"])
    settings = store.get_settings()
    return _render(
        request,
        "leave.html",
        {
            "title": "休暇申請",
            "requests": mine,
            "special_leave_types": settings["special_leave_types_json"],
            "flash_error": str(request.query_params.get("error") or ""),
            "flash_message": str(request.query_params.get("msg") or ""),
        },
    )


@app.get("/menu", response_class=HTMLResponse)
def menu_page(request: Request):
    user = _require_user(request)
    items: list[dict[str, Any]] = [
        {"title": "本日", "href": "/today", "description": "現在時刻と打刻を確認"},
        {"title": "打刻履歴", "href": "/records", "description": "日次の勤怠履歴を確認"},
        {"title": "休暇申請", "href": "/leave", "description": "有給や特別休暇を申請"},
    ]
    if user.get("role") == ROLE_ADMIN:
        items.extend(
            [
                {"title": "集計・承認", "href": "/admin/summary", "description": "月次集計と承認キュー"},
                {"title": "ユーザー管理", "href": "/admin/users", "description": "ユーザーの追加・権限設定"},
                {"title": "管理設定", "href": "/admin/settings", "description": "締め日や勤怠ルール設定"},
            ]
        )
    return _render(
        request,
        "menu.html",
        {
            "title": "メニュー",
            "menu_items": items,
            "flash_error": str(request.query_params.get("error") or ""),
            "flash_message": str(request.query_params.get("msg") or ""),
        },
    )


@app.post("/leave-requests")
async def create_leave_request(request: Request):
    user = _require_user(request)
    form = await request.form()
    csrf_token = str(form.get("csrf_token") or "")
    if not _validate_csrf(request.session, csrf_token):
        return _error_or_redirect(request, redirect_to="/leave", message="CSRF tokenが不正です", code=400)

    leave_date = str(form.get("leave_date") or "").strip()
    leave_type = str(form.get("leave_type") or LEAVE_PAID).strip()
    leave_name = str(form.get("leave_name") or "").strip()
    note = str(form.get("note") or "").strip()

    if not leave_date:
        return _error_or_redirect(request, redirect_to="/leave", message="日付を入力してください", code=400)
    if leave_type not in {LEAVE_PAID, LEAVE_SPECIAL, LEAVE_COMPANY_DESIGNATED, LEAVE_OTHER}:
        return _error_or_redirect(request, redirect_to="/leave", message="休暇種別が不正です", code=400)

    if not leave_name:
        if leave_type == LEAVE_PAID:
            leave_name = "有給"
        elif leave_type == LEAVE_SPECIAL:
            leave_name = "特別休暇"
        elif leave_type == LEAVE_COMPANY_DESIGNATED:
            leave_name = "会社指定有給"
        else:
            leave_name = "その他"

    store.create_leave_request(
        user_id=user["user_id"],
        leave_date=leave_date,
        leave_type=leave_type,
        leave_name=leave_name,
        note=note,
    )
    return _success_or_redirect(request, redirect_to="/leave", payload={}, flash="休暇申請を登録しました")


@app.get("/leave-requests")
def list_leave_requests(request: Request):
    user = _require_user(request)
    if user["role"] == ROLE_ADMIN:
        rows = store.list_leave_requests(user_id=None)
    else:
        rows = store.list_leave_requests(user_id=user["user_id"])
    return {"ok": True, "leave_requests": rows}


@app.put("/leave-requests/{leave_id}/approve")
def approve_leave_api(request: Request, leave_id: str):
    admin = _require_admin(request)
    updated = store.decide_leave_request(leave_id=leave_id, actor_user_id=admin["user_id"], approve=True)
    return {"ok": True, "leave_request": updated}


@app.put("/leave-requests/{leave_id}/reject")
def reject_leave_api(request: Request, leave_id: str):
    admin = _require_admin(request)
    updated = store.decide_leave_request(leave_id=leave_id, actor_user_id=admin["user_id"], approve=False)
    return {"ok": True, "leave_request": updated}


@app.post("/leave-requests/{leave_id}/approve")
async def approve_leave_form(request: Request, leave_id: str):
    _require_admin(request)
    form = await request.form()
    csrf_token = str(form.get("csrf_token") or "")
    if not _validate_csrf(request.session, csrf_token):
        return RedirectResponse(url="/admin/summary?error=CSRF%20tokenが不正です", status_code=303)
    admin = _require_admin(request)
    store.decide_leave_request(leave_id=leave_id, actor_user_id=admin["user_id"], approve=True)
    return RedirectResponse(url="/admin/summary?msg=休暇申請を承認しました", status_code=303)


@app.post("/leave-requests/{leave_id}/reject")
async def reject_leave_form(request: Request, leave_id: str):
    _require_admin(request)
    form = await request.form()
    csrf_token = str(form.get("csrf_token") or "")
    if not _validate_csrf(request.session, csrf_token):
        return RedirectResponse(url="/admin/summary?error=CSRF%20tokenが不正です", status_code=303)
    admin = _require_admin(request)
    store.decide_leave_request(leave_id=leave_id, actor_user_id=admin["user_id"], approve=False)
    return RedirectResponse(url="/admin/summary?msg=休暇申請を却下しました", status_code=303)


@app.get("/admin/users", response_class=HTMLResponse)
def admin_users_page(request: Request):
    _require_admin(request)
    users = _visible_users(store.list_users())
    if _wants_json(request):
        return JSONResponse(content={"ok": True, "users": users})
    return _render(
        request,
        "admin_users.html",
        {
            "title": "ユーザー管理",
            "users": users,
            "flash_error": str(request.query_params.get("error") or ""),
            "flash_message": str(request.query_params.get("msg") or ""),
        },
    )


@app.get("/admin/settings", response_class=HTMLResponse)
def admin_settings_page(request: Request):
    _require_admin(request)
    settings = store.get_settings()
    runtime_policy = store.get_runtime_policy()
    return _render(
        request,
        "admin_settings.html",
        {
            "title": "管理設定",
            "settings": settings,
            "runtime_policy": runtime_policy,
            "flash_error": str(request.query_params.get("error") or ""),
            "flash_message": str(request.query_params.get("msg") or ""),
        },
    )


@app.get("/admin/summary", response_class=HTMLResponse)
def admin_summary_page(request: Request, period: str | None = None, user_id: str | None = None):
    _require_admin(request)
    month_key = _parse_month(period, _jst_today())
    period_start, period_end = store.get_payroll_period_by_month(month_key)
    summaries = store.summary_for_period(user_id=user_id, period_start=period_start, period_end=period_end)
    leaves = store.list_leave_requests(user_id=None)
    users = _visible_users(store.list_users())
    summaries = [item for item in summaries if not _is_hidden_user(item["user"])]

    anomaly_rows: list[dict[str, Any]] = []
    for item in summaries:
        sm = item["summary"]
        if sm["missing_count"] > 0 or sm["overtime_minutes"] > 0 or sm["lateness_count"] > 0:
            anomaly_rows.append(
                {
                    "user": item["user"],
                    "missing_count": sm["missing_count"],
                    "overtime_hhmm": _minutes_to_hhmm(sm["overtime_minutes"]),
                    "lateness_count": sm["lateness_count"],
                }
            )

    return _render(
        request,
        "admin_summary.html",
        {
            "title": "集計・承認",
            "period": month_key,
            "period_start": period_start.isoformat(),
            "period_end": period_end.isoformat(),
            "summaries": summaries,
            "users": users,
            "selected_user_id": user_id or "",
            "leave_requests": leaves,
            "anomalies": anomaly_rows,
            "flash_error": str(request.query_params.get("error") or ""),
            "flash_message": str(request.query_params.get("msg") or ""),
        },
    )


@app.post("/admin/users")
async def admin_users_create(request: Request):
    _require_admin(request)
    form = await request.form()
    if not _validate_csrf(request.session, str(form.get("csrf_token") or "")):
        return _error_or_redirect(request, redirect_to="/admin/users", message="CSRF tokenが不正です", code=400)

    email = str(form.get("email") or "").strip().lower()
    name = str(form.get("name") or "").strip()
    password = str(form.get("password") or "")
    role = str(form.get("role") or "user")
    try:
        closing_day = int(str(form.get("closing_day") or "20"))
    except ValueError:
        closing_day = 20
    active = str(form.get("is_active") or "1") == "1"

    if len(password) < 8:
        return _error_or_redirect(request, redirect_to="/admin/users", message="パスワードは8文字以上で入力してください", code=400)

    try:
        user = store.create_user(
            email=email,
            name=name,
            password=password,
            role=role,
            closing_day=closing_day,
            is_active=active,
        )
    except ValueError as exc:
        return _error_or_redirect(request, redirect_to="/admin/users", message=str(exc), code=400)

    return _success_or_redirect(request, redirect_to="/admin/users", payload={"user": user}, flash="ユーザーを作成しました")


@app.put("/admin/users/{user_id}")
async def admin_users_update_api(request: Request, user_id: str):
    _require_admin(request)
    payload = await request.json()
    user = store.update_user(
        user_id=user_id,
        name=payload.get("name"),
        role=payload.get("role"),
        closing_day=payload.get("closing_day"),
        is_active=payload.get("is_active"),
        password=payload.get("password"),
    )
    return {"ok": True, "user": user}


@app.post("/admin/users/{user_id}")
async def admin_users_update_form(request: Request, user_id: str):
    _require_admin(request)
    form = await request.form()
    if not _validate_csrf(request.session, str(form.get("csrf_token") or "")):
        return RedirectResponse(url="/admin/users?error=CSRF%20tokenが不正です", status_code=303)

    name = str(form.get("name") or "").strip()
    role = str(form.get("role") or "user")
    try:
        closing_day = int(str(form.get("closing_day") or "20"))
    except ValueError:
        closing_day = 20
    active = str(form.get("is_active") or "1") == "1"
    password = str(form.get("password") or "").strip()

    try:
        store.update_user(
            user_id=user_id,
            name=name,
            role=role,
            closing_day=closing_day,
            is_active=active,
            password=password or None,
        )
    except ValueError:
        return RedirectResponse(url="/admin/users?error=ユーザー更新に失敗しました", status_code=303)

    return RedirectResponse(url="/admin/users?msg=ユーザーを更新しました", status_code=303)


@app.put("/admin/settings")
async def admin_settings_update_api(request: Request):
    admin = _require_admin(request)
    payload = await request.json()
    updated = _update_settings_from_payload(actor_user_id=admin["user_id"], payload=payload)
    return {"ok": True, "settings": updated}


@app.post("/admin/settings")
async def admin_settings_update_form(request: Request):
    admin = _require_admin(request)
    form = await request.form()
    if not _validate_csrf(request.session, str(form.get("csrf_token") or "")):
        return RedirectResponse(url="/admin/settings?error=CSRF%20tokenが不正です", status_code=303)

    payload = {
        "payroll_cutoff_day": int(str(form.get("payroll_cutoff_day") or "20")),
        "scheduled_start_time": str(form.get("scheduled_start_time") or "08:55"),
        "scheduled_end_time": str(form.get("scheduled_end_time") or "17:55"),
        "scheduled_work_minutes": int(str(form.get("scheduled_work_minutes") or "480")),
        "grace_minutes": int(str(form.get("grace_minutes") or "5")),
        "break_policy_type": str(form.get("break_policy_type") or "fixed"),
        "break_fixed_minutes": int(str(form.get("break_fixed_minutes") or "60")),
        "break_tier_json": _parse_json_text(str(form.get("break_tier_json") or "[]"), []),
        "working_weekdays_json": _parse_json_text(str(form.get("working_weekdays_json") or "[0,1,2,3,4]"), [0, 1, 2, 3, 4]),
        "weekend_work_counts_as_holiday_work": str(form.get("weekend_work_counts_as_holiday_work") or "1") == "1",
        "paid_leave_approval_mode": str(form.get("paid_leave_approval_mode") or "require_admin"),
        "special_leave_types_json": _parse_csv_lines(str(form.get("special_leave_types") or "慶弔,特別休暇")),
        "company_designated_paid_leave_dates_json": _parse_csv_lines(str(form.get("company_designated_paid_leave_dates") or "")),
        "company_custom_holidays_json": _parse_csv_lines(str(form.get("company_custom_holidays") or "")),
        "night_start": str(form.get("night_start") or "22:00"),
        "night_end": str(form.get("night_end") or "05:00"),
        "allow_multiple_clock_in_same_day": str(form.get("allow_multiple_clock_in_same_day") or "0") == "1",
    }

    _update_settings_from_payload(actor_user_id=admin["user_id"], payload=payload)
    return RedirectResponse(url="/admin/settings?msg=設定を更新しました", status_code=303)


def _parse_json_text(raw: str, default: Any) -> Any:
    text = str(raw or "").strip()
    if not text:
        return default
    try:
        return json.loads(text)
    except Exception:
        return default


def _parse_csv_lines(raw: str) -> list[str]:
    parts = []
    for line in str(raw or "").replace("\n", ",").split(","):
        v = line.strip()
        if v:
            parts.append(v)
    return parts


def _update_settings_from_payload(*, actor_user_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    changes = {
        "payroll_cutoff_day": int(payload.get("payroll_cutoff_day", 20)),
        "scheduled_start_time": str(payload.get("scheduled_start_time", "08:55")),
        "scheduled_end_time": str(payload.get("scheduled_end_time", "17:55")),
        "scheduled_work_minutes": int(payload.get("scheduled_work_minutes", 480)),
        "grace_minutes": int(payload.get("grace_minutes", 5)),
        "break_policy_type": str(payload.get("break_policy_type", "fixed")),
        "break_fixed_minutes": int(payload.get("break_fixed_minutes", 60)),
        "break_tier_json": payload.get("break_tier_json", []),
        "working_weekdays_json": payload.get("working_weekdays_json", [0, 1, 2, 3, 4]),
        "weekend_work_counts_as_holiday_work": bool(payload.get("weekend_work_counts_as_holiday_work", True)),
        "paid_leave_approval_mode": str(payload.get("paid_leave_approval_mode", "require_admin")),
        "special_leave_types_json": payload.get("special_leave_types_json", ["慶弔", "特別休暇"]),
        "company_designated_paid_leave_dates_json": payload.get("company_designated_paid_leave_dates_json", []),
        "company_custom_holidays_json": payload.get("company_custom_holidays_json", []),
    }
    updated = store.update_settings(actor_user_id=actor_user_id, changes=changes)
    store.update_runtime_policy(
        changes={
            "closing_day": int(changes["payroll_cutoff_day"]),
            "required_work_minutes": int(changes["scheduled_work_minutes"]),
            "night_start": str(payload.get("night_start") or "22:00"),
            "night_end": str(payload.get("night_end") or "05:00"),
            "allow_multiple_clock_in_same_day": _parse_bool(str(payload.get("allow_multiple_clock_in_same_day", "0")), default=False),
        }
    )
    store.refresh_holidays_cache(force=True)
    return updated


@app.get("/admin/export.csv")
def admin_export_csv(request: Request, period: str | None = None, user: str | None = None):
    _require_admin(request)
    month_key = _parse_month(period, _jst_today())
    period_start, period_end = store.get_payroll_period_by_month(month_key)

    users = _visible_users(store.list_users())
    target_users = [u for u in users if u["is_active"]]
    if user:
        target_users = [u for u in target_users if u["user_id"] == user]

    headers = [
        "組織",
        "関連エリア",
        "氏名",
        "日付",
        "曜日",
        "始業時刻",
        "遅刻事由",
        "外出",
        "戻り",
        "終業時刻",
        "早退事由",
        "欠勤事由",
        "備考",
        "修正区分",
    ]

    sio = io.StringIO()
    writer = csv.DictWriter(sio, fieldnames=headers)
    writer.writeheader()

    for u in target_users:
        for row in store.export_csv_rows(user_id=u["user_id"], period_start=period_start, period_end=period_end):
            writer.writerow(row)

    payload = sio.getvalue().encode("utf-8-sig")
    filename = f"timeclock_{month_key}.csv"
    return StreamingResponse(
        io.BytesIO(payload),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
