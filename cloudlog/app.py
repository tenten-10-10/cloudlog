from __future__ import annotations

import csv
import io
import logging
import os
import secrets
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

from cloudlog.db import (
    AttendanceRow,
    ROLE_ADMIN,
    ROLE_MANAGER,
    ROLE_MEMBER,
    ROLE_ORDER,
    STATUS_APPROVED,
    STATUS_DRAFT,
    STATUS_REJECTED,
    STATUS_SUBMITTED,
    CloudlogDB,
)
from sitewatcher.web.auth import (
    ensure_csrf_token,
    get_user_id,
    get_username,
    hash_password,
    login_session,
    logout_session,
    validate_csrf,
    verify_password,
)


log = logging.getLogger("cloudlog")
JST = timezone(timedelta(hours=9))


def _resolve_data_dir() -> Path:
    raw = os.getenv("CLOUDLOG_DATA_DIR", ".cloudlog")
    p = Path(raw)
    if not p.is_absolute():
        p = (Path.cwd() / p).resolve()
    return p


def _allow_registration() -> bool:
    raw = (os.getenv("CLOUDLOG_ALLOW_REGISTRATION", "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _https_only() -> bool:
    raw = (os.getenv("CLOUDLOG_HTTPS_ONLY", "0") or "0").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _parse_allowed_hosts(raw: str) -> list[str]:
    parts = [p.strip() for p in str(raw or "").split(",") if p.strip()]
    out: list[str] = []
    for part in parts:
        if part == "*":
            continue
        out.append(part)
    if not out:
        return ["localhost", "127.0.0.1"]
    return out


def _parse_trusted_proxies(raw: str) -> list[str] | str:
    text = str(raw or "").strip()
    if text == "*":
        return "*"
    out = [x.strip() for x in text.split(",") if x.strip()]
    return out if out else ["127.0.0.1"]


def _safe_next(next_path: str | None) -> str:
    if not next_path:
        return "/"
    p = str(next_path).strip()
    if not p.startswith("/"):
        return "/"
    if p.startswith("//"):
        return "/"
    if "://" in p:
        return "/"
    return p


def _to_date(value: str | None, default: date) -> date:
    if not value:
        return default
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return default


def _fmt_hours(hours: float) -> str:
    return f"{hours:.2f}"


def _month_bounds(base: date) -> tuple[date, date]:
    first = base.replace(day=1)
    if first.month == 12:
        next_month = date(first.year + 1, 1, 1)
    else:
        next_month = date(first.year, first.month + 1, 1)
    return first, next_month - timedelta(days=1)


def _week_bounds(base: date) -> tuple[date, date]:
    start = base - timedelta(days=base.weekday())
    return start, start + timedelta(days=6)


def _role_at_least(role: str, minimum_role: str) -> bool:
    return ROLE_ORDER.get(role, 0) >= ROLE_ORDER.get(minimum_role, 0)


def _jst_now() -> datetime:
    return datetime.now(tz=JST)


def _jst_today() -> date:
    return _jst_now().date()


def _jst_today_iso() -> str:
    return _jst_today().isoformat()


def _fmt_ts_jst(ts: int | None) -> str:
    if ts is None:
        return "-"
    return datetime.fromtimestamp(int(ts), tz=JST).strftime("%Y-%m-%d %H:%M:%S JST")


def _to_datetime_local(ts: int | None) -> str:
    if ts is None:
        return ""
    return datetime.fromtimestamp(int(ts), tz=JST).strftime("%Y-%m-%dT%H:%M")


def _parse_datetime_local(raw: str | None) -> int | None:
    text = str(raw or "").strip()
    if not text:
        return None
    dt = datetime.strptime(text, "%Y-%m-%dT%H:%M").replace(tzinfo=JST)
    return int(dt.timestamp())


def _worked_seconds(row: AttendanceRow) -> int:
    if row.clock_in_at is None or row.clock_out_at is None:
        return 0
    if row.clock_out_at < row.clock_in_at:
        return 0
    return int(row.clock_out_at - row.clock_in_at)


def _fmt_seconds_as_hours(seconds: int) -> str:
    return f"{(max(0, int(seconds)) / 3600.0):.2f}"


def _attendance_status(row: AttendanceRow | None) -> str:
    if row is None or row.clock_in_at is None:
        return "未出勤"
    if row.clock_out_at is None:
        return "出勤済"
    return "退勤済"


def _parse_hours(raw: str | None) -> float:
    if raw is None:
        return 0.0
    text = str(raw).strip()
    if not text:
        return 0.0
    return max(0.0, float(text))


def _minutes_from_hours(raw: str | None) -> int:
    hours = _parse_hours(raw)
    return int(round(hours * 60.0))


def _api_error(message: str, code: int = 400) -> JSONResponse:
    return JSONResponse(status_code=code, content={"ok": False, "error": message})


def _notify_webhook(db: CloudlogDB, *, event: str, payload: dict[str, Any]) -> None:
    url = db.get_setting("webhook_url", "").strip()
    if not url:
        return
    try:
        requests.post(
            url,
            json={
                "event": event,
                "timestamp": int(time.time()),
                "payload": payload,
            },
            timeout=4,
        )
    except Exception:
        log.exception("Failed to call webhook: %s", event)


DATA_DIR = _resolve_data_dir()
DB_PATH = DATA_DIR / "cloudlog.sqlite3"
DB = CloudlogDB(DB_PATH)

TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

app = FastAPI(title="Cloudlog Clone")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

_session_secret_env = (os.getenv("CLOUDLOG_SECRET_KEY", "") or "").strip()
_session_secret = _session_secret_env or secrets.token_urlsafe(48)
_https = _https_only()
_allowed_hosts_raw = (os.getenv("CLOUDLOG_ALLOWED_HOSTS", "*") or "*").strip()
_allowed_hosts = _parse_allowed_hosts(_allowed_hosts_raw)
_trusted_proxies_raw = (os.getenv("CLOUDLOG_TRUSTED_PROXIES", "*") or "*").strip()
_trusted_proxies: list[str] | str = _parse_trusted_proxies(_trusted_proxies_raw)

app.add_middleware(ProxyHeadersMiddleware, trusted_hosts=_trusted_proxies)
app.add_middleware(
    SessionMiddleware,
    secret_key=_session_secret,
    session_cookie="cloudlog_session",
    https_only=_https,
    same_site="lax",
)
app.add_middleware(TrustedHostMiddleware, allowed_hosts=_allowed_hosts)


@app.on_event("startup")
def _startup() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if not _session_secret_env:
        log.warning("CLOUDLOG_SECRET_KEY is not set. Session secret will rotate on restart.")
    if "*" in _allowed_hosts_raw:
        log.warning("CLOUDLOG_ALLOWED_HOSTS cannot include '*'. Ignored wildcard and using explicit hosts.")
    if not _https:
        log.warning("CLOUDLOG_HTTPS_ONLY is off. Enable it in production.")


@app.on_event("shutdown")
def _shutdown() -> None:
    DB.close()


def _security_headers(response) -> None:
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "same-origin"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; style-src 'self'; script-src 'self'; img-src 'self' data:; "
        "form-action 'self'; base-uri 'self'; frame-ancestors 'none'"
    )
    if _https:
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"


@app.middleware("http")
async def _auth_middleware(request: Request, call_next):  # type: ignore
    path = request.url.path
    public = {"/login", "/register", "/health"}
    is_api = path.startswith("/api/")
    if path.startswith("/static") or path in public:
        resp = await call_next(request)
        _security_headers(resp)
        return resp

    uid = get_user_id(request.session)
    if uid is None:
        if is_api:
            resp = JSONResponse(status_code=401, content={"ok": False, "error": "authentication required"})
        else:
            resp = RedirectResponse(url=f"/login?next={quote(path)}", status_code=303)
        _security_headers(resp)
        return resp

    user = DB.get_user(uid)
    if user is None or not user.active:
        logout_session(request.session)
        if is_api:
            resp = JSONResponse(status_code=401, content={"ok": False, "error": "invalid session"})
        else:
            resp = RedirectResponse(url="/login", status_code=303)
        _security_headers(resp)
        return resp

    resp = await call_next(request)
    _security_headers(resp)
    return resp


def _require_user(request: Request):
    uid = get_user_id(request.session)
    if uid is None:
        raise HTTPException(status_code=401)
    user = DB.get_user(uid)
    if user is None:
        raise HTTPException(status_code=401)
    return user


def _require_role(request: Request, minimum_role: str):
    user = _require_user(request)
    if not _role_at_least(user.role, minimum_role):
        raise HTTPException(status_code=403)
    return user


def _validate_csrf_or_redirect(request: Request, csrf_token: str | None, redirect_to: str) -> RedirectResponse | None:
    if validate_csrf(request.session, csrf_token):
        return None
    return RedirectResponse(url=redirect_to, status_code=303)


def _render(request: Request, name: str, context: dict[str, Any], *, status_code: int = 200) -> HTMLResponse:
    csrf_token = ensure_csrf_token(request.session)
    uid = get_user_id(request.session)
    user = DB.get_user(uid) if uid else None
    merged = {
        "request": request,
        "csrf_token": csrf_token,
        "auth_user": get_username(request.session),
        "current_user": user,
        "allow_registration": _allow_registration(),
        "ROLE_ADMIN": ROLE_ADMIN,
        "ROLE_MANAGER": ROLE_MANAGER,
        "ROLE_MEMBER": ROLE_MEMBER,
        "fmt_hours": _fmt_hours,
        "fmt_ts_jst": _fmt_ts_jst,
        "fmt_seconds_as_hours": _fmt_seconds_as_hours,
        "to_datetime_local": _to_datetime_local,
        **context,
    }
    return templates.TemplateResponse(name, merged, status_code=status_code)


@app.get("/health", response_class=PlainTextResponse)
def health() -> str:
    return "ok"


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request, next: str = "/"):  # noqa: A002
    if get_user_id(request.session) is not None:
        return RedirectResponse(url="/", status_code=303)
    return _render(request, "login.html", {"title": "ログイン", "next": _safe_next(next), "error": ""})


@app.post("/login")
async def login(request: Request):
    form = await request.form()
    username = str(form.get("username", "")).strip()
    password = str(form.get("password", ""))
    next_path = _safe_next(str(form.get("next", "/")))

    row = DB.get_user_auth(username)
    if row is None or not row.active or not verify_password(password, row.password_hash):
        return _render(
            request,
            "login.html",
            {"title": "ログイン", "next": next_path, "error": "ユーザー名またはパスワードが違います"},
            status_code=401,
        )

    login_session(request.session, user_id=row.id, username=row.username)
    return RedirectResponse(url=next_path, status_code=303)


@app.post("/logout")
async def logout(request: Request):
    form = await request.form()
    csrf_token = str(form.get("csrf_token", "") or "")
    redirect = _validate_csrf_or_redirect(request, csrf_token, "/")
    if redirect:
        return redirect
    logout_session(request.session)
    return RedirectResponse(url="/login", status_code=303)


@app.get("/register", response_class=HTMLResponse)
def register_page(request: Request):
    if not _allow_registration():
        return RedirectResponse(url="/login", status_code=303)
    return _render(request, "register.html", {"title": "新規登録", "error": ""})


@app.post("/register")
async def register(request: Request):
    if not _allow_registration():
        return RedirectResponse(url="/login", status_code=303)

    form = await request.form()
    username = str(form.get("username", "")).strip()
    password = str(form.get("password", ""))

    if len(username) < 3:
        return _render(request, "register.html", {"title": "新規登録", "error": "ユーザー名は3文字以上にしてください"}, status_code=400)
    if len(password) < 8:
        return _render(request, "register.html", {"title": "新規登録", "error": "パスワードは8文字以上にしてください"}, status_code=400)

    if DB.get_user_by_name(username) is not None:
        return _render(request, "register.html", {"title": "新規登録", "error": "同名ユーザーが既に存在します"}, status_code=400)

    user_id = DB.create_user(username=username, password_hash=hash_password(password), role=ROLE_MEMBER)
    login_session(request.session, user_id=user_id, username=username)
    return RedirectResponse(url="/", status_code=303)


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request, month: str | None = None):
    user = _require_user(request)
    base = date.today()
    if month:
        try:
            base = datetime.strptime(month + "-01", "%Y-%m-%d").date()
        except ValueError:
            base = date.today()
    from_d, to_d = _month_bounds(base)

    scoped_user = None if _role_at_least(user.role, ROLE_MANAGER) else user.id
    totals = DB.dashboard_totals(from_date=from_d.isoformat(), to_date=to_d.isoformat(), user_id=scoped_user)
    project_rows = DB.project_report(from_date=from_d.isoformat(), to_date=to_d.isoformat(), user_id=scoped_user)
    top_projects = sorted(project_rows, key=lambda x: x["actual_hours"], reverse=True)[:6]
    status_rows = DB.submission_status_list(from_date=from_d.isoformat(), to_date=to_d.isoformat()) if _role_at_least(user.role, ROLE_MANAGER) else []
    timer = DB.get_timer(user_id=user.id)
    elapsed_minutes = 0
    if timer:
        elapsed_minutes = int(max(0, int(time.time()) - timer.started_at) / 60)

    return _render(
        request,
        "dashboard.html",
        {
            "title": "ダッシュボード",
            "month": from_d.strftime("%Y-%m"),
            "from_date": from_d.isoformat(),
            "to_date": to_d.isoformat(),
            "totals": totals,
            "top_projects": top_projects,
            "status_rows": status_rows,
            "timer": timer,
            "elapsed_minutes": elapsed_minutes,
        },
    )


@app.get("/attendance/today")
def attendance_today(request: Request):
    user = _require_user(request)
    today = _jst_today_iso()
    row = DB.get_attendance_by_user_date(user_id=user.id, work_date=today)
    status = _attendance_status(row)
    return {
        "ok": True,
        "date": today,
        "status": status,
        "clock_in_at": row.clock_in_at if row else None,
        "clock_out_at": row.clock_out_at if row else None,
        "clock_in_at_jst": _fmt_ts_jst(row.clock_in_at) if row else "-",
        "clock_out_at_jst": _fmt_ts_jst(row.clock_out_at) if row else "-",
    }


@app.get("/attendance", response_class=HTMLResponse)
def attendance_page(
    request: Request,
    preset: str = "this-month",
    from_date: str | None = None,
    to_date: str | None = None,
):
    user = _require_user(request)
    today = _jst_today()

    this_month_first, this_month_last = _month_bounds(today)
    last_month_first, last_month_last = _month_bounds(this_month_first - timedelta(days=1))

    if from_date and to_date:
        range_from = _to_date(from_date, this_month_first)
        range_to = _to_date(to_date, this_month_last)
    elif preset == "last-month":
        range_from = last_month_first
        range_to = last_month_last
    else:
        range_from = this_month_first
        range_to = this_month_last

    today_key = today.isoformat()
    today_log = DB.get_attendance_by_user_date(user_id=user.id, work_date=today_key)
    today_status = _attendance_status(today_log)

    rows = DB.list_attendance(
        user_id=user.id,
        from_date=range_from.isoformat(),
        to_date=range_to.isoformat(),
    )
    history: list[dict[str, Any]] = []
    for row in rows:
        seconds = _worked_seconds(row)
        history.append(
            {
                "row": row,
                "worked_seconds": seconds,
                "worked_hours": _fmt_seconds_as_hours(seconds),
            }
        )

    month_summary = DB.attendance_summary(
        user_id=user.id,
        from_date=this_month_first.isoformat(),
        to_date=this_month_last.isoformat(),
    )

    can_clock_in = today_status == "未出勤"
    can_clock_out = today_status == "出勤済"

    return _render(
        request,
        "attendance.html",
        {
            "title": "打刻",
            "today_status": today_status,
            "today_log": today_log,
            "can_clock_in": can_clock_in,
            "can_clock_out": can_clock_out,
            "history": history,
            "from_date": range_from.isoformat(),
            "to_date": range_to.isoformat(),
            "preset": preset,
            "month_worked_days": month_summary["worked_days"],
            "month_worked_hours": _fmt_seconds_as_hours(month_summary["total_seconds"]),
            "flash_error": str(request.query_params.get("error", "") or ""),
            "flash_message": str(request.query_params.get("msg", "") or ""),
        },
    )


@app.post("/attendance/clock-in")
async def attendance_clock_in(request: Request):
    user = _require_user(request)
    form = await request.form()
    csrf_token = str(form.get("csrf_token", "") or "")
    redirect = _validate_csrf_or_redirect(request, csrf_token, "/attendance")
    if redirect:
        return redirect

    work_date = _jst_today_iso()
    try:
        DB.clock_in(user_id=user.id, work_date=work_date, at_ts=int(_jst_now().timestamp()))
    except ValueError as e:
        code = str(e)
        if code == "already_clocked_in":
            return RedirectResponse(url="/attendance?error=既に出勤打刻済みです", status_code=303)
        if code == "already_clocked_out":
            return RedirectResponse(url="/attendance?error=本日は既に退勤済みです", status_code=303)
        return RedirectResponse(url="/attendance?error=出勤打刻に失敗しました", status_code=303)

    return RedirectResponse(url="/attendance?msg=出勤打刻しました", status_code=303)


@app.post("/attendance/clock-out")
async def attendance_clock_out(request: Request):
    user = _require_user(request)
    form = await request.form()
    csrf_token = str(form.get("csrf_token", "") or "")
    redirect = _validate_csrf_or_redirect(request, csrf_token, "/attendance")
    if redirect:
        return redirect

    work_date = _jst_today_iso()
    try:
        DB.clock_out(user_id=user.id, work_date=work_date, at_ts=int(_jst_now().timestamp()))
    except ValueError as e:
        code = str(e)
        if code == "clock_in_required":
            return RedirectResponse(url="/attendance?error=出勤前に退勤打刻はできません", status_code=303)
        if code == "already_clocked_out":
            return RedirectResponse(url="/attendance?error=既に退勤打刻済みです", status_code=303)
        return RedirectResponse(url="/attendance?error=退勤打刻に失敗しました", status_code=303)

    return RedirectResponse(url="/attendance?msg=退勤打刻しました", status_code=303)


@app.get("/admin/attendance", response_class=HTMLResponse)
def admin_attendance_page(
    request: Request,
    from_date: str | None = None,
    to_date: str | None = None,
    user_id: int | None = None,
):
    _require_role(request, ROLE_ADMIN)
    today = _jst_today()
    month_first, month_last = _month_bounds(today)

    range_from = _to_date(from_date, month_first).isoformat()
    range_to = _to_date(to_date, month_last).isoformat()
    selected_user_id = int(user_id) if user_id else None

    rows = DB.list_attendance(
        user_id=selected_user_id,
        from_date=range_from,
        to_date=range_to,
    )
    history: list[dict[str, Any]] = []
    for row in rows:
        seconds = _worked_seconds(row)
        history.append(
            {
                "row": row,
                "worked_seconds": seconds,
                "worked_hours": _fmt_seconds_as_hours(seconds),
            }
        )

    return _render(
        request,
        "admin_attendance.html",
        {
            "title": "打刻管理",
            "users": DB.list_users(active_only=True),
            "selected_user_id": selected_user_id,
            "from_date": range_from,
            "to_date": range_to,
            "history": history,
            "flash_error": str(request.query_params.get("error", "") or ""),
            "flash_message": str(request.query_params.get("msg", "") or ""),
        },
    )


@app.post("/admin/attendance/{attendance_id}")
async def admin_attendance_update(request: Request, attendance_id: int):
    admin = _require_role(request, ROLE_ADMIN)
    form = await request.form()
    csrf_token = str(form.get("csrf_token", "") or "")
    redirect = _validate_csrf_or_redirect(request, csrf_token, "/admin/attendance")
    if redirect:
        return redirect

    from_date = str(form.get("from_date", "") or "").strip()
    to_date = str(form.get("to_date", "") or "").strip()
    user_id = str(form.get("user_id", "") or "").strip()
    query_suffix = f"from_date={quote(from_date)}&to_date={quote(to_date)}"
    if user_id:
        query_suffix += f"&user_id={quote(user_id)}"

    reason = str(form.get("reason", "") or "").strip()
    note = str(form.get("note", "") or "").strip()
    try:
        clock_in_at = _parse_datetime_local(str(form.get("clock_in_at", "") or ""))
        clock_out_at = _parse_datetime_local(str(form.get("clock_out_at", "") or ""))
    except ValueError:
        return RedirectResponse(url=f"/admin/attendance?{query_suffix}&error={quote('日時フォーマットが不正です')}", status_code=303)

    try:
        DB.admin_update_attendance(
            attendance_id=attendance_id,
            actor_user_id=admin.id,
            clock_in_at=clock_in_at,
            clock_out_at=clock_out_at,
            note=note,
            reason=reason,
        )
    except ValueError as e:
        code = str(e)
        if code == "reason_required":
            msg = "修正理由は必須です"
        elif code == "clock_in_required":
            msg = "出勤時刻なしで退勤時刻のみは設定できません"
        elif code == "clock_out_must_be_after_clock_in":
            msg = "退勤時刻は出勤時刻より後に設定してください"
        elif code == "attendance_not_found":
            msg = "対象の打刻が見つかりません"
        else:
            msg = "打刻修正に失敗しました"
        return RedirectResponse(url=f"/admin/attendance?{query_suffix}&error={quote(msg)}", status_code=303)

    return RedirectResponse(url=f"/admin/attendance?{query_suffix}&msg=打刻を修正しました", status_code=303)


@app.get("/entries", response_class=HTMLResponse)
def entries_page(request: Request, week_start: str | None = None, user_id: int | None = None):
    user = _require_user(request)

    base = _to_date(week_start, date.today())
    start_d, end_d = _week_bounds(base)

    target_user_id = user.id
    users = []
    if _role_at_least(user.role, ROLE_MANAGER):
        users = DB.list_users(active_only=True)
        if user_id:
            target_user_id = int(user_id)

    entries = DB.list_entries(
        user_id=target_user_id,
        from_date=start_d.isoformat(),
        to_date=end_d.isoformat(),
    )
    projects = DB.list_projects(include_archived=False)
    tasks = DB.list_tasks(active_only=True)

    task_map: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for task in tasks:
        task_map[task.project_id].append({"id": task.id, "name": task.name})

    by_date: dict[str, list[Any]] = defaultdict(list)
    total_minutes = 0
    for entry in entries:
        by_date[entry.work_date].append(entry)
        total_minutes += entry.minutes

    days: list[date] = [start_d + timedelta(days=i) for i in range(7)]

    timer = DB.get_timer(user_id=target_user_id)
    timer_minutes = 0
    if timer and target_user_id == user.id:
        timer_minutes = int(max(0, int(time.time()) - timer.started_at) / 60)

    return _render(
        request,
        "entries.html",
        {
            "title": "工数入力",
            "week_start": start_d.isoformat(),
            "week_end": end_d.isoformat(),
            "entries": entries,
            "projects": projects,
            "tasks": tasks,
            "task_map_json": {k: v for k, v in task_map.items()},
            "days": days,
            "by_date": by_date,
            "users": users,
            "target_user_id": target_user_id,
            "total_hours": total_minutes / 60.0,
            "timer": timer,
            "timer_minutes": timer_minutes,
        },
    )


@app.post("/entries")
async def create_entry(request: Request):
    user = _require_user(request)
    form = await request.form()

    csrf_token = str(form.get("csrf_token", "") or "")
    redirect = _validate_csrf_or_redirect(request, csrf_token, "/entries")
    if redirect:
        return redirect

    target_user_id = user.id
    raw_user_id = str(form.get("user_id", "") or "").strip()
    if raw_user_id and _role_at_least(user.role, ROLE_MANAGER):
        target_user_id = int(raw_user_id)

    work_date = str(form.get("work_date", date.today().isoformat())).strip()
    project_id = int(str(form.get("project_id", "0") or "0"))
    task_id_raw = str(form.get("task_id", "") or "").strip()
    task_id = int(task_id_raw) if task_id_raw else None
    note = str(form.get("note", "") or "").strip()
    minutes = _minutes_from_hours(str(form.get("hours", "0") or "0"))

    if project_id <= 0 or minutes <= 0:
        return RedirectResponse(url=f"/entries?week_start={quote(work_date)}", status_code=303)

    DB.create_entry(
        user_id=target_user_id,
        project_id=project_id,
        task_id=task_id,
        work_date=work_date,
        minutes=minutes,
        note=note,
        status=STATUS_DRAFT,
    )

    week_start = str(form.get("week_start", work_date) or work_date)
    qs = f"week_start={quote(week_start)}"
    if _role_at_least(user.role, ROLE_MANAGER):
        qs += f"&user_id={target_user_id}"
    return RedirectResponse(url=f"/entries?{qs}", status_code=303)


@app.post("/entries/{entry_id}/update")
async def update_entry(request: Request, entry_id: int):
    user = _require_user(request)
    form = await request.form()

    csrf_token = str(form.get("csrf_token", "") or "")
    redirect = _validate_csrf_or_redirect(request, csrf_token, "/entries")
    if redirect:
        return redirect

    entry = DB.get_entry(entry_id)
    if entry is None:
        return RedirectResponse(url="/entries", status_code=303)

    editable = entry.status in {STATUS_DRAFT, STATUS_REJECTED}
    if not editable:
        return RedirectResponse(url="/entries", status_code=303)

    if not _role_at_least(user.role, ROLE_MANAGER) and entry.user_id != user.id:
        return RedirectResponse(url="/entries", status_code=303)

    work_date = str(form.get("work_date", entry.work_date)).strip()
    project_id = int(str(form.get("project_id", entry.project_id) or entry.project_id))
    task_id_raw = str(form.get("task_id", "") or "").strip()
    task_id = int(task_id_raw) if task_id_raw else None
    note = str(form.get("note", "") or "").strip()
    minutes = _minutes_from_hours(str(form.get("hours", str(entry.minutes / 60.0)) or "0"))

    if project_id > 0 and minutes > 0:
        DB.update_entry(
            entry_id=entry.id,
            project_id=project_id,
            task_id=task_id,
            work_date=work_date,
            minutes=minutes,
            note=note,
        )

    return RedirectResponse(url=f"/entries?week_start={quote(work_date)}&user_id={entry.user_id}", status_code=303)


@app.post("/entries/{entry_id}/delete")
async def delete_entry(request: Request, entry_id: int):
    user = _require_user(request)
    form = await request.form()

    csrf_token = str(form.get("csrf_token", "") or "")
    redirect = _validate_csrf_or_redirect(request, csrf_token, "/entries")
    if redirect:
        return redirect

    entry = DB.get_entry(entry_id)
    if entry is None:
        return RedirectResponse(url="/entries", status_code=303)

    if entry.status == STATUS_APPROVED:
        return RedirectResponse(url="/entries", status_code=303)
    if not _role_at_least(user.role, ROLE_MANAGER) and entry.user_id != user.id:
        return RedirectResponse(url="/entries", status_code=303)

    DB.delete_entry(entry_id)
    return RedirectResponse(url=f"/entries?week_start={quote(entry.work_date)}&user_id={entry.user_id}", status_code=303)


@app.post("/entries/submit")
async def submit_entries(request: Request):
    user = _require_user(request)
    form = await request.form()

    csrf_token = str(form.get("csrf_token", "") or "")
    redirect = _validate_csrf_or_redirect(request, csrf_token, "/entries")
    if redirect:
        return redirect

    from_date = str(form.get("from_date", date.today().isoformat()) or date.today().isoformat())
    to_date = str(form.get("to_date", date.today().isoformat()) or date.today().isoformat())
    target_user_id = user.id
    raw_user_id = str(form.get("user_id", "") or "").strip()
    if raw_user_id and _role_at_least(user.role, ROLE_MANAGER):
        target_user_id = int(raw_user_id)

    DB.submit_entries(user_id=target_user_id, from_date=from_date, to_date=to_date)
    return RedirectResponse(url=f"/entries?week_start={quote(from_date)}&user_id={target_user_id}", status_code=303)


@app.post("/entries/copy")
async def copy_entries(request: Request):
    user = _require_user(request)
    form = await request.form()

    csrf_token = str(form.get("csrf_token", "") or "")
    redirect = _validate_csrf_or_redirect(request, csrf_token, "/entries")
    if redirect:
        return redirect

    source_date = str(form.get("source_date", "") or "").strip()
    target_date = str(form.get("target_date", "") or "").strip()
    if not source_date or not target_date:
        return RedirectResponse(url="/entries", status_code=303)

    target_user_id = user.id
    raw_user_id = str(form.get("user_id", "") or "").strip()
    if raw_user_id and _role_at_least(user.role, ROLE_MANAGER):
        target_user_id = int(raw_user_id)

    DB.copy_entries(user_id=target_user_id, source_date=source_date, target_date=target_date)
    return RedirectResponse(url=f"/entries?week_start={quote(target_date)}&user_id={target_user_id}", status_code=303)


@app.post("/timer/start")
async def start_timer(request: Request):
    user = _require_user(request)
    form = await request.form()
    csrf_token = str(form.get("csrf_token", "") or "")
    redirect = _validate_csrf_or_redirect(request, csrf_token, "/entries")
    if redirect:
        return redirect

    project_id = int(str(form.get("project_id", "0") or "0"))
    if project_id <= 0:
        return RedirectResponse(url="/entries", status_code=303)

    task_id_raw = str(form.get("task_id", "") or "").strip()
    task_id = int(task_id_raw) if task_id_raw else None
    note = str(form.get("note", "") or "")
    DB.start_timer(user_id=user.id, project_id=project_id, task_id=task_id, note=note)
    return RedirectResponse(url="/entries", status_code=303)


@app.post("/timer/stop")
async def stop_timer(request: Request):
    user = _require_user(request)
    form = await request.form()
    csrf_token = str(form.get("csrf_token", "") or "")
    redirect = _validate_csrf_or_redirect(request, csrf_token, "/entries")
    if redirect:
        return redirect

    timer = DB.stop_timer(user_id=user.id)
    if timer is None:
        return RedirectResponse(url="/entries", status_code=303)

    elapsed_seconds = max(60, int(time.time()) - timer.started_at)
    minutes = max(1, int(round(elapsed_seconds / 60.0)))
    work_date = date.today().isoformat()
    DB.create_entry(
        user_id=user.id,
        project_id=timer.project_id,
        task_id=timer.task_id,
        work_date=work_date,
        minutes=minutes,
        note=timer.note,
        status=STATUS_DRAFT,
    )
    return RedirectResponse(url=f"/entries?week_start={quote(work_date)}", status_code=303)


@app.get("/approval", response_class=HTMLResponse)
def approval_page(request: Request, from_date: str | None = None, to_date: str | None = None, user_id: int | None = None):
    manager = _require_role(request, ROLE_MANAGER)
    today = date.today()
    default_from = (today - timedelta(days=14)).isoformat()
    default_to = today.isoformat()

    entries = DB.list_entries(
        user_id=user_id,
        from_date=from_date or default_from,
        to_date=to_date or default_to,
        status=STATUS_SUBMITTED,
    )

    return _render(
        request,
        "approval.html",
        {
            "title": "承認",
            "entries": entries,
            "from_date": from_date or default_from,
            "to_date": to_date or default_to,
            "users": DB.list_users(active_only=True),
            "selected_user_id": user_id,
            "is_manager": _role_at_least(manager.role, ROLE_MANAGER),
        },
    )


@app.post("/entries/{entry_id}/approve")
async def approve_entry(request: Request, entry_id: int):
    manager = _require_role(request, ROLE_MANAGER)
    form = await request.form()

    csrf_token = str(form.get("csrf_token", "") or "")
    redirect = _validate_csrf_or_redirect(request, csrf_token, "/approval")
    if redirect:
        return redirect

    entry = DB.get_entry(entry_id)
    if entry and entry.status == STATUS_SUBMITTED:
        DB.approve_entry(entry_id=entry_id, approver_id=manager.id)
        _notify_webhook(
            DB,
            event="entry.approved",
            payload={
                "entry_id": entry_id,
                "user": entry.username,
                "date": entry.work_date,
                "hours": round(entry.minutes / 60.0, 2),
                "project": entry.project_code,
            },
        )

    return RedirectResponse(url="/approval", status_code=303)


@app.post("/entries/{entry_id}/reject")
async def reject_entry(request: Request, entry_id: int):
    manager = _require_role(request, ROLE_MANAGER)
    form = await request.form()

    csrf_token = str(form.get("csrf_token", "") or "")
    redirect = _validate_csrf_or_redirect(request, csrf_token, "/approval")
    if redirect:
        return redirect

    reason = str(form.get("reason", "差し戻し") or "差し戻し")
    entry = DB.get_entry(entry_id)
    if entry and entry.status == STATUS_SUBMITTED:
        DB.reject_entry(entry_id=entry_id, approver_id=manager.id, reason=reason)
        _notify_webhook(
            DB,
            event="entry.rejected",
            payload={
                "entry_id": entry_id,
                "user": entry.username,
                "date": entry.work_date,
                "reason": reason,
            },
        )

    return RedirectResponse(url="/approval", status_code=303)


@app.get("/projects", response_class=HTMLResponse)
def projects_page(request: Request):
    _require_role(request, ROLE_MANAGER)
    return _render(
        request,
        "projects.html",
        {
            "title": "案件管理",
            "clients": DB.list_clients(),
            "projects": DB.list_projects(include_archived=True),
            "tasks": DB.list_tasks(active_only=False),
        },
    )


@app.post("/clients/new")
async def create_client(request: Request):
    _require_role(request, ROLE_MANAGER)
    form = await request.form()
    csrf_token = str(form.get("csrf_token", "") or "")
    redirect = _validate_csrf_or_redirect(request, csrf_token, "/projects")
    if redirect:
        return redirect

    name = str(form.get("name", "") or "").strip()
    if name:
        try:
            DB.create_client(name)
        except Exception:
            log.exception("Failed to create client")
    return RedirectResponse(url="/projects", status_code=303)


@app.post("/projects/new")
async def create_project(request: Request):
    _require_role(request, ROLE_MANAGER)
    form = await request.form()
    csrf_token = str(form.get("csrf_token", "") or "")
    redirect = _validate_csrf_or_redirect(request, csrf_token, "/projects")
    if redirect:
        return redirect

    name = str(form.get("name", "") or "").strip()
    code = str(form.get("code", "") or "").strip()
    description = str(form.get("description", "") or "").strip()
    client_id_raw = str(form.get("client_id", "") or "").strip()
    start_date = str(form.get("start_date", "") or "").strip() or None
    end_date = str(form.get("end_date", "") or "").strip() or None
    budget_hours = _parse_hours(str(form.get("budget_hours", "0") or "0"))
    budget_cost = _parse_hours(str(form.get("budget_cost", "0") or "0"))
    bill_rate = _parse_hours(str(form.get("bill_rate", "0") or "0"))

    if name and code:
        try:
            DB.create_project(
                client_id=int(client_id_raw) if client_id_raw else None,
                name=name,
                code=code,
                description=description,
                budget_hours=budget_hours,
                budget_cost=budget_cost,
                bill_rate=bill_rate,
                start_date=start_date,
                end_date=end_date,
            )
        except Exception:
            log.exception("Failed to create project")

    return RedirectResponse(url="/projects", status_code=303)


@app.post("/projects/{project_id}/archive")
async def archive_project(request: Request, project_id: int):
    _require_role(request, ROLE_MANAGER)
    form = await request.form()
    csrf_token = str(form.get("csrf_token", "") or "")
    redirect = _validate_csrf_or_redirect(request, csrf_token, "/projects")
    if redirect:
        return redirect

    DB.update_project_status(project_id, "archived")
    return RedirectResponse(url="/projects", status_code=303)


@app.post("/tasks/new")
async def create_task(request: Request):
    _require_role(request, ROLE_MANAGER)
    form = await request.form()
    csrf_token = str(form.get("csrf_token", "") or "")
    redirect = _validate_csrf_or_redirect(request, csrf_token, "/projects")
    if redirect:
        return redirect

    project_id = int(str(form.get("project_id", "0") or "0"))
    name = str(form.get("name", "") or "").strip()
    if project_id > 0 and name:
        try:
            DB.create_task(project_id=project_id, name=name)
        except Exception:
            log.exception("Failed to create task")

    return RedirectResponse(url="/projects", status_code=303)


@app.get("/users", response_class=HTMLResponse)
def users_page(request: Request):
    _require_role(request, ROLE_ADMIN)
    return _render(
        request,
        "users.html",
        {
            "title": "ユーザー管理",
            "users": DB.list_users(active_only=False),
        },
    )


@app.post("/users/new")
async def create_user(request: Request):
    _require_role(request, ROLE_ADMIN)
    form = await request.form()
    csrf_token = str(form.get("csrf_token", "") or "")
    redirect = _validate_csrf_or_redirect(request, csrf_token, "/users")
    if redirect:
        return redirect

    username = str(form.get("username", "") or "").strip()
    password = str(form.get("password", "") or "")
    role = str(form.get("role", ROLE_MEMBER) or ROLE_MEMBER)
    hourly_cost = _parse_hours(str(form.get("hourly_cost", "0") or "0"))

    if len(username) >= 3 and len(password) >= 8 and role in ROLE_ORDER:
        if DB.get_user_by_name(username) is None:
            DB.create_user(username=username, password_hash=hash_password(password), role=role, hourly_cost=hourly_cost)

    return RedirectResponse(url="/users", status_code=303)


@app.post("/users/{user_id}/update")
async def update_user(request: Request, user_id: int):
    _require_role(request, ROLE_ADMIN)
    form = await request.form()
    csrf_token = str(form.get("csrf_token", "") or "")
    redirect = _validate_csrf_or_redirect(request, csrf_token, "/users")
    if redirect:
        return redirect

    role = str(form.get("role", ROLE_MEMBER) or ROLE_MEMBER)
    hourly_cost = _parse_hours(str(form.get("hourly_cost", "0") or "0"))
    if role in ROLE_ORDER:
        DB.update_user_role_and_cost(user_id, role=role, hourly_cost=hourly_cost)
    return RedirectResponse(url="/users", status_code=303)


@app.get("/reports", response_class=HTMLResponse)
def reports_page(request: Request, from_date: str | None = None, to_date: str | None = None):
    user = _require_user(request)
    today = date.today()
    month_first, month_last = _month_bounds(today)

    fd = from_date or month_first.isoformat()
    td = to_date or month_last.isoformat()

    scoped_user = None if _role_at_least(user.role, ROLE_MANAGER) else user.id
    project_rows = DB.project_report(from_date=fd, to_date=td, user_id=scoped_user)
    user_rows = DB.user_report(from_date=fd, to_date=td)

    return _render(
        request,
        "reports.html",
        {
            "title": "レポート",
            "from_date": fd,
            "to_date": td,
            "project_rows": project_rows,
            "user_rows": user_rows,
            "is_manager": _role_at_least(user.role, ROLE_MANAGER),
        },
    )


@app.get("/status", response_class=HTMLResponse)
def status_page(request: Request, from_date: str | None = None, to_date: str | None = None):
    _require_role(request, ROLE_MANAGER)
    today = date.today()
    month_first, month_last = _month_bounds(today)
    fd = from_date or month_first.isoformat()
    td = to_date or month_last.isoformat()

    rows = DB.submission_status_list(from_date=fd, to_date=td)
    return _render(
        request,
        "status.html",
        {
            "title": "入力ステータス",
            "from_date": fd,
            "to_date": td,
            "rows": rows,
        },
    )


@app.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request):
    _require_role(request, ROLE_MANAGER)
    webhook_url = DB.get_setting("webhook_url", "")
    return _render(
        request,
        "settings.html",
        {
            "title": "設定",
            "webhook_url": webhook_url,
        },
    )


@app.post("/settings/webhook")
async def settings_webhook(request: Request):
    _require_role(request, ROLE_MANAGER)
    form = await request.form()
    csrf_token = str(form.get("csrf_token", "") or "")
    redirect = _validate_csrf_or_redirect(request, csrf_token, "/settings")
    if redirect:
        return redirect

    webhook_url = str(form.get("webhook_url", "") or "").strip()
    DB.set_setting("webhook_url", webhook_url)
    return RedirectResponse(url="/settings", status_code=303)


@app.get("/export/entries.csv")
def export_entries(request: Request, from_date: str | None = None, to_date: str | None = None, user_id: int | None = None):
    user = _require_user(request)
    today = date.today()
    month_first, month_last = _month_bounds(today)

    fd = from_date or month_first.isoformat()
    td = to_date or month_last.isoformat()

    scoped_user_id = user.id
    if _role_at_least(user.role, ROLE_MANAGER):
        scoped_user_id = int(user_id) if user_id else None

    rows = DB.export_entries(from_date=fd, to_date=td, user_id=scoped_user_id)

    sio = io.StringIO()
    writer = csv.DictWriter(
        sio,
        fieldnames=[
            "id",
            "username",
            "date",
            "project_code",
            "project",
            "task",
            "hours",
            "status",
            "note",
            "approver",
            "reject_reason",
        ],
    )
    writer.writeheader()
    writer.writerows(rows)

    payload = sio.getvalue().encode("utf-8-sig")
    filename = f"cloudlog_entries_{fd}_{td}.csv"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(io.BytesIO(payload), media_type="text/csv; charset=utf-8", headers=headers)


@app.post("/import/entries.csv")
async def import_entries(request: Request, csrf_token: str = Form(...), file: UploadFile = File(...)):
    user = _require_user(request)
    redirect = _validate_csrf_or_redirect(request, csrf_token, "/entries")
    if redirect:
        return redirect

    content = await file.read()
    text = content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))

    task_cache: dict[tuple[int, str], int] = {}
    task_rows = DB.list_tasks(active_only=False)
    for task in task_rows:
        task_cache[(task.project_id, task.name)] = task.id

    imported = 0
    for row in reader:
        work_date = str(row.get("date", "") or "").strip()
        project_key = str(row.get("project_code", "") or row.get("project", "")).strip()
        if not work_date or not project_key:
            continue

        project = DB.get_project_by_code_or_name(project_key)
        if project is None:
            continue

        target_user_id = user.id
        if _role_at_least(user.role, ROLE_MANAGER):
            uname = str(row.get("username", "") or "").strip()
            if uname:
                u = DB.get_user_by_name(uname)
                if u:
                    target_user_id = u.id

        task_name = str(row.get("task", "") or "").strip()
        task_id = None
        if task_name:
            key = (project.id, task_name)
            task_id = task_cache.get(key)
            if task_id is None:
                task_id = DB.create_task(project_id=project.id, name=task_name)
                task_cache[key] = task_id

        minutes = _minutes_from_hours(str(row.get("hours", "0") or "0"))
        if minutes <= 0:
            continue

        note = str(row.get("note", "") or "")
        status = str(row.get("status", STATUS_DRAFT) or STATUS_DRAFT).lower()
        if status not in {STATUS_DRAFT, STATUS_SUBMITTED, STATUS_APPROVED, STATUS_REJECTED}:
            status = STATUS_DRAFT
        if not _role_at_least(user.role, ROLE_MANAGER):
            status = STATUS_DRAFT

        DB.create_entry(
            user_id=target_user_id,
            project_id=project.id,
            task_id=task_id,
            work_date=work_date,
            minutes=minutes,
            note=note,
            status=status,
        )
        imported += 1

    return RedirectResponse(url=f"/entries?week_start={date.today().isoformat()}&imported={imported}", status_code=303)


@app.get("/calendar.ics")
def calendar_feed(request: Request, from_date: str | None = None, to_date: str | None = None):
    user = _require_user(request)
    today = date.today()
    month_first, month_last = _month_bounds(today)
    fd = from_date or month_first.isoformat()
    td = to_date or month_last.isoformat()

    rows = DB.entries_for_calendar(user_id=user.id, from_date=fd, to_date=td)

    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Cloudlog Clone//JP",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
    ]
    for item in rows:
        work_date = date.fromisoformat(item["work_date"])
        dtstart = work_date.strftime("%Y%m%d")
        dtend = (work_date + timedelta(days=1)).strftime("%Y%m%d")
        uid = f"{user.id}-{item['project_code']}-{dtstart}@cloudlog.local"
        summary = f"{item['project_code']} {item['hours']:.2f}h"
        description = item["project_name"].replace("\n", " ")
        lines.extend(
            [
                "BEGIN:VEVENT",
                f"UID:{uid}",
                f"DTSTAMP:{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}",
                f"DTSTART;VALUE=DATE:{dtstart}",
                f"DTEND;VALUE=DATE:{dtend}",
                f"SUMMARY:{summary}",
                f"DESCRIPTION:{description}",
                "END:VEVENT",
            ]
        )
    lines.append("END:VCALENDAR")

    payload = "\r\n".join(lines) + "\r\n"
    return PlainTextResponse(payload, media_type="text/calendar; charset=utf-8")


@app.get("/api/v1/projects")
def api_projects(request: Request):
    _require_user(request)
    rows = DB.list_projects(include_archived=True)
    return {"ok": True, "projects": [r.__dict__ for r in rows]}


@app.get("/api/v1/entries")
def api_entries(
    request: Request,
    from_date: str | None = None,
    to_date: str | None = None,
    user_id: int | None = None,
    status: str | None = None,
):
    user = _require_user(request)

    target_user_id = user.id
    if _role_at_least(user.role, ROLE_MANAGER):
        target_user_id = int(user_id) if user_id else None

    rows = DB.list_entries(user_id=target_user_id, from_date=from_date, to_date=to_date, status=status)
    return {"ok": True, "entries": [r.__dict__ for r in rows]}


@app.post("/api/v1/entries")
async def api_create_entry(request: Request):
    user = _require_user(request)
    payload = await request.json()

    work_date = str(payload.get("date", "")).strip()
    project_id = int(payload.get("project_id", 0) or 0)
    task_id = payload.get("task_id")
    note = str(payload.get("note", "") or "")
    hours = float(payload.get("hours", 0) or 0)
    minutes = int(round(hours * 60))
    status = str(payload.get("status", STATUS_DRAFT) or STATUS_DRAFT).lower()

    if status not in {STATUS_DRAFT, STATUS_SUBMITTED, STATUS_APPROVED, STATUS_REJECTED}:
        status = STATUS_DRAFT

    target_user_id = user.id
    if _role_at_least(user.role, ROLE_MANAGER) and payload.get("user_id"):
        target_user_id = int(payload["user_id"])

    if not work_date:
        return _api_error("date is required")
    if project_id <= 0:
        return _api_error("project_id must be positive")
    if minutes <= 0:
        return _api_error("hours must be positive")
    if not _role_at_least(user.role, ROLE_MANAGER):
        status = STATUS_DRAFT

    entry_id = DB.create_entry(
        user_id=target_user_id,
        project_id=project_id,
        task_id=int(task_id) if task_id else None,
        work_date=work_date,
        minutes=minutes,
        note=note,
        status=status,
    )
    return {"ok": True, "entry_id": entry_id}


@app.get("/api/v1/reports/summary")
def api_report_summary(request: Request, from_date: str, to_date: str):
    user = _require_user(request)
    scoped_user = None if _role_at_least(user.role, ROLE_MANAGER) else user.id
    return {
        "ok": True,
        "projects": DB.project_report(from_date=from_date, to_date=to_date, user_id=scoped_user),
        "users": DB.user_report(from_date=from_date, to_date=to_date),
    }


@app.get("/api/v1/status")
def api_status(request: Request, from_date: str, to_date: str):
    _require_role(request, ROLE_MANAGER)
    return {
        "ok": True,
        "status": DB.submission_status_list(from_date=from_date, to_date=to_date),
    }
