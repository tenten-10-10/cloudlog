from __future__ import annotations

import logging
import os
import secrets
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware

from sitewatcher.job import run_job_once
from sitewatcher.monitor import check_target
from sitewatcher.storage import StateStore
from sitewatcher.web.auth import (
    ensure_csrf_token,
    is_auth_disabled,
    is_authenticated,
    login_session,
    logout_session,
    validate_csrf,
    verify_login,
)
from sitewatcher.web.db import AppDB, TargetRow, target_state_key
from sitewatcher.web.utils import format_ts, headers_to_text, parse_headers_text


log = logging.getLogger("sitewatcher.web")


def _resolve_data_dir() -> Path:
    raw = os.getenv("SITEWATCHER_DATA_DIR", ".sitewatcher")
    p = Path(raw)
    if not p.is_absolute():
        p = (Path.cwd() / p).resolve()
    return p


DATA_DIR = _resolve_data_dir()
DB_PATH = DATA_DIR / "app.sqlite3"

TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

app = FastAPI(title="SiteWatcher")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

_SESSION_SECRET = (os.getenv("SITEWATCHER_SECRET_KEY", "") or "").strip() or secrets.token_urlsafe(48)
_HTTPS_ONLY = (os.getenv("SITEWATCHER_HTTPS_ONLY", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}

_ALLOWED_HOSTS_RAW = (os.getenv("SITEWATCHER_ALLOWED_HOSTS", "*") or "*").strip()
_ALLOWED_HOSTS = ["*"] if _ALLOWED_HOSTS_RAW == "*" else [h.strip() for h in _ALLOWED_HOSTS_RAW.split(",") if h.strip()]

app.add_middleware(SessionMiddleware, secret_key=_SESSION_SECRET, session_cookie="sitewatcher_session", https_only=_HTTPS_ONLY)
app.add_middleware(TrustedHostMiddleware, allowed_hosts=_ALLOWED_HOSTS)


def _should_start_web_scheduler() -> bool:
    raw = (os.getenv("SITEWATCHER_WEB_SCHEDULER", "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _run_monitor_job(*, reason: str) -> None:
    run_job_once(data_dir=DATA_DIR, reason=reason)


def _scheduler_loop() -> None:
    while True:
        try:
            db = AppDB(DB_PATH)
            interval = max(10, db.get_setting_int("interval_seconds", 300))
            enabled = db.get_setting_bool("scheduler_enabled", True)
            db.close()

            if enabled:
                _run_monitor_job(reason="scheduler")
        except Exception:
            log.exception("Scheduler loop error")
            interval = 60

        time.sleep(interval)


@app.on_event("startup")
def _startup() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    AppDB(DB_PATH).close()
    if _SESSION_SECRET == "":  # pragma: no cover
        log.warning("SITEWATCHER_SECRET_KEY is not set. Sessions will be ephemeral.")
    if _ALLOWED_HOSTS == ["*"]:
        log.warning("SITEWATCHER_ALLOWED_HOSTS is '*' (not recommended for production).")
    if not _HTTPS_ONLY:
        log.warning("SITEWATCHER_HTTPS_ONLY is off. Enable it when serving over HTTPS.")

    if _should_start_web_scheduler():
        t = threading.Thread(target=_scheduler_loop, daemon=True)
        t.start()


def _safe_next(next_path: str | None) -> str:
    if not next_path:
        return "/"
    p = str(next_path).strip()
    if not p.startswith("/"):
        return "/"
    if "://" in p:
        return "/"
    return p


def _render(request: Request, template_name: str, context: dict[str, Any], *, status_code: int = 200) -> HTMLResponse:
    session = request.session
    csrf_token = ensure_csrf_token(session)
    auth_user = session.get("auth_user") if is_authenticated(session) else None
    base = {"request": request, "csrf_token": csrf_token, "auth_user": auth_user, "auth_disabled": is_auth_disabled()}
    merged = {**base, **context}
    return templates.TemplateResponse(template_name, merged, status_code=status_code)


def _require_csrf_or_redirect(request: Request, token: str | None, *, redirect_to: str) -> RedirectResponse | None:
    if is_auth_disabled():
        return None
    if validate_csrf(request.session, token):
        return None
    return RedirectResponse(url=redirect_to, status_code=303)


@app.middleware("http")
async def _auth_middleware(request: Request, call_next):  # type: ignore
    path = request.url.path
    if is_auth_disabled():
        return await call_next(request)

    if path.startswith("/static") or path in {"/login", "/health"}:
        return await call_next(request)

    if not is_authenticated(request.session):
        return RedirectResponse(url=f"/login?next={quote(path)}", status_code=303)

    response = await call_next(request)
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "same-origin"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "style-src 'self' 'unsafe-inline'; "
        "script-src 'self' 'unsafe-inline'; "
        "img-src 'self' data:; "
        "base-uri 'self'; "
        "frame-ancestors 'none'"
    )
    return response


def _load_state_map(targets: list[TargetRow]) -> dict[int, Any]:
    store = StateStore(DATA_DIR / "state.sqlite3")
    try:
        out: dict[int, Any] = {}
        for t in targets:
            out[t.id] = store.get(target_state_key(t.id))
        return out
    finally:
        store.close()


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    db = AppDB(DB_PATH)
    targets = db.list_targets(include_disabled=True)
    notifiers = db.list_notifiers()
    last_run = db.get_last_run()
    interval_seconds = db.get_setting_int("interval_seconds", 300)
    scheduler_enabled = db.get_setting_bool("scheduler_enabled", True)
    notify_on_first = db.get_setting_bool("notify_on_first", False)
    db.close()

    state_map = _load_state_map(targets)

    return _render(
        request,
        "index.html",
        {
            "targets": targets,
            "state_map": state_map,
            "notifiers": notifiers,
            "last_run": last_run,
            "interval_seconds": interval_seconds,
            "scheduler_enabled": scheduler_enabled,
            "notify_on_first": notify_on_first,
            "format_ts": format_ts,
        },
    )


@app.post("/run-now")
async def run_now(request: Request) -> RedirectResponse:
    form = await request.form()
    if (r := _require_csrf_or_redirect(request, str(form.get("csrf_token", "")), redirect_to="/")) is not None:
        return r
    threading.Thread(target=_run_monitor_job, kwargs={"reason": "manual"}, daemon=True).start()
    return RedirectResponse(url="/", status_code=303)


@app.get("/targets/new", response_class=HTMLResponse)
def new_target(request: Request) -> HTMLResponse:
    db = AppDB(DB_PATH)
    notifiers = db.list_notifiers()
    db.close()
    return _render(
        request,
        "target_form.html",
        {"mode": "new", "target": None, "headers_text": "", "notifiers": notifiers},
    )


@app.get("/targets/{target_id}/edit", response_class=HTMLResponse)
def edit_target(request: Request, target_id: int) -> HTMLResponse:
    db = AppDB(DB_PATH)
    target = db.get_target(target_id)
    notifiers = db.list_notifiers()
    db.close()
    if target is None:
        return _render(request, "error.html", {"message": "Target not found"}, status_code=404)
    return _render(
        request,
        "target_form.html",
        {"mode": "edit", "target": target, "headers_text": headers_to_text(target.headers), "notifiers": notifiers},
    )


@app.post("/targets/new")
async def create_target(request: Request) -> RedirectResponse:
    form = await request.form()
    if (r := _require_csrf_or_redirect(request, str(form.get("csrf_token", "")), redirect_to="/targets/new")) is not None:
        return r
    name = str(form.get("name", "")).strip()
    url = str(form.get("url", "")).strip()
    target_type = str(form.get("type", "html")).strip().lower()
    selector_raw = str(form.get("selector", "")).strip()
    selector = selector_raw or None
    extract = str(form.get("extract", "text")).strip().lower()
    render_js = form.get("render_js") in {"on", "1", "true", True}
    enabled = form.get("enabled") in {"on", "1", "true", True}
    try:
        timeout_seconds = int(str(form.get("timeout_seconds", "20")).strip() or "20")
    except ValueError:
        timeout_seconds = 20
    headers_text = str(form.get("headers", "") or "")
    headers = parse_headers_text(headers_text)

    notify_values = form.getlist("notify")
    notify = [str(x) for x in notify_values]

    if not name or not url:
        return RedirectResponse(url="/targets/new", status_code=303)

    db = AppDB(DB_PATH)
    db.create_target(
        name=name,
        type=target_type,
        url=url,
        selector=selector,
        extract=extract,
        render_js=bool(render_js),
        timeout_seconds=timeout_seconds,
        headers=headers,
        notify=notify,
        enabled=bool(enabled),
    )
    db.close()
    return RedirectResponse(url="/", status_code=303)


@app.post("/targets/{target_id}/edit")
async def update_target(request: Request, target_id: int) -> RedirectResponse:
    form = await request.form()
    if (r := _require_csrf_or_redirect(request, str(form.get("csrf_token", "")), redirect_to=f"/targets/{target_id}/edit")) is not None:
        return r
    name = str(form.get("name", "")).strip()
    url = str(form.get("url", "")).strip()
    target_type = str(form.get("type", "html")).strip().lower()
    selector_raw = str(form.get("selector", "")).strip()
    selector = selector_raw or None
    extract = str(form.get("extract", "text")).strip().lower()
    render_js = form.get("render_js") in {"on", "1", "true", True}
    enabled = form.get("enabled") in {"on", "1", "true", True}
    try:
        timeout_seconds = int(str(form.get("timeout_seconds", "20")).strip() or "20")
    except ValueError:
        timeout_seconds = 20
    headers_text = str(form.get("headers", "") or "")
    headers = parse_headers_text(headers_text)
    notify_values = form.getlist("notify")
    notify = [str(x) for x in notify_values]

    db = AppDB(DB_PATH)
    if db.get_target(target_id) is None:
        db.close()
        return RedirectResponse(url="/", status_code=303)
    db.update_target(
        target_id,
        name=name,
        type=target_type,
        url=url,
        selector=selector,
        extract=extract,
        render_js=bool(render_js),
        timeout_seconds=timeout_seconds,
        headers=headers,
        notify=notify,
        enabled=bool(enabled),
    )
    db.close()
    return RedirectResponse(url="/", status_code=303)


@app.post("/targets/{target_id}/delete")
async def delete_target(request: Request, target_id: int) -> RedirectResponse:
    form = await request.form()
    if (r := _require_csrf_or_redirect(request, str(form.get("csrf_token", "")), redirect_to="/")) is not None:
        return r
    db = AppDB(DB_PATH)
    db.delete_target(target_id)
    db.close()
    return RedirectResponse(url="/", status_code=303)


@app.get("/settings", response_class=HTMLResponse)
def settings(request: Request) -> HTMLResponse:
    db = AppDB(DB_PATH)
    notifiers = {n.name: n for n in db.list_notifiers()}
    interval_seconds = db.get_setting_int("interval_seconds", 300)
    scheduler_enabled = db.get_setting_bool("scheduler_enabled", True)
    notify_on_first = db.get_setting_bool("notify_on_first", False)
    last_run = db.get_last_run()
    db.close()
    return _render(
        request,
        "settings.html",
        {
            "notifiers": notifiers,
            "interval_seconds": interval_seconds,
            "scheduler_enabled": scheduler_enabled,
            "notify_on_first": notify_on_first,
            "last_run": last_run,
            "format_ts": format_ts,
        },
    )


@app.post("/settings")
async def save_settings(request: Request) -> RedirectResponse:
    form = await request.form()
    if (r := _require_csrf_or_redirect(request, str(form.get("csrf_token", "")), redirect_to="/settings")) is not None:
        return r
    db = AppDB(DB_PATH)

    try:
        interval_seconds = int(str(form.get("interval_seconds", "300")).strip() or "300")
    except ValueError:
        interval_seconds = 300
    interval_seconds = max(10, interval_seconds)

    scheduler_enabled = form.get("scheduler_enabled") in {"on", "1", "true", True}
    notify_on_first = form.get("notify_on_first") in {"on", "1", "true", True}

    db.set_setting("interval_seconds", str(interval_seconds))
    db.set_setting("scheduler_enabled", "1" if scheduler_enabled else "0")
    db.set_setting("notify_on_first", "1" if notify_on_first else "0")

    for name in ["stdout", "macos", "telegram", "pushover"]:
        current = db.get_notifier(name)
        current_cfg = dict(current.config) if current else {}
        enabled = form.get(f"notifier_{name}_enabled") in {"on", "1", "true", True}

        if name == "telegram":
            bot_token = str(form.get("telegram_bot_token", "") or "").strip()
            chat_id = str(form.get("telegram_chat_id", "") or "").strip()
            if bot_token:
                current_cfg["bot_token"] = bot_token
            if chat_id:
                current_cfg["chat_id"] = chat_id
        elif name == "pushover":
            app_token = str(form.get("pushover_app_token", "") or "").strip()
            user_key = str(form.get("pushover_user_key", "") or "").strip()
            if app_token:
                current_cfg["app_token"] = app_token
            if user_key:
                current_cfg["user_key"] = user_key

        db.upsert_notifier(name, enabled=bool(enabled), config=current_cfg)

    db.close()
    return RedirectResponse(url="/settings", status_code=303)


@app.post("/preview", response_class=HTMLResponse)
async def preview(request: Request) -> HTMLResponse:
    form = await request.form()
    if (r := _require_csrf_or_redirect(request, str(form.get("csrf_token", "")), redirect_to="/targets/new")) is not None:
        return _render(request, "error.html", {"message": "Invalid CSRF token"}, status_code=400)
    url = str(form.get("url", "")).strip()
    target_type = str(form.get("type", "html")).strip().lower()
    selector_raw = str(form.get("selector", "")).strip()
    selector = selector_raw or None
    extract = str(form.get("extract", "text")).strip().lower()
    render_js = form.get("render_js") in {"on", "1", "true", True}
    try:
        timeout_seconds = int(str(form.get("timeout_seconds", "20")).strip() or "20")
    except ValueError:
        timeout_seconds = 20
    headers_text = str(form.get("headers", "") or "")
    headers = parse_headers_text(headers_text)

    result: dict[str, Any] = {"ok": False, "error": "URL is required."}
    if url:
        try:
            signature, content = check_target(
                {
                    "type": target_type,
                    "url": url,
                    "selector": selector,
                    "extract": extract,
                    "render_js": bool(render_js),
                    "timeout_seconds": timeout_seconds,
                    "headers": headers,
                }
            )
            content_out = content
            if len(content_out) > 8000:
                content_out = content_out[:7980] + "\n…(truncated)"
            result = {"ok": True, "signature": signature, "content": content_out}
        except Exception as e:
            result = {"ok": False, "error": str(e)}

    return _render(
        request,
        "preview.html",
        {
            "url": url,
            "type": target_type,
            "selector": selector_raw,
            "extract": extract,
            "render_js": bool(render_js),
            "timeout_seconds": timeout_seconds,
            "headers": headers_text,
            "result": result,
        },
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/login", response_class=HTMLResponse)
def login_get(request: Request, next: str | None = None) -> HTMLResponse:
    if is_auth_disabled():
        return _render(request, "error.html", {"message": "Auth is disabled."}, status_code=400)
    if is_authenticated(request.session):
        return _render(request, "error.html", {"message": "Already logged in."}, status_code=400)
    return _render(request, "login.html", {"next": _safe_next(next), "error": ""})


@app.post("/login")
async def login_post(request: Request) -> RedirectResponse | HTMLResponse:
    if is_auth_disabled():
        return RedirectResponse(url="/", status_code=303)
    form = await request.form()
    csrf = str(form.get("csrf_token", "") or "")
    if not validate_csrf(request.session, csrf):
        return _render(request, "login.html", {"next": "/", "error": "Invalid CSRF token."}, status_code=400)

    username = str(form.get("username", "")).strip()
    password = str(form.get("password", "")).strip()
    next_path = _safe_next(str(form.get("next", "") or ""))

    ok, msg = verify_login(username, password)
    if not ok:
        return _render(request, "login.html", {"next": next_path, "error": msg}, status_code=401)

    request.session.clear()
    login_session(request.session, msg)
    return RedirectResponse(url=next_path, status_code=303)


@app.post("/logout")
async def logout_post(request: Request) -> RedirectResponse:
    form = await request.form()
    if not validate_csrf(request.session, str(form.get("csrf_token", "") or "")):
        return RedirectResponse(url="/", status_code=303)
    logout_session(request.session)
    return RedirectResponse(url="/login", status_code=303)
