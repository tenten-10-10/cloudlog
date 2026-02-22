from __future__ import annotations

import importlib
import os
import re

from fastapi.testclient import TestClient


def _setup_env() -> None:
    os.environ["CLOUDLOG_STORAGE"] = "memory"
    os.environ["CLOUDLOG_SECRET_KEY"] = "test-secret-key-32-chars-aaaaaaaa"
    os.environ["CLOUDLOG_HTTPS_ONLY"] = "0"
    os.environ["CLOUDLOG_ALLOWED_HOSTS"] = "localhost,127.0.0.1,testserver"
    os.environ["CLOUDLOG_BOOTSTRAP_ADMIN_EMAIL"] = "admin@example.com"
    os.environ["CLOUDLOG_BOOTSTRAP_ADMIN_PASSWORD"] = "ChangeMe123!"


def _load_app():
    _setup_env()
    mod = importlib.import_module("cloudlog.app")
    mod = importlib.reload(mod)
    return mod.app


def _extract_csrf(html: str) -> str:
    m = re.search(r'name="csrf_token"\s+value="([^"]+)"', html)
    assert m, "csrf token not found"
    return m.group(1)


def _login(client: TestClient) -> None:
    res = client.post(
        "/auth/login",
        data={
            "email": "admin@example.com",
            "password": "ChangeMe123!",
            "next": "/today",
        },
        follow_redirects=False,
    )
    assert res.status_code == 303


def test_health_and_auth_flow() -> None:
    app = _load_app()
    client = TestClient(app)

    r = client.get("/health")
    assert r.status_code == 200
    assert r.text == "ok"

    login_page = client.get("/login")
    assert login_page.status_code == 200

    _login(client)

    today = client.get("/today")
    assert today.status_code == 200

    csrf = _extract_csrf(today.text)

    cin = client.post("/events/clock-in", data={"csrf_token": csrf}, follow_redirects=False)
    assert cin.status_code in {200, 303}

    today_api = client.get("/attendance/today")
    assert today_api.status_code == 200
    payload = today_api.json()
    assert payload["ok"] is True
    assert "mom" in payload
    assert "recent_events" in payload
    assert len(payload["recent_events"]) >= 1

    target_event_id = payload["recent_events"][0]["event_id"]
    edited = client.post(
        f"/events/{target_event_id}/edit",
        data={
            "csrf_token": csrf,
            "event_type": "IN",
            "event_time": "2026-02-22T09:00:00",
            "note": "修正テスト",
        },
        headers={"accept": "application/json"},
    )
    assert edited.status_code == 200
    assert edited.json()["ok"] is True
    assert edited.json()["event"]["is_edited"] is True

    cout = client.post("/events/clock-out", data={"csrf_token": csrf}, follow_redirects=False)
    assert cout.status_code in {200, 303}


def test_admin_pages_and_leave_request() -> None:
    app = _load_app()
    client = TestClient(app)

    _login(client)

    admin_users = client.get("/admin/users")
    assert admin_users.status_code == 200

    leave_page = client.get("/leave")
    assert leave_page.status_code == 200
    csrf = _extract_csrf(leave_page.text)

    created = client.post(
        "/leave-requests",
        data={
            "csrf_token": csrf,
            "leave_date": "2026-02-10",
            "leave_type": "PAID",
            "leave_name": "有給",
            "note": "test",
        },
        follow_redirects=False,
    )
    assert created.status_code in {200, 303}

    leave_api = client.get("/leave-requests")
    assert leave_api.status_code == 200
    payload = leave_api.json()
    assert payload["ok"] is True
    assert len(payload["leave_requests"]) >= 1
