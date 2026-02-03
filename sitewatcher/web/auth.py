from __future__ import annotations

import base64
import hashlib
import hmac
import os
import secrets
from typing import Optional


AUTH_MODE_ENV = "SITEWATCHER_AUTH_MODE"  # enabled | disabled
ALLOW_REGISTRATION_ENV = "SITEWATCHER_ALLOW_REGISTRATION"

SESSION_USER_ID_KEY = "user_id"
SESSION_USERNAME_KEY = "username"
SESSION_CSRF_KEY = "csrf_token"


def is_auth_disabled() -> bool:
    mode = (os.getenv(AUTH_MODE_ENV, "enabled") or "enabled").strip().lower()
    return mode == "disabled"


def allow_registration() -> bool:
    raw = (os.getenv(ALLOW_REGISTRATION_ENV, "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _b64e(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode("ascii").rstrip("=")


def _b64d(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("ascii"))


def hash_password(password: str, *, iterations: int = 260_000) -> str:
    salt = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"pbkdf2_sha256${iterations}${_b64e(salt)}${_b64e(dk)}"


def verify_password(password: str, encoded: str) -> bool:
    try:
        scheme, iter_s, salt_s, hash_s = encoded.split("$", 3)
        if scheme != "pbkdf2_sha256":
            return False
        iterations = int(iter_s)
        salt = _b64d(salt_s)
        expected = _b64d(hash_s)
    except Exception:
        return False

    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return hmac.compare_digest(dk, expected)


def ensure_csrf_token(session: dict) -> str:
    token = session.get(SESSION_CSRF_KEY)
    if isinstance(token, str) and token:
        return token
    token = secrets.token_urlsafe(32)
    session[SESSION_CSRF_KEY] = token
    return token


def validate_csrf(session: dict, token: str | None) -> bool:
    expected = session.get(SESSION_CSRF_KEY)
    if not expected or not isinstance(expected, str):
        return False
    if not token:
        return False
    return hmac.compare_digest(expected, str(token))


def get_user_id(session: dict) -> Optional[int]:
    raw = session.get(SESSION_USER_ID_KEY)
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def get_username(session: dict) -> Optional[str]:
    raw = session.get(SESSION_USERNAME_KEY)
    if raw is None:
        return None
    s = str(raw).strip()
    return s or None


def is_authenticated(session: dict) -> bool:
    return get_user_id(session) is not None


def login_session(session: dict, *, user_id: int, username: str) -> None:
    session[SESSION_USER_ID_KEY] = int(user_id)
    session[SESSION_USERNAME_KEY] = username
    ensure_csrf_token(session)


def logout_session(session: dict) -> None:
    session.pop(SESSION_USER_ID_KEY, None)
    session.pop(SESSION_USERNAME_KEY, None)

