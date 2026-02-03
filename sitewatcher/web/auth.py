from __future__ import annotations

import base64
import hashlib
import hmac
import os
import secrets
from dataclasses import dataclass
from typing import Optional


AUTH_MODE_ENV = "SITEWATCHER_AUTH_MODE"  # password | disabled
ADMIN_USER_ENV = "SITEWATCHER_ADMIN_USER"
ADMIN_PASSWORD_ENV = "SITEWATCHER_ADMIN_PASSWORD"
ADMIN_PASSWORD_HASH_ENV = "SITEWATCHER_ADMIN_PASSWORD_HASH"

SESSION_AUTH_KEY = "auth_ok"
SESSION_USER_KEY = "auth_user"
SESSION_CSRF_KEY = "csrf_token"


@dataclass(frozen=True)
class AdminAuthConfig:
    mode: str
    username: str
    password_hash: Optional[str]
    password_plain: Optional[str]


def get_auth_config() -> AdminAuthConfig:
    mode = (os.getenv(AUTH_MODE_ENV, "password") or "password").strip().lower()
    username = (os.getenv(ADMIN_USER_ENV, "admin") or "admin").strip()
    password_hash = (os.getenv(ADMIN_PASSWORD_HASH_ENV, "") or "").strip() or None
    password_plain = (os.getenv(ADMIN_PASSWORD_ENV, "") or "").strip() or None
    return AdminAuthConfig(mode=mode, username=username, password_hash=password_hash, password_plain=password_plain)


def is_auth_disabled() -> bool:
    return get_auth_config().mode == "disabled"


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


def verify_login(username: str, password: str) -> tuple[bool, str]:
    cfg = get_auth_config()
    if cfg.mode == "disabled":
        return True, cfg.username

    if username != cfg.username:
        return False, "Invalid username or password."

    if cfg.password_hash:
        if verify_password(password, cfg.password_hash):
            return True, username
        return False, "Invalid username or password."

    if cfg.password_plain:
        if hmac.compare_digest(password, cfg.password_plain):
            return True, username
        return False, "Invalid username or password."

    return False, "Server auth is not configured. Set SITEWATCHER_ADMIN_PASSWORD_HASH (recommended) or SITEWATCHER_ADMIN_PASSWORD."


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


def is_authenticated(session: dict) -> bool:
    return bool(session.get(SESSION_AUTH_KEY))


def login_session(session: dict, username: str) -> None:
    session[SESSION_AUTH_KEY] = True
    session[SESSION_USER_KEY] = username
    ensure_csrf_token(session)


def logout_session(session: dict) -> None:
    session.pop(SESSION_AUTH_KEY, None)
    session.pop(SESSION_USER_KEY, None)

