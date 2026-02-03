from __future__ import annotations

import ipaddress
import logging
import os
import socket
from dataclasses import dataclass
from typing import Any, Mapping, Optional
from urllib.parse import urljoin, urlparse

import requests


DEFAULT_HEADERS = {
    "User-Agent": "SiteWatcher/0.1 (+https://localhost/)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


@dataclass(frozen=True)
class FetchResult:
    url: str
    status_code: int
    content_type: str
    text: str


def _allow_private_network() -> bool:
    raw = (os.getenv("SITEWATCHER_ALLOW_PRIVATE_NETWORK", "0") or "0").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _is_ip_allowed(ip: ipaddress._BaseAddress, *, allow_private: bool) -> bool:
    if ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_unspecified:
        return False
    if getattr(ip, "is_private", False) and not allow_private:
        return False
    if getattr(ip, "is_reserved", False):
        return False
    return True


def _validate_url_for_fetch(url: str) -> None:
    allow_private = _allow_private_network()
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("Only http/https URLs are allowed.")
    if not parsed.netloc:
        raise ValueError("Invalid URL (missing host).")
    if parsed.username or parsed.password:
        raise ValueError("Credentials in URL are not allowed.")

    host = parsed.hostname
    if not host:
        raise ValueError("Invalid URL host.")
    if host.lower() == "localhost" and not allow_private:
        raise ValueError("localhost is not allowed.")

    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        ip = None
    if ip is not None:
        if not _is_ip_allowed(ip, allow_private=allow_private):
            raise ValueError("Target IP is not allowed.")
        return

    try:
        infos = socket.getaddrinfo(host, parsed.port or (443 if parsed.scheme == "https" else 80), type=socket.SOCK_STREAM)
    except socket.gaierror as e:
        raise ValueError(f"DNS resolution failed for host: {host}") from e

    for family, _, _, _, sockaddr in infos:
        if family == socket.AF_INET:
            ip_s = sockaddr[0]
        elif family == socket.AF_INET6:
            ip_s = sockaddr[0]
        else:
            continue
        ip = ipaddress.ip_address(ip_s)
        if not _is_ip_allowed(ip, allow_private=allow_private):
            raise ValueError("Resolved IP is not allowed.")


def fetch_text(
    url: str,
    *,
    timeout_seconds: int = 20,
    headers: Optional[Mapping[str, str]] = None,
) -> FetchResult:
    merged_headers: dict[str, str] = dict(DEFAULT_HEADERS)
    if headers:
        merged_headers.update(dict(headers))

    session = requests.Session()
    session.trust_env = False

    current = url
    for _ in range(6):
        _validate_url_for_fetch(current)
        resp = session.get(current, headers=merged_headers, timeout=timeout_seconds, allow_redirects=False)
        if resp.status_code in {301, 302, 303, 307, 308}:
            loc = resp.headers.get("location", "")
            if not loc:
                raise RuntimeError("Redirect without Location header.")
            current = urljoin(current, loc)
            continue

        resp.raise_for_status()
        content_type = resp.headers.get("content-type", "")
        return FetchResult(url=current, status_code=resp.status_code, content_type=content_type, text=resp.text)

    raise RuntimeError("Too many redirects.")


def fetch_rendered_html(
    url: str,
    *,
    timeout_seconds: int = 30,
    wait_until: str = "domcontentloaded",
    extra_wait_ms: int = 0,
) -> str:
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "render_js=true requires playwright. Install: pip install playwright && playwright install"
        ) from e

    _validate_url_for_fetch(url)
    logging.debug("Fetching with Playwright: %s", url)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.goto(url, wait_until=wait_until, timeout=timeout_seconds * 1000)
            if extra_wait_ms > 0:
                page.wait_for_timeout(extra_wait_ms)
            return page.content()
        finally:
            browser.close()


def get_headers_from_target(target: Mapping[str, Any]) -> Mapping[str, str]:
    headers = target.get("headers")
    if headers is None:
        return {}
    if not isinstance(headers, dict):
        raise TypeError("target.headers must be a mapping")
    return {str(k): str(v) for k, v in headers.items()}
