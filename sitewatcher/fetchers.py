from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping, Optional

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


def fetch_text(
    url: str,
    *,
    timeout_seconds: int = 20,
    headers: Optional[Mapping[str, str]] = None,
) -> FetchResult:
    merged_headers: dict[str, str] = dict(DEFAULT_HEADERS)
    if headers:
        merged_headers.update(dict(headers))

    resp = requests.get(url, headers=merged_headers, timeout=timeout_seconds)
    resp.raise_for_status()
    content_type = resp.headers.get("content-type", "")
    return FetchResult(url=url, status_code=resp.status_code, content_type=content_type, text=resp.text)


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

