from __future__ import annotations

import re
from typing import Optional

from bs4 import BeautifulSoup


class ExtractionError(RuntimeError):
    pass


_re_whitespace = re.compile(r"[ \t\r\f\v]+")


def extract_from_html(html: str, *, selector: Optional[str], mode: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    node = soup.select_one(selector) if selector else soup
    if node is None:
        raise ExtractionError(f"CSS selector not found: {selector}")

    if mode == "html":
        return str(node)
    if mode != "text":
        raise ExtractionError(f"Unknown extract mode: {mode} (expected 'text' or 'html')")

    for tag in node.select("script,style,noscript"):
        tag.decompose()

    text = node.get_text("\n", strip=True)
    text = text.replace("\u00a0", " ")
    lines = []
    for line in text.splitlines():
        line = _re_whitespace.sub(" ", line).strip()
        if line:
            lines.append(line)
    return "\n".join(lines)

