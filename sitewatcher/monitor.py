from __future__ import annotations

import hashlib
import logging
import os
import time
from pathlib import Path
from typing import Any, Iterable, Mapping

import feedparser

from sitewatcher.config import resolve_data_dir
from sitewatcher.diffutil import unified_diff
from sitewatcher.extract import ExtractionError, extract_from_html
from sitewatcher.fetchers import fetch_rendered_html, fetch_text, get_headers_from_target
from sitewatcher.notifiers.base import Notification, Notifier
from sitewatcher.notifiers.macos import MacOSNotifier
from sitewatcher.notifiers.pushover import PushoverConfig, PushoverNotifier
from sitewatcher.notifiers.stdout import StdoutNotifier
from sitewatcher.notifiers.telegram import TelegramConfig, TelegramNotifier
from sitewatcher.storage import StateStore, TargetState


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _build_notifiers(config: Mapping[str, Any]) -> dict[str, Notifier]:
    out: dict[str, Notifier] = {}
    ncfg = config.get("notifiers", {}) or {}

    if (ncfg.get("stdout", {}) or {}).get("enabled", False):
        out["stdout"] = StdoutNotifier()

    if (ncfg.get("macos", {}) or {}).get("enabled", False):
        out["macos"] = MacOSNotifier()

    if (tcfg := (ncfg.get("telegram", {}) or {})).get("enabled", False):
        try:
            bot_token_env = str(tcfg.get("bot_token_env", "TELEGRAM_BOT_TOKEN"))
            chat_id_env = str(tcfg.get("chat_id_env", "TELEGRAM_CHAT_ID"))
            bot_token = str(tcfg.get("bot_token", "") or "").strip() or os.getenv(bot_token_env, "").strip()
            chat_id = str(tcfg.get("chat_id", "") or "").strip() or os.getenv(chat_id_env, "").strip()
            if bot_token and chat_id:
                out["telegram"] = TelegramNotifier(TelegramConfig(bot_token=bot_token, chat_id=chat_id))
            else:
                logging.warning("Telegram enabled but not configured (missing bot_token/chat_id).")
        except Exception:
            logging.exception("Failed to configure Telegram notifier")

    if (pcfg := (ncfg.get("pushover", {}) or {})).get("enabled", False):
        try:
            app_token_env = str(pcfg.get("app_token_env", "PUSHOVER_APP_TOKEN"))
            user_key_env = str(pcfg.get("user_key_env", "PUSHOVER_USER_KEY"))
            app_token = str(pcfg.get("app_token", "") or "").strip() or os.getenv(app_token_env, "").strip()
            user_key = str(pcfg.get("user_key", "") or "").strip() or os.getenv(user_key_env, "").strip()
            if app_token and user_key:
                out["pushover"] = PushoverNotifier(PushoverConfig(app_token=app_token, user_key=user_key))
            else:
                logging.warning("Pushover enabled but not configured (missing app_token/user_key).")
        except Exception:
            logging.exception("Failed to configure Pushover notifier")

    return out


def _select_notifiers(all_notifiers: Mapping[str, Notifier], notify_list: Iterable[Any]) -> list[Notifier]:
    selected: list[Notifier] = []
    for name in notify_list:
        key = str(name)
        n = all_notifiers.get(key)
        if n is None:
            logging.warning("Notifier not found or disabled: %s", key)
            continue
        selected.append(n)
    return selected


def _notify(notifiers: Iterable[Notifier], *, title: str, message: str) -> None:
    notification = Notification(title=title, message=message)
    for n in notifiers:
        try:
            n.send(notification)
        except Exception:
            logging.exception("Notifier failed: %s", getattr(n, "name", n.__class__.__name__))


def check_html_target(target: Mapping[str, Any]) -> tuple[str, str]:
    url = str(target["url"])
    timeout_seconds = int(target.get("timeout_seconds", 20))
    selector = target.get("selector")
    selector = str(selector) if selector else None
    mode = str(target.get("extract", "text")).lower()
    render_js = bool(target.get("render_js", False))

    if render_js:
        html = fetch_rendered_html(
            url,
            timeout_seconds=timeout_seconds,
            wait_until=str(target.get("wait_until", "domcontentloaded")),
            extra_wait_ms=int(target.get("extra_wait_ms", 0)),
        )
    else:
        res = fetch_text(url, timeout_seconds=timeout_seconds, headers=get_headers_from_target(target))
        html = res.text

    extracted = extract_from_html(html, selector=selector, mode=mode)
    return _sha256(extracted), extracted


def check_rss_target(target: Mapping[str, Any]) -> tuple[str, str]:
    url = str(target["url"])
    timeout_seconds = int(target.get("timeout_seconds", 20))
    res = fetch_text(url, timeout_seconds=timeout_seconds, headers=get_headers_from_target(target))
    feed = feedparser.parse(res.text)

    if not feed.entries:
        signature = _sha256("")
        return signature, "No entries found."

    # Take the newest entry by the order provided.
    entry = feed.entries[0]
    entry_id = str(getattr(entry, "id", "") or getattr(entry, "link", "") or getattr(entry, "title", ""))
    title = str(getattr(entry, "title", ""))
    link = str(getattr(entry, "link", ""))
    content = "\n".join([s for s in [title, link] if s]).strip()
    signature = _sha256(entry_id + "\n" + content)
    return signature, content or entry_id


def check_target(target: Mapping[str, Any]) -> tuple[str, str]:
    target_type = str(target.get("type", "html")).lower()
    if target_type == "rss":
        return check_rss_target(target)
    return check_html_target(target)


def _build_change_message(
    *,
    target_name: str,
    target_type: str,
    url: str,
    old_state: TargetState | None,
    new_content: str,
    max_chars: int = 3500,
) -> str:
    header = f"監視: {target_name}\n種類: {target_type}\nURL: {url}\n"
    if old_state is None:
        body = f"\n初回スナップショットを保存しました。\n\n{new_content}"
    else:
        diff = unified_diff(old_state.content, new_content, fromfile="before", tofile="after", n=3)
        body = f"\n変更を検知しました。\n\n{diff or '(diff unavailable)'}"

    msg = header + body
    if len(msg) > max_chars:
        msg = msg[: max_chars - 20] + "\n…(truncated)"
    return msg


def run_once(
    config: Mapping[str, Any],
    *,
    config_path: Path | None = None,
    data_dir: Path | None = None,
) -> None:
    if data_dir is None:
        if config_path is None:
            raise ValueError("config_path is required when data_dir is not provided.")
        data_dir = resolve_data_dir(dict(config), config_path=config_path)
    store = StateStore(data_dir / "state.sqlite3")
    try:
        all_notifiers = _build_notifiers(config)
        notify_on_first = bool(config.get("notify_on_first", False))

        targets = config.get("targets", []) or []
        for target in targets:
            try:
                target_name = str(target["name"])
                state_key = str(target.get("state_key") or target_name)
                target_type = str(target.get("type", "html")).lower()
                url = str(target["url"])

                selected_notifiers = _select_notifiers(all_notifiers, target.get("notify", []) or [])
                if not selected_notifiers:
                    logging.debug("No notifiers selected for target: %s", target_name)

                old_state = store.get(state_key)
                logging.info("Checking: %s (%s)", target_name, target_type)

                signature, content = check_target(target)

                if old_state is None:
                    store.upsert(target_name=state_key, signature=signature, content=content)
                    if notify_on_first:
                        msg = _build_change_message(
                            target_name=target_name,
                            target_type=target_type,
                            url=url,
                            old_state=None,
                            new_content=content,
                        )
                        _notify(selected_notifiers, title=f"[SiteWatcher] {target_name}", message=msg)
                    continue

                if signature != old_state.signature:
                    now = int(time.time())
                    store.upsert(
                        target_name=state_key,
                        signature=signature,
                        content=content,
                        checked_at=now,
                        changed_at=now,
                    )
                    msg = _build_change_message(
                        target_name=target_name,
                        target_type=target_type,
                        url=url,
                        old_state=old_state,
                        new_content=content,
                    )
                    _notify(selected_notifiers, title=f"[SiteWatcher] {target_name}", message=msg)
                else:
                    store.touch_checked(target_name=state_key)
                    logging.info("No change: %s", target_name)
            except ExtractionError as e:
                logging.error("Extraction error (%s): %s", target.get("name"), e)
            except Exception:
                logging.exception("Target failed: %s", target.get("name"))
    finally:
        store.close()
