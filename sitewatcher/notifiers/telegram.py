from __future__ import annotations

import os
from dataclasses import dataclass

import requests

from sitewatcher.notifiers.base import Notification, Notifier


@dataclass(frozen=True)
class TelegramConfig:
    bot_token: str
    chat_id: str


class TelegramNotifier(Notifier):
    name = "telegram"

    def __init__(self, cfg: TelegramConfig) -> None:
        self._cfg = cfg

    @staticmethod
    def from_env(bot_token_env: str, chat_id_env: str) -> "TelegramNotifier":
        bot_token = os.getenv(bot_token_env, "").strip()
        chat_id = os.getenv(chat_id_env, "").strip()
        if not bot_token or not chat_id:
            raise RuntimeError(f"Missing env vars for Telegram: {bot_token_env}, {chat_id_env}")
        return TelegramNotifier(TelegramConfig(bot_token=bot_token, chat_id=chat_id))

    def send(self, notification: Notification) -> None:
        url = f"https://api.telegram.org/bot{self._cfg.bot_token}/sendMessage"
        # Telegram limit: 4096 chars. Keep some headroom.
        text = f"{notification.title}\n\n{notification.message}"
        if len(text) > 3800:
            text = text[:3790] + "\n…(truncated)"
        resp = requests.post(
            url,
            json={
                "chat_id": self._cfg.chat_id,
                "text": text,
                "disable_web_page_preview": True,
            },
            timeout=20,
        )
        resp.raise_for_status()
