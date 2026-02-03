from __future__ import annotations

import os
from dataclasses import dataclass

import requests

from sitewatcher.notifiers.base import Notification, Notifier


@dataclass(frozen=True)
class PushoverConfig:
    app_token: str
    user_key: str


class PushoverNotifier(Notifier):
    name = "pushover"

    def __init__(self, cfg: PushoverConfig) -> None:
        self._cfg = cfg

    @staticmethod
    def from_env(app_token_env: str, user_key_env: str) -> "PushoverNotifier":
        app_token = os.getenv(app_token_env, "").strip()
        user_key = os.getenv(user_key_env, "").strip()
        if not app_token or not user_key:
            raise RuntimeError(f"Missing env vars for Pushover: {app_token_env}, {user_key_env}")
        return PushoverNotifier(PushoverConfig(app_token=app_token, user_key=user_key))

    def send(self, notification: Notification) -> None:
        resp = requests.post(
            "https://api.pushover.net/1/messages.json",
            data={
                "token": self._cfg.app_token,
                "user": self._cfg.user_key,
                "title": notification.title,
                "message": notification.message[:1024],
            },
            timeout=20,
        )
        resp.raise_for_status()

