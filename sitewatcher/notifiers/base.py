from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Notification:
    title: str
    message: str


class Notifier:
    name: str

    def send(self, notification: Notification) -> None:  # pragma: no cover
        raise NotImplementedError

