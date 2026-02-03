from __future__ import annotations

from sitewatcher.notifiers.base import Notification, Notifier


class StdoutNotifier(Notifier):
    name = "stdout"

    def send(self, notification: Notification) -> None:
        print(f"[{notification.title}]\n{notification.message}\n")

