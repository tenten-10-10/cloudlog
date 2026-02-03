from __future__ import annotations

import json
import subprocess

from sitewatcher.notifiers.base import Notification, Notifier


class MacOSNotifier(Notifier):
    name = "macos"

    def send(self, notification: Notification) -> None:
        # AppleScript string escaping: JSON string escaping is compatible here.
        title = json.dumps(notification.title)
        message = json.dumps(notification.message)
        script = f"display notification {message} with title {title}"
        subprocess.run(["osascript", "-e", script], check=False)

