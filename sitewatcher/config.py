from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


class ConfigError(RuntimeError):
    pass


def load_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"Config not found: {path}")

    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ConfigError("Config root must be a mapping (YAML dict).")

    cfg: dict[str, Any] = {}
    cfg["data_dir"] = raw.get("data_dir", ".sitewatcher")
    cfg["interval_seconds"] = int(raw.get("interval_seconds", 300))
    cfg["notify_on_first"] = bool(raw.get("notify_on_first", False))

    notifiers = raw.get("notifiers", {}) or {}
    if not isinstance(notifiers, dict):
        raise ConfigError("notifiers must be a mapping.")
    cfg["notifiers"] = notifiers

    targets = raw.get("targets", []) or []
    if not isinstance(targets, list):
        raise ConfigError("targets must be a list.")

    normalized_targets: list[dict[str, Any]] = []
    for i, t in enumerate(targets):
        if not isinstance(t, dict):
            raise ConfigError(f"targets[{i}] must be a mapping.")
        name = t.get("name")
        url = t.get("url")
        if not name or not url:
            raise ConfigError(f"targets[{i}] requires name and url.")
        target_type = (t.get("type") or "html").lower()
        if target_type not in {"html", "rss"}:
            raise ConfigError(f"targets[{i}].type must be 'html' or 'rss'.")

        normalized = dict(t)
        normalized["type"] = target_type
        normalized.setdefault("timeout_seconds", 20)
        normalized.setdefault("notify", [])
        normalized_targets.append(normalized)

    cfg["targets"] = normalized_targets
    return cfg


def resolve_data_dir(config: dict[str, Any], *, config_path: Path) -> Path:
    data_dir_raw = config.get("data_dir", ".sitewatcher")
    data_dir = Path(data_dir_raw)
    if not data_dir.is_absolute():
        data_dir = (config_path.parent / data_dir).resolve()
    return data_dir

