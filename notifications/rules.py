"""Notification rules loader."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Mapping


@dataclass(slots=True, frozen=True)
class NotificationDefaults:
    locale: str
    channel_priority: tuple[str, ...]
    check_flags: tuple[str, ...]


@dataclass(slots=True, frozen=True)
class NotificationEvent:
    key: str
    name: str
    recipient: str
    template: str
    variables: tuple[str, ...]
    delay_minutes: int = 0


class NotificationRules:
    """Container for notification events and defaults."""

    def __init__(self, defaults: NotificationDefaults, events: Mapping[str, NotificationEvent]):
        self.defaults = defaults
        self._events = dict(events)

    def get_event(self, key: str) -> NotificationEvent:
        try:
            return self._events[key]
        except KeyError as exc:  # pragma: no cover - configuration error
            raise KeyError(f"Notification event '{key}' is not defined") from exc

    @property
    def locale(self) -> str:
        return self.defaults.locale


def _parse_defaults(data: Mapping[str, Any]) -> NotificationDefaults:
    locale = str(data.get("locale") or "ru-RU")
    channel_priority = tuple(str(x) for x in data.get("channel_priority", []) if x)
    check_flags = tuple(str(x) for x in data.get("check_flags", []) if x)
    return NotificationDefaults(locale=locale, channel_priority=channel_priority, check_flags=check_flags)


def _parse_event(entry: Mapping[str, Any]) -> NotificationEvent:
    timing = entry.get("timing") or {}
    delay_minutes = int(timing.get("delay_minutes") or 0)
    variables = tuple(str(var) for var in entry.get("variables", []) if var)
    return NotificationEvent(
        key=str(entry.get("key")),
        name=str(entry.get("name") or entry.get("key") or ""),
        recipient=str(entry.get("recipient") or "client"),
        template=str(entry.get("template") or ""),
        variables=variables,
        delay_minutes=delay_minutes,
    )


def load_notification_rules(path: str | Path) -> NotificationRules:
    file_path = Path(path)
    with file_path.open("r", encoding="utf-8") as fp:
        raw = json.load(fp)
    defaults = _parse_defaults(raw.get("defaults") or {})
    events_data = raw.get("events") or []
    events = {item.get("key"): _parse_event(item) for item in events_data if item.get("key")}
    return NotificationRules(defaults=defaults, events=events)


__all__ = [
    "NotificationDefaults",
    "NotificationEvent",
    "NotificationRules",
    "load_notification_rules",
]
