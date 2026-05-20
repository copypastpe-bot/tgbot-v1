"""amoCRM webhook payload normalization and admin alert formatting."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


SUPPORTED_EVENT_TYPES = {"leads.add", "unsorted.add", "message.add", "leads.chat"}


@dataclass(slots=True, frozen=True)
class AmoCRMEvent:
    event_type: str
    entity_kind: str | None
    entity_id: str | None
    payload: dict[str, str]
    lead_id: str | None = None
    title: str | None = None
    amount: str | None = None
    source: str | None = None
    phone: str | None = None
    contact_name: str | None = None
    text: str | None = None
    responsible_user_id: str | None = None
    pipeline_id: str | None = None
    status_id: str | None = None

    @property
    def is_supported(self) -> bool:
        return self.event_type in SUPPORTED_EVENT_TYPES


def normalize_amocrm_payload(payload: Mapping[str, Any]) -> AmoCRMEvent:
    flat = _stringify_payload(payload)
    event_type = _detect_event_type(flat)

    if event_type == "leads.add":
        entity_id = _find_first(flat, ("leads[add]",), ("[id]",))
        return AmoCRMEvent(
            event_type=event_type,
            entity_kind="lead",
            entity_id=entity_id,
            lead_id=entity_id,
            payload=flat,
            title=_find_first(flat, ("leads[add]",), ("[name]",)),
            amount=_find_first(flat, ("leads[add]",), ("[price]",)),
            responsible_user_id=_find_first(flat, ("leads[add]",), ("[responsible_user_id]",)),
            pipeline_id=_find_first(flat, ("leads[add]",), ("[pipeline_id]",)),
            status_id=_find_first(flat, ("leads[add]",), ("[status_id]",)),
        )

    if event_type == "unsorted.add":
        entity_id = _find_first(flat, ("unsorted[add]",), ("[uid]", "[id]"))
        return AmoCRMEvent(
            event_type=event_type,
            entity_kind="unsorted",
            entity_id=entity_id,
            payload=flat,
            title=_find_first(flat, ("unsorted[add]",), ("[data][name]", "[name]")),
            source=_find_first(flat, ("unsorted[add]",), ("[source_name]", "[source]", "[origin]")),
            phone=_find_first(flat, ("unsorted[add]",), ("[data][phone]", "[phone]")),
            contact_name=_find_first(flat, ("unsorted[add]",), ("[data][contact][name]", "[contact][name]")),
        )

    if event_type == "message.add":
        entity_id = _find_first(flat, ("message[add]", "messages[add]"), ("[id]",))
        lead_id = _find_first(flat, ("message[add]", "messages[add]"), ("[entity_id]", "[lead_id]"))
        return AmoCRMEvent(
            event_type=event_type,
            entity_kind="message",
            entity_id=entity_id,
            lead_id=lead_id,
            payload=flat,
            source=_find_first(flat, ("message[add]", "messages[add]"), ("[origin]", "[source]", "[source_name]")),
            contact_name=_find_first(flat, ("message[add]", "messages[add]"), ("[author][name]", "[contact][name]", "[name]")),
            text=_find_first(flat, ("message[add]", "messages[add]"), ("[text]", "[message]", "[body]")),
        )

    if event_type == "leads.chat":
        entity_id = _find_first(flat, ("leads[chat]",), ("[id]", "[entity_id]", "[lead_id]"))
        return AmoCRMEvent(
            event_type=event_type,
            entity_kind="chat",
            entity_id=entity_id,
            lead_id=entity_id,
            payload=flat,
            contact_name=_find_first(flat, ("leads[chat]",), ("[contact][name]", "[author][name]", "[name]")),
            text=_find_first(flat, ("leads[chat]",), ("[message]", "[text]", "[body]")),
            source=_find_first(flat, ("leads[chat]",), ("[origin]", "[source]", "[source_name]")),
        )

    return AmoCRMEvent(
        event_type="unknown",
        entity_kind=None,
        entity_id=_find_first(flat, ("",), ("[id]",)),
        payload=flat,
    )


def format_amocrm_admin_alert(event: AmoCRMEvent, *, account_domain: str | None = None) -> str:
    label = {
        "leads.add": "новая сделка",
        "unsorted.add": "новое неразобранное",
        "message.add": "новое входящее сообщение",
        "leads.chat": "новый чат/сообщение",
    }.get(event.event_type, "неизвестное событие")

    lines = [
        "🚨 amoCRM: новая входящая заявка",
        "",
        f"Тип: {label}",
    ]
    lead_id = event.lead_id if event.lead_id else event.entity_id if event.entity_kind == "lead" else None
    if lead_id:
        lines.append(f"Сделка: #{lead_id}")
    if event.title:
        lines.append(f"Название: {event.title}")
    if event.contact_name:
        lines.append(f"Клиент: {event.contact_name}")
    if event.phone:
        lines.append(f"Телефон: {event.phone}")
    if event.source:
        lines.append(f"Источник: {event.source}")
    if event.amount:
        lines.append(f"Сумма: {event.amount}")
    if event.text:
        lines.append(f"Текст: {_clip(event.text, 700)}")
    link = _build_lead_link(account_domain, lead_id)
    if link:
        lines.append(f"Ссылка: {link}")
    lines.extend(["", "Нужно ответить или позвонить."])
    return "\n".join(lines)


def _detect_event_type(payload: Mapping[str, str]) -> str:
    keys = tuple(payload.keys())
    if any(key.startswith("leads[add]") for key in keys):
        return "leads.add"
    if any(key.startswith("unsorted[add]") for key in keys):
        return "unsorted.add"
    if any(key.startswith("message[add]") or key.startswith("messages[add]") for key in keys):
        return "message.add"
    if any(key.startswith("leads[chat]") for key in keys):
        return "leads.chat"
    return "unknown"


def _stringify_payload(payload: Mapping[str, Any]) -> dict[str, str]:
    flat: dict[str, str] = {}

    def visit(prefix: str, value: Any) -> None:
        if isinstance(value, Mapping):
            for child_key, child_value in value.items():
                child_prefix = f"{prefix}[{child_key}]" if prefix else str(child_key)
                visit(child_prefix, child_value)
            return
        if isinstance(value, list):
            for idx, child_value in enumerate(value):
                visit(f"{prefix}[{idx}]", child_value)
            return
        flat[prefix] = "" if value is None else str(value)

    for key, value in payload.items():
        visit(str(key), value)
    return flat


def _find_first(payload: Mapping[str, str], prefixes: tuple[str, ...], suffixes: tuple[str, ...]) -> str | None:
    for suffix in suffixes:
        for key, value in payload.items():
            if not value:
                continue
            if prefixes != ("",) and not any(key.startswith(prefix) for prefix in prefixes):
                continue
            if key.endswith(suffix):
                return value
    return None


def _build_lead_link(account_domain: str | None, lead_id: str | None) -> str | None:
    if not account_domain or not lead_id:
        return None
    domain = account_domain.strip().removeprefix("https://").removeprefix("http://").strip("/")
    if not domain:
        return None
    return f"https://{domain}/leads/detail/{lead_id}"


def _clip(value: str, limit: int) -> str:
    text = " ".join(value.split())
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


__all__ = [
    "AmoCRMEvent",
    "SUPPORTED_EVENT_TYPES",
    "format_amocrm_admin_alert",
    "normalize_amocrm_payload",
]
