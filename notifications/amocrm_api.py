"""amoCRM API polling helpers for admin alerts."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

import aiohttp


@dataclass(slots=True, frozen=True)
class AmoCRMEvent:
    event_id: str
    event_type: str
    entity_id: int | None
    entity_type: str | None
    created_at: int
    payload: dict[str, Any]


@dataclass(slots=True, frozen=True)
class AmoCRMLead:
    lead_id: int
    name: str | None
    pipeline_id: int | None
    status_id: int | None
    created_at: int | None
    contact_ids: list[int]
    payload: dict[str, Any]


@dataclass(slots=True, frozen=True)
class AmoCRMAlert:
    alert_type: str
    title: str | None
    lead_id: int | None
    contact_id: int | None
    contact_name: str | None
    phone: str | None
    source: str | None
    text: str | None
    comment: str | None
    link: str | None


class AmoCRMAPIError(RuntimeError):
    def __init__(self, status: int, message: str):
        super().__init__(f"amoCRM API error {status}: {message}")
        self.status = status
        self.message = message


class AmoCRMAPIAuthError(AmoCRMAPIError):
    pass


class AmoCRMAPIRateLimitError(AmoCRMAPIError):
    pass


class AmoCRMAPIClient:
    def __init__(
        self,
        api_base: str,
        token: str,
        *,
        session: aiohttp.ClientSession | Any | None = None,
        timeout_sec: float = 15.0,
    ) -> None:
        self.api_base = api_base.rstrip("/")
        self.token = token.strip()
        self.session = session
        self.timeout_sec = timeout_sec
        self._owns_session = session is None

    async def __aenter__(self) -> "AmoCRMAPIClient":
        if self.session is None:
            self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self._owns_session and self.session is not None:
            await self.session.close()

    async def get(self, path: str, *, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        if self.session is None:
            self.session = aiohttp.ClientSession()
            self._owns_session = True
        url = f"{self.api_base}{path}"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        }
        async with self.session.get(url, headers=headers, params=dict(params or {}), timeout=self.timeout_sec) as resp:
            try:
                payload = await resp.json()
            except Exception:
                payload = {"text": await resp.text()}
            if resp.status == 401:
                raise AmoCRMAPIAuthError(resp.status, str(payload))
            if resp.status == 429:
                raise AmoCRMAPIRateLimitError(resp.status, str(payload))
            if resp.status >= 400:
                raise AmoCRMAPIError(resp.status, str(payload))
            if not isinstance(payload, dict):
                raise AmoCRMAPIError(resp.status, "unexpected non-object response")
            return payload

    async def fetch_events(
        self,
        *,
        event_types: list[str],
        created_from: int,
        limit: int = 100,
        entity: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "filter[type]": ",".join(event_types),
            "filter[created_at][from]": created_from,
            "limit": limit,
        }
        if entity:
            params["filter[entity]"] = entity
        payload = await self.get("/api/v4/events", params=params)
        return list(((payload.get("_embedded") or {}).get("events") or []))

    async def fetch_lead(self, lead_id: int) -> dict[str, Any]:
        return await self.get(f"/api/v4/leads/{lead_id}", params={"with": "contacts"})

    async def fetch_contact(self, contact_id: int) -> dict[str, Any]:
        return await self.get(f"/api/v4/contacts/{contact_id}")

    async def fetch_contact_leads(self, contact_id: int, *, pipeline_id: int | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "filter[contacts][id]": contact_id,
            "with": "contacts",
            "limit": 50,
        }
        if pipeline_id:
            params["filter[pipeline_id]"] = pipeline_id
        payload = await self.get("/api/v4/leads", params=params)
        return list(((payload.get("_embedded") or {}).get("leads") or []))

    async def fetch_lead_notes(self, lead_id: int) -> list[dict[str, Any]]:
        payload = await self.get(f"/api/v4/leads/{lead_id}/notes", params={"limit": 100})
        return list(((payload.get("_embedded") or {}).get("notes") or []))

    async def fetch_unsorted(self, *, pipeline_id: int, created_from: int, limit: int = 100) -> list[dict[str, Any]]:
        payload = await self.get(
            "/api/v4/leads/unsorted",
            params={
                "filter[pipeline_id]": pipeline_id,
                "filter[created_at][from]": created_from,
                "limit": limit,
            },
        )
        return list(((payload.get("_embedded") or {}).get("unsorted") or []))


def extract_contact_phone(contact: Mapping[str, Any] | None) -> str | None:
    if not contact:
        return None
    for field in contact.get("custom_fields_values") or []:
        if not isinstance(field, Mapping):
            continue
        if str(field.get("field_code") or "").upper() != "PHONE":
            continue
        for value in field.get("values") or []:
            if isinstance(value, Mapping) and value.get("value"):
                return str(value["value"]).strip()
    return None


def extract_lead_contact_ids(lead: Mapping[str, Any]) -> list[int]:
    contacts = ((lead.get("_embedded") or {}).get("contacts") or [])
    ids: list[int] = []
    for contact in contacts:
        if not isinstance(contact, Mapping):
            continue
        try:
            ids.append(int(contact["id"]))
        except Exception:
            continue
    return ids


def is_accepted_call_note(note: Mapping[str, Any], *, min_duration_sec: int) -> bool:
    if str(note.get("note_type") or "") != "call_in":
        return False
    params = note.get("params") if isinstance(note.get("params"), Mapping) else {}
    try:
        duration = int(params.get("duration") or 0)
    except Exception:
        duration = 0
    return duration >= min_duration_sec


def is_call_note(note: Mapping[str, Any]) -> bool:
    return str(note.get("note_type") or "") == "call_in"


def extract_event_type(event: Mapping[str, Any]) -> str:
    return str(event.get("type") or "").strip()


def extract_event_message_id(event: Mapping[str, Any]) -> str | None:
    for change in event.get("value_after") or []:
        if not isinstance(change, Mapping):
            continue
        message = change.get("message")
        if isinstance(message, Mapping) and message.get("id"):
            return str(message["id"]).strip()
    return None


def extract_event_entity_id(event: Mapping[str, Any]) -> int | None:
    for key in ("entity_id", "lead_id"):
        if event.get(key) is not None:
            try:
                return int(event[key])
            except Exception:
                return None
    for change in event.get("value_after") or []:
        if not isinstance(change, Mapping):
            continue
        lead = change.get("lead")
        if isinstance(lead, Mapping) and lead.get("id") is not None:
            try:
                return int(lead["id"])
            except Exception:
                return None
    return None


def extract_event_identity(event: Mapping[str, Any]) -> dict[str, Any]:
    identity: dict[str, Any] = {
        "event_id": str(event.get("id") or ""),
        "event_type": extract_event_type(event),
        "created_at": _as_int(event.get("created_at"), 0),
        "message_id": extract_event_message_id(event),
        "lead_id": None,
        "contact_id": None,
        "talk_id": None,
        "text": None,
    }
    for change in event.get("value_after") or []:
        if not isinstance(change, Mapping):
            continue
        for key, target in (("lead", "lead_id"), ("contact", "contact_id"), ("talk", "talk_id")):
            value = change.get(key)
            if isinstance(value, Mapping) and value.get("id") is not None:
                identity[target] = value["id"]
        message = change.get("message")
        if isinstance(message, Mapping):
            identity["text"] = str(message.get("text") or message.get("message") or "").strip() or None
    if identity["contact_id"] is None and str(event.get("entity_type") or "") == "contact" and event.get("entity_id"):
        identity["contact_id"] = _as_int(event.get("entity_id"), 0) or None
    return identity


def normalize_lead(payload: Mapping[str, Any]) -> AmoCRMLead:
    return AmoCRMLead(
        lead_id=int(payload["id"]),
        name=str(payload.get("name") or "").strip() or None,
        pipeline_id=int(payload["pipeline_id"]) if payload.get("pipeline_id") is not None else None,
        status_id=int(payload["status_id"]) if payload.get("status_id") is not None else None,
        created_at=int(payload["created_at"]) if payload.get("created_at") is not None else None,
        contact_ids=extract_lead_contact_ids(payload),
        payload=dict(payload),
    )


def should_skip_new_lead_alert(
    lead: AmoCRMLead,
    *,
    target_pipeline_id: int,
    new_lead_status_id: int,
    notes: list[Mapping[str, Any]],
    accepted_call_min_duration_sec: int | None = None,
) -> bool:
    if lead.pipeline_id != target_pipeline_id:
        return True
    if lead.status_id != new_lead_status_id:
        return False
    return any(is_call_note(note) for note in notes)


def build_new_lead_alert(
    lead: AmoCRMLead,
    *,
    contact: Mapping[str, Any] | None,
    api_base: str,
) -> AmoCRMAlert:
    return AmoCRMAlert(
        alert_type="new_lead",
        title=lead.name,
        lead_id=lead.lead_id,
        contact_id=int(contact["id"]) if contact and contact.get("id") is not None else None,
        contact_name=str(contact.get("name") or "").strip() if contact else None,
        phone=extract_contact_phone(contact),
        source=None,
        text=None,
        comment=None,
        link=build_lead_link(api_base, lead.lead_id),
    )


def build_unsorted_alert(
    item: Mapping[str, Any],
    *,
    api_base: str,
    contact: Mapping[str, Any] | None = None,
) -> AmoCRMAlert:
    embedded = item.get("_embedded") if isinstance(item.get("_embedded"), Mapping) else {}
    leads = embedded.get("leads") or []
    lead_id = None
    if leads and isinstance(leads[0], Mapping) and leads[0].get("id") is not None:
        lead_id = int(leads[0]["id"])
    contacts = embedded.get("contacts") or []
    contact_id = None
    if contacts and isinstance(contacts[0], Mapping) and contacts[0].get("id") is not None:
        contact_id = int(contacts[0]["id"])
    metadata = item.get("metadata") if isinstance(item.get("metadata"), Mapping) else {}
    contact_name = _first_text(
        _nested_text(metadata, "client", "name"),
        str(contact.get("name") or "").strip() if contact else None,
        str(metadata.get("from") or "").strip() if not _extract_phone(str(metadata.get("from") or "")) else None,
        str(metadata.get("name") or "").strip(),
    )
    phone = _first_text(
        extract_contact_phone(contact),
        _extract_phone(str(metadata.get("phone") or "")),
        _extract_phone(str(metadata.get("from") or "")),
    )
    return AmoCRMAlert(
        alert_type="new_unsorted",
        title=None,
        lead_id=lead_id,
        contact_id=contact_id,
        contact_name=contact_name,
        phone=phone,
        source=_human_source_name(item, metadata),
        text=str(metadata.get("text") or metadata.get("message") or "").strip() or None,
        comment=_unsorted_comment(item, metadata),
        link=build_lead_link(api_base, lead_id),
    )


def build_unanswered_message_alert(
    *,
    lead: Mapping[str, Any] | None,
    contact: Mapping[str, Any] | None,
    text: str | None,
    api_base: str,
) -> AmoCRMAlert:
    lead_id = int(lead["id"]) if lead and lead.get("id") is not None else None
    return AmoCRMAlert(
        alert_type="unanswered_message",
        title=str(lead.get("name") or "").strip() if lead else None,
        lead_id=lead_id,
        contact_id=int(contact["id"]) if contact and contact.get("id") is not None else None,
        contact_name=str(contact.get("name") or "").strip() if contact else None,
        phone=extract_contact_phone(contact),
        source=None,
        text=text,
        comment=None,
        link=build_lead_link(api_base, lead_id),
    )


def format_amocrm_api_alert(alert: AmoCRMAlert) -> str:
    labels = {
        "new_lead": "новая сделка",
        "new_unsorted": "новое неразобранное",
        "unanswered_message": "новое входящее сообщение без ответа 10 минут",
    }
    lines = [
        "🚨 amoCRM: новая входящая заявка",
        "",
        f"Тип: {labels.get(alert.alert_type, alert.alert_type)}",
    ]
    if alert.lead_id:
        lines.append(f"Сделка: #{alert.lead_id}")
    if alert.title and alert.alert_type != "new_unsorted":
        lines.append(f"Название: {alert.title}")
    if alert.contact_name:
        lines.append(f"Клиент: {alert.contact_name}")
    if alert.phone:
        lines.append(f"Телефон: {alert.phone}")
    if alert.source:
        lines.append(f"Источник: {alert.source}")
    if alert.text:
        lines.append(f"Текст: {alert.text[:700]}")
    if alert.comment:
        lines.append(f"Комментарий: {alert.comment[:700]}")
    if alert.link:
        lines.append(f"Ссылка: {alert.link}")
    lines.extend(["", "Нужно ответить или позвонить."])
    return "\n".join(lines)


def build_lead_link(api_base: str, lead_id: int | str | None) -> str | None:
    if not api_base or not lead_id:
        return None
    return f"{api_base.rstrip('/')}/leads/detail/{lead_id}"


def _extract_phone(value: str) -> str | None:
    match = re.search(r"(?:\+7|8)\D*\d{3}\D*\d{3}\D*\d{2}\D*\d{2}", value)
    return match.group(0).strip() if match else None


def _first_text(*values: str | None) -> str | None:
    for value in values:
        if value:
            cleaned = str(value).strip()
            if cleaned:
                return cleaned
    return None


def _nested_text(payload: Mapping[str, Any], *path: str) -> str | None:
    current: Any = payload
    for key in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return str(current).strip() if current else None


def _human_source_name(item: Mapping[str, Any], metadata: Mapping[str, Any]) -> str | None:
    raw = _first_text(
        str(metadata.get("source_name") or "").strip(),
        str(metadata.get("service") or "").strip(),
        str(item.get("source_name") or "").strip(),
        str(item.get("category") or "").strip(),
    )
    if not raw:
        return None
    lowered = raw.casefold()
    if lowered == "max":
        return "MAX"
    if "telegram" in lowered or "телеграм" in lowered:
        return "Telegram"
    if "whatsapp" in lowered or "wahelp" in lowered:
        return "WhatsApp/мессенджер"
    if lowered == "sip" or "onlinepbx" in lowered:
        return "Телефония"
    return raw


def _unsorted_comment(item: Mapping[str, Any], metadata: Mapping[str, Any]) -> str:
    category = str(item.get("category") or "").casefold()
    service = str(metadata.get("service") or "").casefold()
    source = str(metadata.get("source_name") or item.get("source_name") or "").casefold()
    if category == "sip" or "onlinepbx" in service or source == "sip":
        return "звонок"
    if category == "chats" or "whatbot" in service:
        return "сообщение"
    if "site" in source or "сайт" in source:
        return "заявка с сайта"
    return "заявка"


def _as_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(str(value).strip())
    except Exception:
        return default


__all__ = [
    "AmoCRMAlert",
    "AmoCRMAPIAuthError",
    "AmoCRMAPIClient",
    "AmoCRMAPIError",
    "AmoCRMAPIRateLimitError",
    "AmoCRMEvent",
    "AmoCRMLead",
    "build_lead_link",
    "build_new_lead_alert",
    "build_unanswered_message_alert",
    "build_unsorted_alert",
    "extract_contact_phone",
    "extract_event_entity_id",
    "extract_event_identity",
    "extract_event_message_id",
    "extract_event_type",
    "extract_lead_contact_ids",
    "format_amocrm_api_alert",
    "is_accepted_call_note",
    "is_call_note",
    "normalize_lead",
    "should_skip_new_lead_alert",
]
