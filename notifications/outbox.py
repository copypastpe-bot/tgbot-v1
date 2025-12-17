"""Database helpers for notification outbox processing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import logging
import re
from typing import Any, Awaitable, Callable, Iterable, Mapping, Sequence

import asyncpg

from .rules import NotificationRules

logger = logging.getLogger(__name__)

STATUS_PRIORITY = {
    "pending": 0,
    "sending": 1,
    "sent": 2,
    "delivered": 3,
    "read": 4,
    "failed": -1,
    "cancelled": -1,
}


@dataclass(slots=True)
class NotificationOutboxEntry:
    id: int
    event_key: str
    recipient_kind: str
    client_id: int
    template: str
    payload: Mapping[str, Any]
    locale: str
    scheduled_at: datetime
    attempts: int
    client_phone: str | None
    client_name: str | None
    client_preferred_channel: str | None
    client_user_id_wa: int | None
    client_user_id_tg: int | None
    client_requires_connection: bool
    notifications_enabled: bool
    bot_tg_user_id: int | None = None
    bot_started: bool = False
    preferred_contact: str | None = None


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _serialize_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, (datetime,)):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    if hasattr(value, "isoformat") and callable(value.isoformat):  # date
        try:
            return value.isoformat()
        except Exception:  # pragma: no cover - fallback
            return str(value)
    return value


def serialize_payload(data: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): _serialize_value(val) for key, val in data.items()}


PLACEHOLDER_RE = re.compile(r"{{\s*([\w\.]+)\s*}}")


def render_template(template: str, payload: Mapping[str, Any]) -> str:
    def _replace(match: re.Match[str]) -> str:
        key = match.group(1)
        value = payload.get(key)
        if value is None:
            return ""
        return str(value)

    return PLACEHOLDER_RE.sub(_replace, template)


async def ensure_notification_schema(conn: asyncpg.Connection) -> None:
    await conn.execute(
        """
        ALTER TABLE clients
        ADD COLUMN IF NOT EXISTS notifications_enabled boolean NOT NULL DEFAULT true;
        """
    )
    await conn.execute(
        """
        ALTER TABLE clients
        ADD COLUMN IF NOT EXISTS wahelp_preferred_channel text;
        ALTER TABLE clients
        ADD COLUMN IF NOT EXISTS wahelp_user_id_tg bigint;
        ALTER TABLE clients
        ADD COLUMN IF NOT EXISTS wahelp_user_id_wa bigint;
        ALTER TABLE clients
        ADD COLUMN IF NOT EXISTS wahelp_requires_connection boolean NOT NULL DEFAULT false;
        """
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS wahelp_delivery_issues (
            id bigserial PRIMARY KEY,
            entity_kind text NOT NULL,
            entity_id bigint NOT NULL,
            channel text,
            phone text,
            reason text NOT NULL,
            details text,
            created_at timestamptz NOT NULL DEFAULT NOW(),
            resolved_at timestamptz
        );
        """
    )
    await conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_wahelp_delivery_issues_entity
        ON wahelp_delivery_issues(entity_kind, entity_id, COALESCE(channel,''));
        """
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS notification_outbox (
            id              bigserial PRIMARY KEY,
            event_key       text NOT NULL,
            recipient_kind  text NOT NULL,
            client_id       integer REFERENCES clients(id) ON DELETE CASCADE,
            template        text NOT NULL,
            payload         jsonb NOT NULL,
            locale          text NOT NULL DEFAULT 'ru-RU',
            status          text NOT NULL DEFAULT 'pending',
            scheduled_at    timestamptz NOT NULL DEFAULT NOW(),
            last_attempt_at timestamptz,
            attempts        integer NOT NULL DEFAULT 0,
            last_error      text,
            sent_at         timestamptz,
            created_at      timestamptz NOT NULL DEFAULT NOW(),
            updated_at      timestamptz NOT NULL DEFAULT NOW()
        );
        """
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS notification_messages (
            id                   bigserial PRIMARY KEY,
            outbox_id            bigint REFERENCES notification_outbox(id) ON DELETE SET NULL,
            client_id            integer REFERENCES clients(id) ON DELETE CASCADE,
            event_key            text,
            channel              text NOT NULL,
            message_text         text NOT NULL,
            wahelp_message_id    text,
            status               text NOT NULL DEFAULT 'sent',
            sent_at              timestamptz NOT NULL DEFAULT NOW(),
            delivered_at         timestamptz,
            read_at              timestamptz,
            failed_at            timestamptz,
            last_status_payload  jsonb,
            created_at           timestamptz NOT NULL DEFAULT NOW(),
            updated_at           timestamptz NOT NULL DEFAULT NOW()
        );
        """
    )
    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_notification_outbox_status
        ON notification_outbox(status, scheduled_at);
        """
    )
    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_notification_messages_client
        ON notification_messages(client_id);
        """
    )
    await conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_notification_messages_provider
        ON notification_messages(wahelp_message_id)
        WHERE wahelp_message_id IS NOT NULL;
        """
    )


async def enqueue_notification(
    conn: asyncpg.Connection,
    rules: NotificationRules,
    *,
    event_key: str,
    client_id: int,
    payload: Mapping[str, Any],
    scheduled_at: datetime | None = None,
) -> int:
    event = rules.get_event(event_key)
    schedule_time = scheduled_at
    if schedule_time is None:
        schedule_time = _now_utc() + timedelta(minutes=event.delay_minutes)
    row_id = await conn.fetchval(
        """
        INSERT INTO notification_outbox (event_key, recipient_kind, client_id, template, payload, locale, scheduled_at)
        VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7)
        RETURNING id
        """,
        event.key,
        event.recipient,
        client_id,
        event.template,
        json.dumps(serialize_payload(payload), ensure_ascii=False),
        rules.locale,
        schedule_time,
    )
    return int(row_id)


async def pick_ready_batch(conn: asyncpg.Connection, limit: int = 10) -> list[NotificationOutboxEntry]:
    async with conn.transaction():
        rows = await conn.fetch(
            """
            SELECT
                o.id,
                o.event_key,
                o.recipient_kind,
                o.client_id,
                o.template,
                o.payload,
                o.locale,
                o.scheduled_at,
                o.attempts
            FROM notification_outbox o
            WHERE o.status = 'pending'
              AND o.scheduled_at <= NOW()
            ORDER BY o.scheduled_at, o.id
            LIMIT $1
            FOR UPDATE SKIP LOCKED
            """,
            limit,
        )
        if not rows:
            return []
        ids = [row["id"] for row in rows]
        updated = await conn.fetch(
            """
            UPDATE notification_outbox
            SET status='sending',
                attempts = attempts + 1,
                last_attempt_at = NOW(),
                updated_at = NOW()
            WHERE id = ANY($1::bigint[])
            RETURNING id, attempts
            """,
            ids,
        )
    attempts_map = {row["id"]: row["attempts"] for row in updated}
    client_ids = [row["client_id"] for row in rows if row["client_id"] is not None]
    client_map: dict[int, dict[str, Any]] = {}
    if client_ids:
        client_rows = await conn.fetch(
            """
            SELECT id, full_name, phone, wahelp_preferred_channel, wahelp_user_id_wa, wahelp_user_id_tg, 
                   COALESCE(wahelp_requires_connection, false) AS wahelp_requires_connection, 
                   COALESCE(notifications_enabled, true) AS notifications_enabled,
                   bot_tg_user_id, bot_started, preferred_contact
            FROM clients
            WHERE id = ANY($1::int[])
            """,
            client_ids,
        )
        for crow in client_rows:
            client_map[int(crow["id"])] = {
                "full_name": crow["full_name"],
                "phone": crow["phone"],
                "preferred": crow["wahelp_preferred_channel"],
                "user_id_wa": crow["wahelp_user_id_wa"],
                "user_id_tg": crow["wahelp_user_id_tg"],
                "requires_connection": bool(crow["wahelp_requires_connection"]),
                "enabled": bool(crow["notifications_enabled"]),
                "bot_tg_user_id": crow["bot_tg_user_id"],
                "bot_started": bool(crow["bot_started"]) if crow["bot_started"] else False,
                "preferred_contact": crow["preferred_contact"],
            }
    entries: list[NotificationOutboxEntry] = []
    for row in rows:
        payload = row["payload"] or {}
        if isinstance(payload, str):
            payload = json.loads(payload)
        client_info = client_map.get(row["client_id"] or -1, {})
        entries.append(
            NotificationOutboxEntry(
                id=row["id"],
                event_key=row["event_key"],
                recipient_kind=row["recipient_kind"],
                client_id=row["client_id"],
                template=row["template"],
                payload=payload,
                locale=row["locale"],
                scheduled_at=row["scheduled_at"],
                attempts=attempts_map.get(row["id"], row["attempts"]),
                client_phone=client_info.get("phone"),
                client_name=client_info.get("full_name"),
                client_preferred_channel=client_info.get("preferred"),
                client_user_id_wa=client_info.get("user_id_wa"),
                client_user_id_tg=client_info.get("user_id_tg"),
                client_requires_connection=client_info.get("requires_connection", False),
                notifications_enabled=client_info.get("enabled", True),
                bot_tg_user_id=client_info.get("bot_tg_user_id"),
                bot_started=client_info.get("bot_started", False),
                preferred_contact=client_info.get("preferred_contact"),
            )
        )
    return entries


async def mark_outbox_sent(
    conn: asyncpg.Connection,
    entry: NotificationOutboxEntry,
    *,
    channel: str,
    message_text: str,
    provider_payload: Mapping[str, Any] | None,
    provider_message_id: str | None,
) -> None:
    await conn.execute(
        """
        UPDATE notification_outbox
        SET status='sent',
            sent_at = COALESCE(sent_at, NOW()),
            last_error = NULL,
            updated_at = NOW()
        WHERE id = $1
        """,
        entry.id,
    )
    await conn.execute(
        """
        INSERT INTO notification_messages (
            outbox_id,
            client_id,
            event_key,
            channel,
            message_text,
            wahelp_message_id,
            status,
            last_status_payload
        )
        VALUES ($1,$2,$3,$4,$5,$6,'sent',$7::jsonb)
        """,
        entry.id,
        entry.client_id,
        entry.event_key,
        channel,
        message_text,
        provider_message_id,
        json.dumps(provider_payload, ensure_ascii=False) if provider_payload is not None else None,
    )


async def mark_outbox_failure(
    conn: asyncpg.Connection,
    entry: NotificationOutboxEntry,
    *,
    error_message: str,
    attempts: int,
    max_attempts: int,
    retry_delay_minutes: int = 5,
) -> None:
    status = 'failed' if attempts >= max_attempts else 'pending'
    next_schedule = None
    if status == 'pending':
        next_schedule = _now_utc() + timedelta(minutes=retry_delay_minutes)
    await conn.execute(
        """
        UPDATE notification_outbox
        SET status=$2,
            last_error=$3,
            scheduled_at = COALESCE($4, scheduled_at),
            updated_at = NOW()
        WHERE id=$1
        """,
        entry.id,
        status,
        error_message[:500],
        next_schedule,
    )


async def cancel_outbox_entry(conn: asyncpg.Connection, entry: NotificationOutboxEntry, reason: str) -> None:
    await conn.execute(
        """
        UPDATE notification_outbox
        SET status='cancelled',
            last_error=$2,
            updated_at=NOW()
        WHERE id=$1
        """,
        entry.id,
        reason,
    )


_MESSAGE_ID_KEYS: tuple[str, ...] = (
    "message_id",
    "messageid",
    "message_uuid",
    "messageuuid",
    "wahelp_id",
    "wahelpid",
    "msg_id",
    "msgid",
)


def _extract_id_from_mapping(
    data: Mapping[str, Any] | None,
    *,
    allow_plain_id: bool = False,
) -> str | None:
    if not isinstance(data, Mapping):
        return None
    for key, value in data.items():
        if value is None:
            continue
        normalized = key.lower().replace("-", "_")
        if normalized in _MESSAGE_ID_KEYS:
            return str(value)
    if allow_plain_id:
        plain_id = data.get("id")
        if plain_id:
            return str(plain_id)
    return None


def extract_provider_message_id(payload: Mapping[str, Any] | None) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    direct = _extract_id_from_mapping(payload)
    if direct:
        return direct
    data = payload.get("data")
    data_id = _extract_id_from_mapping(data, allow_plain_id=True)
    if data_id:
        return data_id
    message_section = None
    if isinstance(data, Mapping):
        message_section = data.get("message")
        msg_direct = _extract_id_from_mapping(message_section, allow_plain_id=True)
        if msg_direct:
            return msg_direct
    # Sometimes Wahelp nests payload deeper inside message -> data -> payload
    if isinstance(message_section, Mapping):
        inner = message_section.get("payload")
        inner_id = _extract_id_from_mapping(inner, allow_plain_id=True)
        if inner_id:
            return inner_id
    return None


def _normalize_status(value: str | None, event_name: str | None) -> str | None:
    if value:
        low = value.lower()
    else:
        low = ""
    if not low and event_name:
        event_low = event_name.lower()
        if "message.read" in event_low:
            return "read"
        if "message.delivered" in event_low:
            return "delivered"
        if "message.failed" in event_low:
            return "failed"
    if low in {"sent", "send"}:
        return "sent"
    if low in {"delivered", "delivered_to_recipient"}:
        return "delivered"
    if low in {"read", "seen", "viewed"}:
        return "read"
    if low in {"failed", "error", "not_delivered"}:
        return "failed"
    return None


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except Exception:  # pragma: no cover - invalid timestamp
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            try:
                return datetime.fromtimestamp(float(text), tz=timezone.utc)
            except Exception:  # pragma: no cover - invalid
                return None
    return None


def _status_priority(value: str | None) -> int:
    if value is None:
        return -1
    return STATUS_PRIORITY.get(value, 0)


async def apply_provider_status_update(
    pool: asyncpg.Pool,
    payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    *,
    cancel_followup: Callable[[int], None] | None = None,
    schedule_followup_on_delivered: Callable[[int, str, str, str], Awaitable[None]] | None = None,
) -> bool:
    if isinstance(payload, Sequence) and not isinstance(payload, Mapping):
        handled_any = False
        for item in payload:
            if await apply_provider_status_update(
                pool,
                item,
                cancel_followup=cancel_followup,
                schedule_followup_on_delivered=schedule_followup_on_delivered,
            ):
                handled_any = True
        return handled_any

    if not isinstance(payload, Mapping):
        logger.debug("Webhook payload is not a mapping: %s", payload)
        return False

    data = payload.get("data")
    if isinstance(data, list):
        handled_any = False
        for item in data:
            sub_payload = dict(payload)
            sub_payload["data"] = item
            if await apply_provider_status_update(
                pool,
                sub_payload,
                cancel_followup=cancel_followup,
                schedule_followup_on_delivered=schedule_followup_on_delivered,
            ):
                handled_any = True
        return handled_any

    event_name = str(payload.get("event") or payload.get("type") or "")
    message_id = extract_provider_message_id(payload)
    if not message_id:
        logger.warning(
            "Webhook payload missing message id. event=%s payload=%s",
            event_name,
            payload,
        )
        return False

    status_value = None
    status_time = None
    channel_alias = None
    data_section = payload.get("data")
    if isinstance(data_section, Mapping):
        status_value = data_section.get("status") or data_section.get("state")
        status_time = data_section.get("status_at") or data_section.get("created_at")
        channel = data_section.get("channel")
        if isinstance(channel, Mapping):
            channel_alias = channel.get("alias") or channel.get("name")
        msg_obj = data_section.get("message")
        if isinstance(msg_obj, Mapping):
            status_value = status_value or msg_obj.get("status")
            status_time = status_time or msg_obj.get("status_at")
            if not channel_alias:
                ch = msg_obj.get("channel")
                if isinstance(ch, Mapping):
                    channel_alias = ch.get("alias")
    else:
        status_value = payload.get("status")
        status_time = payload.get("status_at")

    normalized_status = _normalize_status(status_value if isinstance(status_value, str) else None, event_name)
    event_time = _parse_timestamp(status_time) or _now_utc()

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, client_id, channel, status, message_text
            FROM notification_messages
            WHERE wahelp_message_id=$1
            LIMIT 1
            """,
            message_id,
        )
        if not row:
            # try lead_logs fallback
            lead_row = await conn.fetchrow(
                "SELECT id, status FROM lead_logs WHERE wahelp_message_id=$1 LIMIT 1",
                message_id,
            )
            if not lead_row:
                logger.info("Notification message %s not found for webhook", message_id)
                return False
            updates: list[str] = ["last_status_payload = $1::jsonb", "updated_at = NOW()"]
            params: list[Any] = [json.dumps(payload, ensure_ascii=False)]
            idx = 2
            if normalized_status:
                updates.append(f"status = ${idx}")
                params.append(normalized_status)
                idx += 1
            if normalized_status in {"delivered", "read"}:
                updates.append(f"delivered_at = COALESCE(delivered_at, ${idx})")
                params.append(event_time)
                idx += 1
            if normalized_status == "read":
                updates.append(f"read_at = COALESCE(read_at, ${idx})")
                params.append(event_time)
                idx += 1
            if normalized_status == "failed":
                updates.append(f"failed_at = COALESCE(failed_at, ${idx})")
                params.append(event_time)
                idx += 1
            params.append(lead_row["id"])
            sql = "UPDATE lead_logs SET " + ", ".join(updates) + " WHERE id = ${}".format(idx)
            await conn.execute(sql, *params)
            return True

        updates: list[str] = ["last_status_payload = $1::jsonb", "updated_at = NOW()"]
        params: list[Any] = [json.dumps(payload, ensure_ascii=False)]
        param_idx = 2

        if normalized_status:
            current_priority = _status_priority(row["status"])
            new_priority = _status_priority(normalized_status)
            if new_priority >= current_priority:
                updates.append(f"status = ${param_idx}")
                params.append(normalized_status)
                param_idx += 1
        if normalized_status in {"delivered", "read"}:
            updates.append(f"delivered_at = COALESCE(delivered_at, ${param_idx})")
            params.append(event_time)
            param_idx += 1
        if normalized_status == "read":
            updates.append(f"read_at = COALESCE(read_at, ${param_idx})")
            params.append(event_time)
            param_idx += 1
        if normalized_status == "failed":
            updates.append(f"failed_at = COALESCE(failed_at, ${param_idx})")
            params.append(event_time)
            param_idx += 1

        params.append(row["id"])
        sql = "UPDATE notification_messages SET " + ", ".join(updates) + " WHERE id = ${}".format(param_idx)
        await conn.execute(sql, *params)

        if normalized_status == "read" and row["channel"] == "clients_tg" and cancel_followup:
            cancel_followup(row["client_id"])
        if normalized_status == "delivered" and row["channel"] == "clients_tg" and schedule_followup_on_delivered:
            msg_text = row.get("message_text")
            if msg_text:
                client_row = await conn.fetchrow(
                    "SELECT phone, COALESCE(full_name, name, 'Клиент') AS name FROM clients WHERE id=$1",
                    row["client_id"],
                )
                if client_row and client_row["phone"]:
                    await schedule_followup_on_delivered(
                        row["client_id"],
                        client_row["phone"],
                        client_row["name"],
                        msg_text,
                    )

    return True


__all__ = [
    "NotificationOutboxEntry",
    "ensure_notification_schema",
    "enqueue_notification",
    "pick_ready_batch",
    "mark_outbox_sent",
    "mark_outbox_failure",
    "cancel_outbox_entry",
    "render_template",
    "extract_provider_message_id",
    "apply_provider_status_update",
]
