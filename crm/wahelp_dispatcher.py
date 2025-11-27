"""
Message routing rules for Wahelp channels.

New priority:
- If we already know Wahelp user_id for the channel, send directly via that user.
- For clients, WhatsApp (clients_wa) has priority; Telegram (clients_tg) is fallback.
- For leads, only the Telegram leads channel is available.
- If neither channel returns a user_id (messenger not connected), mark the contact as requiring bot connection
  and log the issue for admins. Future notifications will be skipped automatically.
"""

from __future__ import annotations

import os
import asyncio
import logging
from dataclasses import dataclass
from typing import Awaitable, Callable, Literal, Mapping, Sequence

import asyncpg

from .wahelp_client import WahelpAPIError
from .wahelp_service import ChannelKind, ensure_user_in_channel, send_text_message

logger = logging.getLogger(__name__)

TELEGRAM_CHANNEL: ChannelKind = "clients_tg"
WHATSAPP_CHANNEL: ChannelKind = "clients_wa"
LEADS_CHANNEL: ChannelKind = "leads"

FOLLOWUP_DELAY_SECONDS = 24 * 60 * 60  # 24h
WA_FOLLOWUP_DISABLED = os.getenv("WA_FOLLOWUP_DISABLED", "").lower() in {"1", "true", "yes", "on"}
_followup_tasks: dict[int, asyncio.Task] = {}


@dataclass
class ClientContact:
    client_id: int
    phone: str
    name: str
    preferred_channel: ChannelKind | None = None
    recipient_kind: str = "client"
    wa_user_id: int | None = None
    tg_user_id: int | None = None
    lead_user_id: int | None = None
    requires_connection: bool = False


@dataclass(slots=True)
class SendResult:
    channel: ChannelKind
    response: Mapping[str, object] | None = None


@dataclass(slots=True)
class ChannelAttempt:
    channel: ChannelKind
    address_kind: Literal["user_id", "phone"] = "phone"
    user_id: int | None = None


MissingMessengerLogger = Callable[[ClientContact, ChannelKind, str], Awaitable[None]]
_missing_logger: MissingMessengerLogger | None = None


def set_missing_messenger_logger(callback: MissingMessengerLogger | None) -> None:
    """Register optional logger that reports contacts without messengers."""
    global _missing_logger
    _missing_logger = callback


async def send_with_rules(
    conn: asyncpg.Connection,
    contact: ClientContact,
    *,
    text: str,
) -> SendResult:
    if contact.requires_connection:
        raise WahelpAPIError(0, "Contact requires Wahelp connection", None)
    attempts = _build_channel_sequence(contact)
    last_error: Exception | None = None
    last_channel: ChannelKind | None = None
    for attempt in attempts:
        channel = attempt.channel
        last_channel = channel
        try:
            if attempt.address_kind == "user_id" and attempt.user_id is not None:
                response = await send_text_message(channel, user_id=attempt.user_id, text=text)
            else:
                user_id = await _ensure_user_id(conn, contact, channel)
                await _persist_user_id(conn, contact, channel, user_id)
                response = await send_text_message(channel, user_id=user_id, text=text)
            if contact.recipient_kind == "client":
                await _set_preferred_channel(conn, contact.client_id, channel)
                _cancel_followup(contact.client_id)
            logger.info("Message sent via %s to client %s", channel, contact.client_id)
            return SendResult(channel=channel, response=response if isinstance(response, Mapping) else None)
        except WahelpAPIError as exc:
            if _is_messenger_missing_error(exc):
                await _handle_messenger_missing(conn, contact, channel)
            logger.warning("Send via %s failed for %s %s: %s", channel, contact.recipient_kind, contact.client_id, exc)
            last_error = exc
            continue
    if last_error:
        if isinstance(last_error, WahelpAPIError) and _is_messenger_missing_error(last_error):
            await _mark_requires_connection(conn, contact, str(last_error), last_channel or WHATSAPP_CHANNEL)
        raise last_error
    raise RuntimeError("All Wahelp channels failed without exception")


def _build_channel_sequence(contact: ClientContact) -> Sequence[ChannelAttempt]:
    if contact.recipient_kind == "lead":
        return _build_lead_sequence(contact)
    attempts: list[ChannelAttempt] = []
    if contact.wa_user_id:
        attempts.append(ChannelAttempt(channel=WHATSAPP_CHANNEL, address_kind="user_id", user_id=contact.wa_user_id))
    if contact.tg_user_id:
        attempts.append(ChannelAttempt(channel=TELEGRAM_CHANNEL, address_kind="user_id", user_id=contact.tg_user_id))
    attempts.extend(_build_client_phone_attempts(contact))
    return tuple(_deduplicate_attempts(attempts))


def _build_lead_sequence(contact: ClientContact) -> Sequence[ChannelAttempt]:
    attempts: list[ChannelAttempt] = []
    if contact.lead_user_id:
        attempts.append(ChannelAttempt(channel=LEADS_CHANNEL, address_kind="user_id", user_id=contact.lead_user_id))
    attempts.append(ChannelAttempt(channel=LEADS_CHANNEL, address_kind="phone"))
    return tuple(_deduplicate_attempts(attempts))


def _build_client_phone_attempts(contact: ClientContact) -> list[ChannelAttempt]:
    order: Sequence[ChannelKind]
    preferred = contact.preferred_channel
    if preferred == WHATSAPP_CHANNEL:
        order = (WHATSAPP_CHANNEL, TELEGRAM_CHANNEL)
    elif preferred == TELEGRAM_CHANNEL:
        order = (TELEGRAM_CHANNEL, WHATSAPP_CHANNEL)
    else:
        order = (WHATSAPP_CHANNEL, TELEGRAM_CHANNEL)
    return [ChannelAttempt(channel=chan, address_kind="phone") for chan in order]


def _deduplicate_attempts(attempts: Sequence[ChannelAttempt]) -> list[ChannelAttempt]:
    seen: set[tuple[ChannelKind, str]] = set()
    result: list[ChannelAttempt] = []
    for attempt in attempts:
        key = (attempt.channel, attempt.address_kind)
        if key in seen:
            continue
        seen.add(key)
        result.append(attempt)
    return result


async def _set_preferred_channel(conn: asyncpg.Connection, client_id: int, channel: ChannelKind) -> None:
    await conn.execute(
        """
        UPDATE clients
        SET wahelp_preferred_channel = $1,
            last_updated = NOW()
        WHERE id = $2
        """,
        channel,
        client_id,
    )


def _cancel_followup(client_id: int) -> None:
    task = _followup_tasks.pop(client_id, None)
    if task:
        task.cancel()


def cancel_followup_for_client(client_id: int) -> None:
    """Expose follow-up cancellation for external consumers (e.g. webhook)."""
    _cancel_followup(client_id)


async def schedule_followup_for_client(
    client_id: int,
    phone: str,
    name: str,
    text: str,
) -> None:
    """Schedule WA follow-up 24h after delivery if not read."""
    if WA_FOLLOWUP_DISABLED:
        return
    _cancel_followup(client_id)

    async def _task() -> None:
        try:
            await asyncio.sleep(FOLLOWUP_DELAY_SECONDS)
            logger.info("Follow-up via WhatsApp for client %s", client_id)
            await send_text_to_phone(
                WHATSAPP_CHANNEL,
                phone=phone,
                name=name,
                text=text,
            )
        except asyncio.CancelledError:  # pragma: no cover
            logger.debug("Follow-up task for client %s cancelled", client_id)
        except Exception as exc:  # noqa: BLE001
            logger.error("Follow-up send failed for client %s: %s", client_id, exc)
        finally:
            _followup_tasks.pop(client_id, None)

    loop = asyncio.get_running_loop()
    _followup_tasks[client_id] = loop.create_task(_task())


def _is_messenger_missing_error(error: WahelpAPIError) -> bool:
    text = str(error)
    lowered = text.lower()
    return (
        "мессенджер не подключен" in text
        or ("messenger" in lowered and "not connected" in lowered)
        or "слишком много попыток" in lowered
        or "too many requests" in lowered
    )


async def _handle_messenger_missing(
    conn: asyncpg.Connection,
    contact: ClientContact,
    failed_channel: ChannelKind,
) -> None:
    if contact.recipient_kind != "client":
        return
    if failed_channel == TELEGRAM_CHANNEL:
        fallback = WHATSAPP_CHANNEL
    elif failed_channel == WHATSAPP_CHANNEL:
        fallback = TELEGRAM_CHANNEL
    else:
        return
    await _set_preferred_channel(conn, contact.client_id, fallback)
    contact.preferred_channel = fallback


async def _ensure_user_id(
    conn: asyncpg.Connection,
    contact: ClientContact,
    channel: ChannelKind,
) -> int:
    if not contact.phone:
        raise WahelpAPIError(0, "Client phone is missing", None)
    user_id = await ensure_user_in_channel(
        channel,
        phone=contact.phone,
        name=contact.name,
    )
    return user_id


async def _persist_user_id(
    conn: asyncpg.Connection,
    contact: ClientContact,
    channel: ChannelKind,
    user_id: int,
) -> None:
    if contact.recipient_kind == "lead":
        await conn.execute(
            """
            UPDATE leads
            SET wahelp_user_id_leads=$1,
                wahelp_requires_connection=false,
                last_updated = NOW()
            WHERE id=$2
            """,
            user_id,
            contact.client_id,
        )
        contact.lead_user_id = user_id
        contact.requires_connection = False
        return
    column = "wahelp_user_id_wa" if channel == WHATSAPP_CHANNEL else "wahelp_user_id_tg"
    await conn.execute(
        f"""
        UPDATE clients
        SET {column}=$1,
            wahelp_requires_connection=false,
            last_updated = NOW()
        WHERE id=$2
        """,
        user_id,
        contact.client_id,
    )
    if channel == WHATSAPP_CHANNEL:
        contact.wa_user_id = user_id
    else:
        contact.tg_user_id = user_id
    contact.requires_connection = False


async def _mark_requires_connection(
    conn: asyncpg.Connection,
    contact: ClientContact,
    reason: str,
    channel: ChannelKind,
) -> None:
    already_flagged = contact.requires_connection
    if contact.recipient_kind == "lead":
        await conn.execute(
            "UPDATE leads SET wahelp_requires_connection=true, last_updated = NOW() WHERE id=$1",
            contact.client_id,
        )
    else:
        await conn.execute(
            "UPDATE clients SET wahelp_requires_connection=true, last_updated = NOW() WHERE id=$1",
            contact.client_id,
        )
    await conn.execute(
        """
        INSERT INTO wahelp_delivery_issues(entity_kind, entity_id, channel, phone, reason)
        VALUES ($1,$2,$3,$4,$5)
        ON CONFLICT (entity_kind, entity_id, COALESCE(channel,''))
        DO UPDATE SET reason=EXCLUDED.reason, phone=EXCLUDED.phone, created_at=NOW(), resolved_at=NULL
        """,
        contact.recipient_kind,
        contact.client_id,
        channel,
        contact.phone,
        reason[:200],
    )
    contact.requires_connection = True
    if not already_flagged and _missing_logger is not None:
        try:
            await _missing_logger(contact, channel, reason)
        except Exception:  # pragma: no cover - logging best-effort
            logger.exception(
                "Failed to notify about missing messenger for %s %s",
                contact.recipient_kind,
                contact.client_id,
            )
