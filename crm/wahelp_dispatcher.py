"""
Message routing rules for Wahelp channels.

Updated priority:
- If tg_username is known, try Telegram via username first and remember it as preferred on success.
- Without tg_username, try WhatsApp by phone first, fallback to Telegram by phone.
- Leads always use the Telegram leads channel (username first when available).
- Any "messenger not connected" error flips preference to the fallback channel for future sends.
"""

from __future__ import annotations

import os
import asyncio
import logging
from dataclasses import dataclass
from typing import Literal, Mapping, Sequence

import asyncpg

from .wahelp_client import WahelpAPIError
from .wahelp_service import ChannelKind, send_text_to_phone

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
    tg_username: str | None = None
    preferred_channel: ChannelKind | None = None
    recipient_kind: str = "client"


@dataclass(slots=True)
class SendResult:
    channel: ChannelKind
    response: Mapping[str, object] | None = None


@dataclass(slots=True)
class ChannelAttempt:
    channel: ChannelKind
    address_kind: Literal["phone", "username"] = "phone"


async def send_with_rules(
    conn: asyncpg.Connection,
    contact: ClientContact,
    *,
    text: str,
) -> SendResult:
    attempts = _build_channel_sequence(contact)
    last_error: Exception | None = None
    for attempt in attempts:
        channel = attempt.channel
        try:
            response = await send_text_to_phone(
                channel,
                phone=contact.phone,
                name=contact.name,
                text=text,
                tg_username=contact.tg_username,
                use_username=attempt.address_kind == "username",
            )
            if contact.recipient_kind == "client":
                await _set_preferred_channel(conn, contact.client_id, channel)
                _cancel_followup(contact.client_id)
            logger.info("Message sent via %s to client %s", channel, contact.client_id)
            return SendResult(channel=channel, response=response if isinstance(response, Mapping) else None)
        except WahelpAPIError as exc:
            if _is_messenger_missing_error(exc):
                await _handle_messenger_missing(conn, contact, channel)
            logger.warning("Send via %s failed for client %s: %s", channel, contact.client_id, exc)
            last_error = exc
            continue
    if last_error:
        raise last_error
    raise RuntimeError("All Wahelp channels failed without exception")


def _build_channel_sequence(contact: ClientContact) -> Sequence[ChannelAttempt]:
    if contact.recipient_kind == "lead":
        return _build_lead_sequence(contact)
    attempts: list[ChannelAttempt] = []
    if contact.tg_username:
        attempts.append(ChannelAttempt(channel=TELEGRAM_CHANNEL, address_kind="username"))
    attempts.extend(_build_client_phone_attempts(contact))
    return tuple(_deduplicate_attempts(attempts))


def _build_lead_sequence(contact: ClientContact) -> Sequence[ChannelAttempt]:
    attempts: list[ChannelAttempt] = []
    if contact.tg_username:
        attempts.append(ChannelAttempt(channel=LEADS_CHANNEL, address_kind="username"))
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
    return "Мессенджер не подключен" in text or "messenger" in text.lower() and "not connected" in text.lower()


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
