"""
Message routing rules for Wahelp channels.

Priority:
- Default to Telegram (clients_tg).
- On failure, send via WhatsApp (clients_wa).
- If Telegram succeeds, schedule WhatsApp follow-up after 24h (can be cancelled if not needed).
- When delivery succeeds on a channel, mark it as preferred for future messages.
"""

from __future__ import annotations

import os
import asyncio
import logging
from dataclasses import dataclass
from typing import Mapping, Sequence

import asyncpg

from .wahelp_client import WahelpAPIError
from .wahelp_service import ChannelKind, send_text_to_phone

logger = logging.getLogger(__name__)

TELEGRAM_CHANNEL: ChannelKind = "clients_tg"
WHATSAPP_CHANNEL: ChannelKind = "clients_wa"

FOLLOWUP_DELAY_SECONDS = 24 * 60 * 60  # 24h
WA_FOLLOWUP_DISABLED = os.getenv("WA_FOLLOWUP_DISABLED", "").lower() in {"1", "true", "yes", "on"}
_followup_tasks: dict[int, asyncio.Task] = {}


@dataclass
class ClientContact:
    client_id: int
    phone: str
    name: str
    preferred_channel: ChannelKind | None = None


@dataclass(slots=True)
class SendResult:
    channel: ChannelKind
    response: Mapping[str, object] | None = None


async def send_with_rules(
    conn: asyncpg.Connection,
    contact: ClientContact,
    *,
    text: str,
) -> SendResult:
    channels = _build_channel_sequence(contact.preferred_channel)
    last_error: Exception | None = None
    for channel in channels:
        try:
            response = await send_text_to_phone(
                channel,
                phone=contact.phone,
                name=contact.name,
                text=text,
            )
            await _set_preferred_channel(conn, contact.client_id, channel)
            _cancel_followup(contact.client_id)
            logger.info("Message sent via %s to client %s", channel, contact.client_id)
            return SendResult(channel=channel, response=response if isinstance(response, Mapping) else None)
        except WahelpAPIError as exc:
            logger.warning("Send via %s failed for client %s: %s", channel, contact.client_id, exc)
            last_error = exc
            continue
    if last_error:
        raise last_error
    raise RuntimeError("All Wahelp channels failed without exception")


def _build_channel_sequence(preferred: ChannelKind | None) -> Sequence[ChannelKind]:
    if preferred == WHATSAPP_CHANNEL:
        return (WHATSAPP_CHANNEL, TELEGRAM_CHANNEL)
    if preferred == TELEGRAM_CHANNEL:
        return (TELEGRAM_CHANNEL, WHATSAPP_CHANNEL)
    # default priority: Telegram first
    return (TELEGRAM_CHANNEL, WHATSAPP_CHANNEL)


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


def _schedule_followup(contact: ClientContact, text: str) -> None:
    _cancel_followup(contact.client_id)


def _cancel_followup(client_id: int) -> None:
    task = _followup_tasks.pop(client_id, None)
    if task:
        task.cancel()


def cancel_followup_for_client(client_id: int) -> None:
    """Expose follow-up cancellation for external consumers (e.g. webhook)."""
    _cancel_followup(client_id)


async def schedule_followup_for_client(
    client_id: int,
    *,
    phone: str,
    name: str,
    text: str,
) -> None:
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
