"""
Message routing rules for Wahelp channels.

Current priority (MAX is disabled):
- If we already know Wahelp user_id for the channel, send directly via that user.
- For clients, WhatsApp (clients_wa) has priority; Telegram (clients_tg) is fallback.
- For leads, only the Telegram leads channel is available.
- If neither channel returns a user_id (messenger not connected), mark the contact as requiring bot connection.
"""

from __future__ import annotations

import os
import re
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Awaitable, Callable, Literal, Mapping, Sequence
from zoneinfo import ZoneInfo

import asyncpg
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramAPIError

from .wahelp_client import WahelpAPIError
from .wahelp_service import ChannelKind, ensure_user_in_channel, send_text_message

logger = logging.getLogger(__name__)

TELEGRAM_CHANNEL: ChannelKind = "clients_tg"
WHATSAPP_CHANNEL: ChannelKind = "clients_wa"
MAX_CHANNEL: ChannelKind = "clients_max"
LEADS_CHANNEL: ChannelKind = "leads"

FOLLOWUP_DELAY_SECONDS = 24 * 60 * 60  # 24h
WA_FOLLOWUP_DISABLED = os.getenv("WA_FOLLOWUP_DISABLED", "").lower() in {"1", "true", "yes", "on"}
_followup_tasks: dict[int, asyncio.Task] = {}

CLIENT_BOT_TOKEN = os.getenv("CLIENT_BOT_TOKEN")
BOT_TOKEN = os.getenv("BOT_TOKEN")
WA_TG_FALLBACK_TEXT = (os.getenv("WA_TG_FALLBACK_TEXT") or "").strip()
TG_LINK = (os.getenv("TEXTS_TG_LINK") or "").strip()
DAILY_SEND_LIMIT = int(os.getenv("DAILY_SEND_LIMIT", "60") or "60")
DAILY_SEND_LIMIT_ALERT_JOB = "send_daily_limit_alert"
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
ADMIN_TG_IDS: set[int] = set()
_admin_ids_env = os.getenv("ADMIN_TG_IDS", "") or os.getenv("ADMIN_IDS", "")
for part in re.split(r"[ ,;]+", _admin_ids_env.strip()):
    if part.isdigit():
        ADMIN_TG_IDS.add(int(part))
SERVICE_WA_FOOTER_KEYS = {
    "order_completed_summary",
    "order_rating_reminder",
    "order_rating_response_high_client",
    "order_rating_response_mid_client",
    "order_rating_response_low_client",
}


@dataclass
class ClientContact:
    client_id: int
    phone: str
    name: str
    preferred_channel: ChannelKind | None = None
    recipient_kind: str = "client"
    wa_user_id: int | None = None
    tg_user_id: int | None = None
    max_user_id: int | None = None
    lead_user_id: int | None = None
    requires_connection: bool = False
    bot_tg_user_id: int | None = None
    bot_started: bool = False
    preferred_contact: str | None = None


@dataclass(slots=True)
class SendResult:
    channel: ChannelKind
    response: Mapping[str, object] | None = None


class DailySendLimitReached(RuntimeError):
    def __init__(self, limit: int, total: int) -> None:
        super().__init__(f"Daily send limit reached: {total}/{limit}")
        self.limit = limit
        self.total = total


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


def _today_window_utc() -> tuple[datetime, datetime]:
    now_local = datetime.now(MOSCOW_TZ)
    start_local = datetime.combine(now_local.date(), time.min, tzinfo=MOSCOW_TZ)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


async def _count_daily_sends(conn: asyncpg.Connection) -> int:
    start_utc, end_utc = _today_window_utc()
    client_total = await conn.fetchval(
        """
        SELECT COUNT(*)
        FROM notification_messages
        WHERE sent_at >= $1
          AND sent_at < $2
        """,
        start_utc,
        end_utc,
    ) or 0
    lead_total = await conn.fetchval(
        """
        SELECT COUNT(*)
        FROM lead_logs
        WHERE sent_at >= $1
          AND sent_at < $2
        """,
        start_utc,
        end_utc,
    ) or 0
    return int(client_total) + int(lead_total)


async def _should_send_limit_alert(conn: asyncpg.Connection, now_utc: datetime) -> bool:
    row = await conn.fetchrow(
        "SELECT last_run FROM daily_job_runs WHERE job_name=$1",
        DAILY_SEND_LIMIT_ALERT_JOB,
    )
    if row and row["last_run"]:
        last_local = row["last_run"].astimezone(MOSCOW_TZ)
        if last_local.date() >= now_utc.astimezone(MOSCOW_TZ).date():
            return False
    return True


async def _mark_limit_alert(conn: asyncpg.Connection, now_utc: datetime) -> None:
    await conn.execute(
        """
        INSERT INTO daily_job_runs (job_name, last_run)
        VALUES ($1, $2)
        ON CONFLICT (job_name)
        DO UPDATE SET last_run = EXCLUDED.last_run
        """,
        DAILY_SEND_LIMIT_ALERT_JOB,
        now_utc,
    )


async def _send_limit_alert(conn: asyncpg.Connection, total: int) -> None:
    if not BOT_TOKEN or not ADMIN_TG_IDS:
        return
    now_utc = datetime.now(timezone.utc)
    if not await _should_send_limit_alert(conn, now_utc):
        return
    text = (
        "🚫 Достигнут дневной лимит отправок.\n"
        f"Всего: {total} из {DAILY_SEND_LIMIT}\n"
        "Отправка остановлена до завтра."
    )
    bot = Bot(token=BOT_TOKEN)
    try:
        for admin_id in ADMIN_TG_IDS:
            try:
                await bot.send_message(admin_id, text)
            except Exception:
                logger.exception("Failed to send daily limit alert to admin %s", admin_id)
    finally:
        await bot.session.close()
    await _mark_limit_alert(conn, now_utc)


async def _enforce_daily_limit(conn: asyncpg.Connection) -> None:
    if DAILY_SEND_LIMIT <= 0:
        return
    total = await _count_daily_sends(conn)
    if total >= DAILY_SEND_LIMIT:
        await _send_limit_alert(conn, total)
        raise DailySendLimitReached(DAILY_SEND_LIMIT, total)


async def send_to_client_bot(
    bot_tg_user_id: int,
    text: str,
    *,
    logs_chat_id: int | None = None,
) -> tuple[bool, str | None]:
    """
    Отправляет сообщение клиенту через наш клиентский бот напрямую.
    
    Returns:
        (success: bool, error_message: str | None)
    """
    if not CLIENT_BOT_TOKEN:
        return False, "CLIENT_BOT_TOKEN not configured"
    
    bot = Bot(token=CLIENT_BOT_TOKEN)
    try:
        await bot.send_message(bot_tg_user_id, text)
        return True, None
    except TelegramForbiddenError:
        # Клиент заблокировал бота
        error_msg = "Client blocked the bot"
        if logs_chat_id:
            try:
                await bot.send_message(
                    logs_chat_id,
                    f"❌ Ошибка доставки в бот\n"
                    f"TG ID: {bot_tg_user_id}\n"
                    f"Причина: Клиент заблокировал бота"
                )
            except Exception:
                pass
        return False, error_msg
    except TelegramBadRequest as e:
        # Неверный chat_id или другая ошибка запроса
        error_msg = f"Telegram API error: {e.message}"
        if logs_chat_id:
            try:
                await bot.send_message(
                    logs_chat_id,
                    f"❌ Ошибка доставки в бот\n"
                    f"TG ID: {bot_tg_user_id}\n"
                    f"Причина: {e.message}"
                )
            except Exception:
                pass
        return False, error_msg
    except TelegramAPIError as e:
        # Другие ошибки Telegram API
        error_msg = f"Telegram API error: {str(e)}"
        if logs_chat_id:
            try:
                await bot.send_message(
                    logs_chat_id,
                    f"❌ Ошибка доставки в бот\n"
                    f"TG ID: {bot_tg_user_id}\n"
                    f"Причина: {str(e)}"
                )
            except Exception:
                pass
        return False, error_msg
    except Exception as e:
        # Неожиданные ошибки
        error_msg = f"Unexpected error: {str(e)}"
        if logs_chat_id:
            try:
                await bot.send_message(
                    logs_chat_id,
                    f"❌ Ошибка доставки в бот\n"
                    f"TG ID: {bot_tg_user_id}\n"
                    f"Причина: {str(e)}"
                )
            except Exception:
                pass
        return False, error_msg
    finally:
        await bot.session.close()


async def send_with_rules(
    conn: asyncpg.Connection,
    contact: ClientContact,
    *,
    text: str,
    event_key: str | None = None,
    logs_chat_id: int | None = None,
) -> SendResult:
    if contact.requires_connection:
        raise WahelpAPIError(0, "Contact requires Wahelp connection", None)
    await _enforce_daily_limit(conn)
    
    # Клиентский бот временно отключён для исходящих сервисных сообщений.

    # Продолжаем со старой логикой через Wahelp
    dead_channels = await _get_dead_channels(conn, contact)
    attempts = _build_channel_sequence(contact, dead_channels)
    last_error: Exception | None = None
    last_channel: ChannelKind | None = None
    failed_channels: list[ChannelKind] = []
    for attempt in attempts:
        channel = attempt.channel
        last_channel = channel
        try:
            send_text = _with_wa_fallback(text, channel, event_key)
            if attempt.address_kind == "user_id" and attempt.user_id is not None:
                response = await send_text_message(channel, user_id=attempt.user_id, text=send_text)
            else:
                user_id = await _ensure_user_id(conn, contact, channel)
                await _persist_user_id(conn, contact, channel, user_id)
                response = await send_text_message(channel, user_id=user_id, text=send_text)
            if contact.recipient_kind == "client":
                await _set_preferred_channel(conn, contact.client_id, channel)
                _cancel_followup(contact.client_id)
                await _resolve_dead_channel(conn, contact.client_id, channel)
            logger.info("Message sent via %s to client %s", channel, contact.client_id)
            return SendResult(channel=channel, response=response if isinstance(response, Mapping) else None)
        except WahelpAPIError as exc:
            if _is_not_connected_error(exc):
                await _handle_messenger_missing(conn, contact, channel)
                await _mark_channel_dead(conn, contact, channel, str(exc))
            elif _is_rate_limit_error(exc):
                logger.warning("Rate limit for %s %s: %s", contact.recipient_kind, contact.client_id, exc)
            failed_channels.append(channel)
            logger.warning("Send via %s failed for %s %s: %s", channel, contact.recipient_kind, contact.client_id, exc)
            last_error = exc
            continue
    # Детальный лог отключён — достаточно сводок и флага requires_connection.
    if last_error:
        if isinstance(last_error, WahelpAPIError) and _is_not_connected_error(last_error):
            await _mark_requires_connection(conn, contact, str(last_error), last_channel or MAX_CHANNEL)
        raise last_error
    raise RuntimeError("All Wahelp channels failed without exception")


async def send_via_channel(
    conn: asyncpg.Connection,
    contact: ClientContact,
    channel: ChannelKind,
    *,
    text: str,
    event_key: str | None = None,
) -> SendResult:
    if contact.requires_connection:
        raise WahelpAPIError(0, "Contact requires Wahelp connection", None)
    await _enforce_daily_limit(conn)
    dead_channels = await _get_dead_channels(conn, contact)
    if channel in dead_channels:
        raise WahelpAPIError(0, f"Channel {channel} is marked as dead", None)
    send_text = _with_wa_fallback(text, channel, event_key)
    try:
        user_id = None
        if channel == WHATSAPP_CHANNEL:
            user_id = contact.wa_user_id
        elif channel == TELEGRAM_CHANNEL:
            user_id = contact.tg_user_id
        elif channel == MAX_CHANNEL:
            user_id = contact.max_user_id
        if user_id:
            response = await send_text_message(channel, user_id=user_id, text=send_text)
        else:
            user_id = await _ensure_user_id(conn, contact, channel)
            await _persist_user_id(conn, contact, channel, user_id)
            response = await send_text_message(channel, user_id=user_id, text=send_text)
        if contact.recipient_kind == "client":
            await _set_preferred_channel(conn, contact.client_id, channel)
            _cancel_followup(contact.client_id)
            await _resolve_dead_channel(conn, contact.client_id, channel)
        return SendResult(channel=channel, response=response if isinstance(response, Mapping) else None)
    except WahelpAPIError as exc:
        if _is_not_connected_error(exc):
            await _handle_messenger_missing(conn, contact, channel)
            await _mark_channel_dead(conn, contact, channel, str(exc))
        elif _is_rate_limit_error(exc):
            logger.warning("Rate limit for %s %s: %s", contact.recipient_kind, contact.client_id, exc)
        raise


def _build_channel_sequence(contact: ClientContact, dead_channels: set[ChannelKind]) -> Sequence[ChannelAttempt]:
    if contact.recipient_kind == "lead":
        return _build_lead_sequence(contact)
    attempts: list[ChannelAttempt] = []
    preferred = contact.preferred_channel
    if preferred == TELEGRAM_CHANNEL and contact.tg_user_id and TELEGRAM_CHANNEL not in dead_channels:
        attempts.append(ChannelAttempt(channel=TELEGRAM_CHANNEL, address_kind="user_id", user_id=contact.tg_user_id))
    if preferred == WHATSAPP_CHANNEL and contact.wa_user_id and WHATSAPP_CHANNEL not in dead_channels:
        attempts.append(ChannelAttempt(channel=WHATSAPP_CHANNEL, address_kind="user_id", user_id=contact.wa_user_id))
    if contact.wa_user_id and (preferred != WHATSAPP_CHANNEL) and WHATSAPP_CHANNEL not in dead_channels:
        attempts.append(ChannelAttempt(channel=WHATSAPP_CHANNEL, address_kind="user_id", user_id=contact.wa_user_id))
    if contact.tg_user_id and (preferred != TELEGRAM_CHANNEL) and TELEGRAM_CHANNEL not in dead_channels:
        attempts.append(ChannelAttempt(channel=TELEGRAM_CHANNEL, address_kind="user_id", user_id=contact.tg_user_id))
    attempts.extend(_build_client_phone_attempts(contact, dead_channels))
    return tuple(_deduplicate_attempts(attempts))


def _build_lead_sequence(contact: ClientContact) -> Sequence[ChannelAttempt]:
    attempts: list[ChannelAttempt] = []
    if contact.lead_user_id:
        attempts.append(ChannelAttempt(channel=LEADS_CHANNEL, address_kind="user_id", user_id=contact.lead_user_id))
    attempts.append(ChannelAttempt(channel=LEADS_CHANNEL, address_kind="phone"))
    return tuple(_deduplicate_attempts(attempts))


def _build_client_phone_attempts(contact: ClientContact, dead_channels: set[ChannelKind]) -> list[ChannelAttempt]:
    """
    Строит последовательность попыток отправки по телефону.
    Приоритет: WA -> TG (MAX отключён)
    """
    order: Sequence[ChannelKind]
    preferred = contact.preferred_channel
    if preferred == TELEGRAM_CHANNEL:
        order = (TELEGRAM_CHANNEL, WHATSAPP_CHANNEL)
    elif preferred == WHATSAPP_CHANNEL:
        order = (WHATSAPP_CHANNEL, TELEGRAM_CHANNEL)
    else:
        # По умолчанию: WA -> TG
        order = (WHATSAPP_CHANNEL, TELEGRAM_CHANNEL)
    return [
        ChannelAttempt(channel=chan, address_kind="phone")
        for chan in order
        if chan not in dead_channels
    ]


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


def _is_not_connected_error(error: WahelpAPIError) -> bool:
    text = str(error)
    lowered = text.lower()
    return (
        "мессенджер не подключен" in text
        or ("messenger" in lowered and "not connected" in lowered)
    )


def _is_rate_limit_error(error: WahelpAPIError) -> bool:
    lowered = str(error).lower()
    return "слишком много попыток" in lowered or "too many requests" in lowered


def _with_wa_fallback(text: str, channel: ChannelKind, event_key: str | None) -> str:
    if channel != WHATSAPP_CHANNEL:
        return text
    if event_key not in SERVICE_WA_FOOTER_KEYS:
        return text
    if WA_TG_FALLBACK_TEXT:
        return f"{text}\n\n{WA_TG_FALLBACK_TEXT}"
    if TG_LINK:
        return f"{text}\n\nПроблемы с whatsapp? Подпишитесь на наш телеграм {TG_LINK}"
    return f"{text}\n\nПроблемы с whatsapp? Подпишитесь на наш телеграм"


async def _get_dead_channels(conn: asyncpg.Connection, contact: ClientContact) -> set[ChannelKind]:
    if contact.recipient_kind != "client":
        return set()
    rows = await conn.fetch(
        """
        SELECT channel
        FROM wahelp_delivery_issues
        WHERE entity_kind = 'client'
          AND entity_id = $1
          AND resolved_at IS NULL
          AND reason ILIKE '%Мессенджер не подключен%'
        """,
        contact.client_id,
    )
    return {row["channel"] for row in rows if row.get("channel")}


async def _mark_channel_dead(
    conn: asyncpg.Connection,
    contact: ClientContact,
    channel: ChannelKind,
    reason: str,
) -> None:
    if contact.recipient_kind != "client":
        return
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


async def _resolve_dead_channel(conn: asyncpg.Connection, client_id: int, channel: ChannelKind) -> None:
    await conn.execute(
        """
        UPDATE wahelp_delivery_issues
        SET resolved_at = NOW()
        WHERE entity_kind='client'
          AND entity_id=$1
          AND channel=$2
          AND resolved_at IS NULL
        """,
        client_id,
        channel,
    )


async def _handle_messenger_missing(
    conn: asyncpg.Connection,
    contact: ClientContact,
    failed_channel: ChannelKind,
) -> None:
    if contact.recipient_kind != "client":
        return
    # Выбираем следующий канал по приоритету: WA -> TG
    if failed_channel == WHATSAPP_CHANNEL:
        fallback = TELEGRAM_CHANNEL
    elif failed_channel == TELEGRAM_CHANNEL:
        fallback = WHATSAPP_CHANNEL
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
    # Определяем колонку для сохранения user_id
    if channel == WHATSAPP_CHANNEL:
        column = "wahelp_user_id_wa"
    elif channel == TELEGRAM_CHANNEL:
        column = "wahelp_user_id_tg"
    elif channel == MAX_CHANNEL:
        column = "wahelp_user_id_max"
    else:
        raise ValueError(f"Unknown channel for clients: {channel}")
    
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
    elif channel == TELEGRAM_CHANNEL:
        contact.tg_user_id = user_id
    elif channel == MAX_CHANNEL:
        contact.max_user_id = user_id
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
