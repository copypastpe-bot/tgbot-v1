"""Async worker that delivers notification outbox items via Wahelp."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, Mapping

import asyncpg

from crm import ClientContact, DailySendLimitReached, send_with_rules
from .outbox import (
    PRE_SEND_OK,
    NotificationOutboxEntry,
    PreSendVerdict,
    cancel_outbox_entry,
    extract_provider_message_id,
    mark_outbox_failure,
    mark_outbox_sent,
    pick_ready_batch,
    render_template,
)
from .rules import NotificationRules

logger = logging.getLogger(__name__)


class NotificationWorker:
    def __init__(
        self,
        pool: asyncpg.Pool,
        rules: NotificationRules,
        *,
        poll_interval: float = 5.0,
        batch_size: int = 10,
        max_attempts: int = 5,
        promo_texts_fn=None,
        birthday_texts_fn=None,
        logs_chat_id: int | None = None,
        before_send: Callable[[NotificationOutboxEntry],
                              Awaitable[PreSendVerdict]] | None = None,
        after_send: Callable[[asyncpg.Connection, NotificationOutboxEntry],
                             Awaitable[None]] | None = None,
        precheck_retry_minutes: int = 15,
    ) -> None:
        """Работник очереди — курьер: он умеет доставлять письма и больше ничего.

        `before_send` и `after_send` — две точки подключения для тех, кто знает
        про письмо больше курьера. Их заполняет `bot.py`: сам работник ничего
        не знает ни про amoCRM, ни про ожидания подтверждения, и знать не должен.

        * `before_send` спрашивает «можно ли ещё слать?» — так письмо-вопрос
          не уходит по заказу, которого в CRM уже нет.
        * `after_send` получает ту же связь с базой, что и отметка «отправлено»,
          и пишет в неё факт отправки одной транзакцией с ней.
        """
        self.pool = pool
        self.rules = rules
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self.max_attempts = max_attempts
        self.promo_texts_fn = promo_texts_fn
        self.birthday_texts_fn = birthday_texts_fn
        self.logs_chat_id = logs_chat_id
        self.before_send = before_send
        self.after_send = after_send
        self.precheck_retry_minutes = precheck_retry_minutes
        self._task: asyncio.Task | None = None
        self._stopping = False

    def start(self) -> asyncio.Task:
        if self._task is None:
            self._task = asyncio.create_task(self.run(), name="notification-worker")
        return self._task

    async def stop(self) -> None:
        self._stopping = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:  # noqa: PERF203 - expected flow
                pass

    async def run(self) -> None:
        logger.info("Notification worker started")
        try:
            while not self._stopping:
                processed = False
                try:
                    processed = await self._process_once()
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001
                    logger.exception("Notification worker iteration failed")
                sleep_for = 0 if processed else self.poll_interval
                await asyncio.sleep(sleep_for)
        except asyncio.CancelledError:
            logger.info("Notification worker cancelled")
            raise
        finally:
            logger.info("Notification worker stopped")

    async def _process_once(self) -> bool:
        async with self.pool.acquire() as conn:
            entries = await pick_ready_batch(conn, self.batch_size)
        if not entries:
            return False
        for entry in entries:
            await self._handle_entry(entry)
        return True

    async def _handle_entry(self, entry: NotificationOutboxEntry) -> None:
        if entry.client_id is None:
            logger.warning("Notification %s has no client_id", entry.id)
            async with self.pool.acquire() as conn:
                await cancel_outbox_entry(conn, entry, "missing client")
            return
        if not entry.notifications_enabled:
            async with self.pool.acquire() as conn:
                await cancel_outbox_entry(conn, entry, "notifications disabled")
            return
        if entry.client_requires_connection:
            async with self.pool.acquire() as conn:
                await cancel_outbox_entry(conn, entry, "wahelp requires connection")
            return
        if not entry.client_phone:
            async with self.pool.acquire() as conn:
                await cancel_outbox_entry(conn, entry, "client phone missing")
            return

        # Последнее слово перед отправкой — за тем, кто знает смысл письма.
        # Дешёвые проверки выше уже отсеяли лишнее: незачем ходить в CRM ради
        # письма, которое и так отменится.
        if self.before_send is not None:
            verdict = await self.before_send(entry)
            if verdict.action == "cancel":
                logger.info("Письмо %s отменено перед отправкой: %s",
                            entry.id, verdict.reason)
                async with self.pool.acquire() as conn:
                    await cancel_outbox_entry(conn, entry, verdict.reason)
                return
            if verdict.action == "retry":
                # Проверить не удалось. Молча отменить письмо здесь значило бы
                # наказать живого клиента за чужую сетевую заминку. Пауза длиннее
                # обычной: попыток всего пять, и тратить их за полчаса на одну
                # и ту же недоступную CRM бессмысленно.
                logger.warning("Письмо %s отложено: %s", entry.id, verdict.reason)
                async with self.pool.acquire() as conn:
                    await mark_outbox_failure(
                        conn,
                        entry,
                        error_message=verdict.reason,
                        attempts=entry.attempts,
                        max_attempts=self.max_attempts,
                        retry_delay_minutes=self.precheck_retry_minutes,
                    )
                return

        contact = ClientContact(
            client_id=entry.client_id,
            phone=entry.client_phone,
            name=(entry.client_name or "Клиент"),
            preferred_channel=entry.client_preferred_channel,
            wa_user_id=entry.client_user_id_wa,
            tg_user_id=entry.client_user_id_tg,
            max_user_id=entry.client_user_id_max,
            requires_connection=entry.client_requires_connection,
            recipient_kind=entry.recipient_kind,
            bot_tg_user_id=entry.bot_tg_user_id,
            bot_started=entry.bot_started,
            preferred_contact=entry.preferred_contact,
        )
        message_text = self._build_message_text(entry)

        async with self.pool.acquire() as conn:
            try:
                result = await send_with_rules(
                    conn,
                    contact,
                    text=message_text,
                    event_key=entry.event_key,
                    logs_chat_id=self.logs_chat_id,
                )
            except DailySendLimitReached as exc:
                next_attempt = datetime.now(timezone.utc) + timedelta(days=1)
                await conn.execute(
                    """
                    UPDATE notification_outbox
                    SET status='pending',
                        scheduled_at=$2,
                        last_error=$3,
                        updated_at=NOW()
                    WHERE id=$1
                    """,
                    entry.id,
                    next_attempt,
                    str(exc)[:500],
                )
                logger.warning(
                    "Daily limit reached; rescheduled notification %s to %s",
                    entry.id,
                    next_attempt.isoformat(),
                )
                return
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Notification send failed (id=%s, client=%s): %s",
                    entry.id,
                    entry.client_id,
                    exc,
                )
                await mark_outbox_failure(
                    conn,
                    entry,
                    error_message=str(exc),
                    attempts=entry.attempts,
                    max_attempts=self.max_attempts,
                )
                return

            provider_payload: Mapping[str, object] | None = (
                result.response if isinstance(result.response, Mapping) else None
            )
            provider_message_id = extract_provider_message_id(provider_payload)
            # Одной транзакцией: разойдись отметка «отправлено» и то, что пишет
            # `after_send`, — робот получил бы отправленный вопрос, про который
            # не помнит, что задавал его, и ответ клиента пропал бы впустую.
            async with conn.transaction():
                await mark_outbox_sent(
                    conn,
                    entry,
                    channel=result.channel,
                    message_text=message_text,
                    provider_payload=provider_payload,
                    provider_message_id=provider_message_id,
                )
                if self.after_send is not None:
                    await self.after_send(conn, entry)

    def _build_message_text(self, entry: NotificationOutboxEntry) -> str:
        template = entry.template
        payload = dict(entry.payload)
        ek = entry.event_key or ""
        # try sheets for promo/birthday
        try:
            if ek.startswith("promo_reengage") and callable(self.promo_texts_fn):
                texts = self.promo_texts_fn() or []
                if texts:
                    idx = (entry.id - 1) % len(texts)
                    template = texts[idx]
                    template = (
                        template
                        .replace("{BONUS_SUM}", "{{bonus}}")
                        .replace("{BONUS_EXPIRES_AT}", "{{expire_date}}")
                    )
            elif ek.startswith("birthday_congrats") and callable(self.birthday_texts_fn):
                texts = self.birthday_texts_fn() or []
                if texts:
                    idx = (entry.id - 1) % len(texts)
                    template = texts[idx]
                    template = (
                        template
                        .replace("{BONUS_SUM}", "{{bonus_balance}}")
                        .replace("{BONUS_EXPIRES_AT}", "{{expire_date}}")
                    )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Sheet text fallback for %s failed: %s", ek, exc)
        return render_template(template, payload)


__all__ = ["NotificationWorker"]
