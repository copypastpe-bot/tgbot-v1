"""Async worker that delivers notification outbox items via Wahelp."""

from __future__ import annotations

import asyncio
import logging
from typing import Mapping

import asyncpg

from crm import ClientContact, send_with_rules
from .outbox import (
    NotificationOutboxEntry,
    cancel_outbox_entry,
    extract_provider_message_id,
    mark_outbox_failure,
    mark_outbox_sent,
    pick_ready_batch,
    render_template,
)
from .rules import NotificationRules
from bot import get_promo_texts, get_birthday_texts, TEXTS_TG_LINK

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
    ) -> None:
        self.pool = pool
        self.rules = rules
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self.max_attempts = max_attempts
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

        contact = ClientContact(
            client_id=entry.client_id,
            phone=entry.client_phone,
            name=(entry.client_name or "Клиент"),
            preferred_channel=entry.client_preferred_channel,
            wa_user_id=entry.client_user_id_wa,
            tg_user_id=entry.client_user_id_tg,
            requires_connection=entry.client_requires_connection,
            recipient_kind=entry.recipient_kind,
        )
        message_text = self._build_message_text(entry)

        async with self.pool.acquire() as conn:
            try:
                result = await send_with_rules(conn, contact, text=message_text)
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
            await mark_outbox_sent(
                conn,
                entry,
                channel=result.channel,
                message_text=message_text,
                provider_payload=provider_payload,
                provider_message_id=provider_message_id,
            )

    def _build_message_text(self, entry: NotificationOutboxEntry) -> str:
        template = entry.template
        payload = dict(entry.payload)
        ek = entry.event_key or ""
        # try sheets for promo/birthday
        try:
            if ek.startswith("promo_reengage"):
                texts = get_promo_texts()
                if texts:
                    idx = (entry.id - 1) % len(texts)
                    template = texts[idx]
                    template = (
                        template
                        .replace("{BONUS_SUM}", "{{bonus}}")
                        .replace("{BONUS_EXPIRES_AT}", "{{expire_date}}")
                        .replace("{TG_LINK}", TEXTS_TG_LINK or "")
                    )
            elif ek.startswith("birthday_congrats"):
                texts = get_birthday_texts()
                if texts:
                    idx = (entry.id - 1) % len(texts)
                    template = texts[idx]
                    template = (
                        template
                        .replace("{BONUS_SUM}", "{{bonus_balance}}")
                        .replace("{BONUS_EXPIRES_AT}", "{{expire_date}}")
                        .replace("{TG_LINK}", TEXTS_TG_LINK or "")
                    )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Sheet text fallback for %s failed: %s", ek, exc)
        return render_template(template, payload)


__all__ = ["NotificationWorker"]
