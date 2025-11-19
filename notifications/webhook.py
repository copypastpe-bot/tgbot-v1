"""Minimal aiohttp server to accept Wahelp webhook callbacks."""

from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Mapping

import asyncpg
from aiohttp import web

from crm import cancel_followup_for_client, schedule_followup_for_client
from .outbox import apply_provider_status_update

logger = logging.getLogger(__name__)


class WahelpWebhookServer:
    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        token: str | None = None,
        inbound_handler: Callable[[Mapping[str, Any]], Awaitable[bool]] | None = None,
    ) -> None:
        self.pool = pool
        self.token = token
        self.inbound_handler = inbound_handler
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None

    def create_app(self) -> web.Application:
        app = web.Application()
        app.router.add_post("/wahelp/webhook", self._handle)
        app.router.add_post("/wahelp/webhook/", self._handle)
        return app

    async def start(self, host: str, port: int) -> None:
        if self._runner:
            return
        app = self.create_app()
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, host, port)
        await self._site.start()
        logger.info("Wahelp webhook server listening on %s:%s", host, port)

    async def stop(self) -> None:
        if self._site:
            await self._site.stop()
            self._site = None
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
        logger.info("Wahelp webhook server stopped")

    async def _handle(self, request: web.Request) -> web.Response:
        if self.token:
            provided = request.headers.get("X-Wahelp-Token") or request.rel_url.query.get("token")
            if provided != self.token:
                return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

        try:
            payload: Any = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid_json"}, status=400)

        logger.info("Wahelp webhook payload: %s", payload)

        normalized_payload = _normalize_payload(payload)

        custom_handled = False
        if self.inbound_handler is not None:
            try:
                custom_handled = await self.inbound_handler(normalized_payload)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Inbound handler failed: %s", exc)

        handled = await apply_provider_status_update(
            self.pool,
            normalized_payload,
            cancel_followup=cancel_followup_for_client,
            schedule_followup_on_delivered=schedule_followup_for_client,
        )
        if not handled and not custom_handled:
            logger.debug("Webhook payload ignored: %s", payload)
        return web.json_response({"ok": True, "handled": handled or custom_handled})


async def start_wahelp_webhook(
    pool: asyncpg.Pool,
    *,
    host: str,
    port: int,
    token: str | None = None,
    inbound_handler: Callable[[Mapping[str, Any]], Awaitable[bool]] | None = None,
) -> WahelpWebhookServer:
    server = WahelpWebhookServer(pool, token=token, inbound_handler=inbound_handler)
    await server.start(host, port)
    return server


def _normalize_payload(payload: Mapping[str, Any] | Any) -> Mapping[str, Any] | Any:
    if not isinstance(payload, Mapping):
        return payload
    if "data" in payload:
        return payload
    inner = payload.get("payload")
    if isinstance(inner, Mapping):
        normalized = dict(payload)
        normalized["data"] = inner
        if "event" not in normalized:
            normalized["event"] = payload.get("action")
        return normalized
    return payload


__all__ = ["WahelpWebhookServer", "start_wahelp_webhook"]
