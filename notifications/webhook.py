"""Minimal aiohttp server to accept Wahelp webhook callbacks."""

from __future__ import annotations

import logging
from typing import Any

import asyncpg
from aiohttp import web

from crm import cancel_followup_for_client
from .outbox import apply_provider_status_update

logger = logging.getLogger(__name__)


class WahelpWebhookServer:
    def __init__(self, pool: asyncpg.Pool, *, token: str | None = None) -> None:
        self.pool = pool
        self.token = token
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

        handled = await apply_provider_status_update(
            self.pool,
            payload,
            cancel_followup=cancel_followup_for_client,
        )
        if not handled:
            logger.debug("Webhook payload ignored: %s", payload)
        return web.json_response({"ok": True, "handled": handled})


async def start_wahelp_webhook(
    pool: asyncpg.Pool,
    *,
    host: str,
    port: int,
    token: str | None = None,
) -> WahelpWebhookServer:
    server = WahelpWebhookServer(pool, token=token)
    await server.start(host, port)
    return server


__all__ = ["WahelpWebhookServer", "start_wahelp_webhook"]
