"""
Async Wahelp API client.

Wahelp docs: see docs/api_wahelp.txt
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, MutableMapping
import logging

import aiohttp

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://app.wahelp.me"
DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=20)


class WahelpAPIError(RuntimeError):
    """Raised when Wahelp API responds with an error."""

    def __init__(self, status: int, message: str, details: Any | None = None):
        super().__init__(f"Wahelp API error {status}: {message}")
        self.status = status
        self.message = message
        self.details = details


@dataclass(slots=True)
class WahelpProjectConfig:
    """Per-project credentials."""

    token: str
    project_id: str
    base_url: str = DEFAULT_BASE_URL

    def headers(self) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self.token}",
        }
        if self.project_id:
            headers["X-Project"] = str(self.project_id)
        return headers


class WahelpClient:
    """Thin async wrapper around Wahelp REST API."""

    def __init__(
        self,
        *,
        session: aiohttp.ClientSession | None = None,
        timeout: aiohttp.ClientTimeout = DEFAULT_TIMEOUT,
    ) -> None:
        self._own_session = session is None
        self._session = session or aiohttp.ClientSession(timeout=timeout)

    async def close(self) -> None:
        if self._own_session and not self._session.closed:
            await self._session.close()

    async def __aenter__(self) -> "WahelpClient":
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()

    async def send_message(
        self,
        config: WahelpProjectConfig,
        *,
        channel_uuid: str,
        user_id: str | int,
        payload: Mapping[str, Any],
    ) -> Any:
        path = f"/api/app/projects/{config.project_id}/channels/{channel_uuid}/send_message/{user_id}"
        return await self._request("POST", path, config=config, json=payload)

    async def ensure_user(
        self,
        config: WahelpProjectConfig,
        *,
        channel_uuid: str,
        user_payload: Mapping[str, Any],
    ) -> Any:
        """
        Create or update user inside channel.
        If user already exists, API will return validation error with existing ID.
        """
        path = f"/api/app/projects/{config.project_id}/channels/{channel_uuid}/users"
        return await self._request("POST", path, config=config, json=user_payload)

    async def get_messages(
        self,
        config: WahelpProjectConfig,
        *,
        channel_id: str,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        path = f"/api/app/projects/{config.project_id}/channels/{channel_id}/messages"
        return await self._request("GET", path, config=config, params=params)

    async def set_webhook(
        self,
        config: WahelpProjectConfig,
        *,
        url: str,
        events: list[str] | None = None,
    ) -> Any:
        payload: MutableMapping[str, Any] = {"url": url}
        if events:
            payload["events"] = events
        path = f"/api/app/projects/{config.project_id}/hook"
        return await self._request("PUT", path, config=config, json=payload)

    async def _request(
        self,
        method: str,
        path: str,
        *,
        config: WahelpProjectConfig,
        params: Mapping[str, Any] | None = None,
        json: Any | None = None,
    ) -> Any:
        url = f"{config.base_url.rstrip('/')}{path}"
        headers = config.headers()
        logger.debug("Wahelp %s %s params=%s json=%s", method, url, params, json)
        async with self._session.request(
            method,
            url,
            headers=headers,
            params=params,
            json=json,
        ) as resp:
            data = await self._parse_response(resp)
            if resp.status >= 400:
                raise WahelpAPIError(resp.status, data if isinstance(data, str) else str(data), data)
            return data

    @staticmethod
    async def _parse_response(resp: aiohttp.ClientResponse) -> Any:
        ctype = resp.headers.get("Content-Type", "")
        if "application/json" in ctype:
            return await resp.json()
        return await resp.text()


async def test_connection(config: WahelpProjectConfig, *, base_path: str = "/api/app/projects") -> Any:
    """
    Convenience helper for smoke tests in REPL.
    """
    async with WahelpClient() as client:
        return await client._request("GET", base_path, config=config)
