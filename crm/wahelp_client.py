"""
Async Wahelp API client.

Wahelp docs: see docs/api_wahelp.txt
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, MutableMapping
import asyncio
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
    """Per-project configuration."""

    project_id: str
    base_url: str = DEFAULT_BASE_URL
    token: str | None = None


@dataclass(slots=True)
class WahelpCredentials:
    """Login/password pair for automated token retrieval."""

    login: str
    password: str
    base_url: str = DEFAULT_BASE_URL


class WahelpAuthManager:
    """Handles token issuance/refresh via /api/app/user/login."""

    def __init__(
        self,
        credentials: WahelpCredentials,
        *,
        session: aiohttp.ClientSession | None = None,
        timeout: aiohttp.ClientTimeout = DEFAULT_TIMEOUT,
        safety_margin: int = 60,
    ) -> None:
        self._credentials = credentials
        self._base_url = credentials.base_url.rstrip("/")
        self._own_session = session is None
        self._session = session or aiohttp.ClientSession(timeout=timeout)
        self._token: str | None = None
        self._expires_at: float = 0.0
        self._lock = asyncio.Lock()
        self._safety_margin = max(0, safety_margin)

    async def close(self) -> None:
        if self._own_session and not self._session.closed:
            await self._session.close()

    async def get_token(self) -> str:
        now = asyncio.get_running_loop().time()
        if self._token and (self._expires_at - now) > self._safety_margin:
            return self._token
        async with self._lock:
            now = asyncio.get_running_loop().time()
            if self._token and (self._expires_at - now) > self._safety_margin:
                return self._token
            return await self._refresh()

    async def _refresh(self) -> str:
        url = f"{self._base_url}/api/app/user/login"
        payload = {
            "login": self._credentials.login,
            "password": self._credentials.password,
        }
        async with self._session.post(url, json=payload) as resp:
            data = await WahelpClient._parse_response(resp)
            if resp.status >= 400:
                raise WahelpAPIError(resp.status, "login failed", data)
            token = (
                data.get("data", {}).get("access_token")
                if isinstance(data, Mapping)
                else None
            )
            if not token:
                raise WahelpAPIError(resp.status, "missing access_token", data)
            expires_in = (
                data.get("data", {}).get("expires_in", 3600)
                if isinstance(data, Mapping)
                else 3600
            )
            now = asyncio.get_running_loop().time()
            self._token = token
            self._expires_at = now + max(self._safety_margin, expires_in)
            logger.info("Wahelp token refreshed (expires in %s s)", expires_in)
            return token


class WahelpClient:
    """Thin async wrapper around Wahelp REST API."""

    def __init__(
        self,
        *,
        session: aiohttp.ClientSession | None = None,
        timeout: aiohttp.ClientTimeout = DEFAULT_TIMEOUT,
        auth_manager: WahelpAuthManager | None = None,
    ) -> None:
        self._own_session = session is None
        self._session = session or aiohttp.ClientSession(timeout=timeout)
        self._auth_manager = auth_manager

    async def close(self) -> None:
        if self._own_session and not self._session.closed:
            await self._session.close()
        if self._auth_manager:
            await self._auth_manager.close()

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

    async def list_channels(self, config: WahelpProjectConfig) -> Any:
        path = f"/api/app/projects/{config.project_id}/channels"
        return await self._request("GET", path, config=config)

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
        token = await self._resolve_token(config)
        headers = {
            "Authorization": f"Bearer {token}",
        }
        if config.project_id:
            headers["X-Project"] = str(config.project_id)
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

    async def _resolve_token(self, config: WahelpProjectConfig) -> str:
        if config.token:
            return config.token
        if not self._auth_manager:
            raise RuntimeError("WahelpClient has no auth manager and token not provided")
        return await self._auth_manager.get_token()

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
