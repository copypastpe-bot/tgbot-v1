"""Helpers to work with Wahelp API inside the bot."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import os
from typing import Literal

import aiohttp
from dotenv import load_dotenv

from .wahelp_client import (
    DEFAULT_BASE_URL,
    WahelpAPIError,
    WahelpAuthManager,
    WahelpClient,
    WahelpCredentials,
    WahelpProjectConfig,
)

logger = logging.getLogger(__name__)

ChannelKind = Literal["clients_tg", "clients_wa", "leads"]


@dataclass(frozen=True)
class WahelpChannelConfig:
    project_id: str
    channel_uuid: str


CHANNEL_ENV_KEYS: dict[ChannelKind, tuple[str, str]] = {
    "clients_tg": ("WAHELP_CLIENTS_PROJECT_ID", "WAHELP_CLIENTS_CHANNEL_UUID"),
    "clients_wa": ("WAHELP_CLIENTS_WA_PROJECT_ID", "WAHELP_CLIENTS_WA_CHANNEL_UUID"),
    "leads": ("WAHELP_LEADS_PROJECT_ID", "WAHELP_LEADS_CHANNEL_UUID"),
}

load_dotenv()

_base_url = os.getenv("WAHELP_API_BASE", DEFAULT_BASE_URL)
_login = os.getenv("WAHELP_LOGIN")
_password = os.getenv("WAHELP_PASSWORD")
_static_token = os.getenv("WAHELP_ACCESS_TOKEN") or os.getenv("WAHELP_CLIENTS_TOKEN")

_credentials: WahelpCredentials | None = None
_auth_manager: WahelpAuthManager | None = None
_client: WahelpClient | None = None

timeout = aiohttp.ClientTimeout(total=20)
if _login and _password:
    _credentials = WahelpCredentials(login=_login, password=_password, base_url=_base_url)
else:
    logger.warning("Wahelp credentials are not fully configured; integration disabled.")


def get_wahelp_client() -> WahelpClient:
    global _client, _auth_manager
    if _client is None:
        if _credentials is None and not _static_token:
            raise RuntimeError("Wahelp client is not configured. Check WAHELP_LOGIN/WAHELP_PASSWORD or WAHELP_ACCESS_TOKEN in .env")
        _auth_manager = WahelpAuthManager(credentials=_credentials, timeout=timeout)
        if _static_token:
            # preload token if provided explicitly (bypasses login/refresh until expiry)
            _auth_manager._token = _static_token  # type: ignore[attr-defined]
            _auth_manager._expires_at = 10**12  # distant future
        _client = WahelpClient(auth_manager=_auth_manager)
    return _client


def get_project_config(kind: ChannelKind) -> WahelpProjectConfig:
    project_id = _get_channel_config(kind).project_id
    return WahelpProjectConfig(project_id=project_id, base_url=_base_url)


def get_channel_uuid(kind: ChannelKind) -> str:
    return _get_channel_config(kind).channel_uuid


def _get_channel_config(kind: ChannelKind) -> WahelpChannelConfig:
    env_keys = CHANNEL_ENV_KEYS.get(kind)
    if not env_keys:
        raise ValueError(f"Unknown Wahelp channel kind: {kind}")
    project_id = os.getenv(env_keys[0])
    channel_uuid = os.getenv(env_keys[1])
    if not project_id or not channel_uuid:
        raise RuntimeError(
            f"Environment variables {env_keys[0]} / {env_keys[1]} must be set for channel {kind}."
    )
    return WahelpChannelConfig(project_id=project_id, channel_uuid=channel_uuid)


async def ensure_user_in_channel(
    channel_kind: ChannelKind,
    *,
    phone: str,
    name: str | None = None,
) -> int:
    client = get_wahelp_client()
    cfg = get_project_config(channel_kind)
    channel_uuid = get_channel_uuid(channel_kind)
    payload: dict[str, str] = {"phone": phone}
    if name:
        payload["name"] = name
    resp = await client.ensure_user(cfg, channel_uuid=channel_uuid, user_payload=payload)
    user_id = _extract_user_id(resp)
    logger.debug("Wahelp ensure_user %s -> %s", channel_kind, user_id)
    return user_id


async def send_text_message(
    channel_kind: ChannelKind,
    *,
    user_id: int,
    text: str,
) -> dict:
    client = get_wahelp_client()
    cfg = get_project_config(channel_kind)
    channel_uuid = get_channel_uuid(channel_kind)
    payload = {"text": text}
    resp = await client.send_message(cfg, channel_uuid=channel_uuid, user_id=user_id, payload=payload)
    if isinstance(resp, dict) and resp.get("success") is False:
        raise WahelpAPIError(0, f"Failed to send message via {channel_kind}", resp)
    logger.info("Wahelp send via %s user=%s text=%s", channel_kind, user_id, text[:40])
    return resp if isinstance(resp, dict) else {"data": resp}


async def send_text_to_phone(
    channel_kind: ChannelKind,
    *,
    phone: str,
    name: str | None = None,
    text: str,
) -> dict:
    user_id = await ensure_user_in_channel(channel_kind, phone=phone, name=name)
    return await send_text_message(channel_kind, user_id=user_id, text=text)


def _extract_user_id(resp: dict | Any) -> int:
    if isinstance(resp, dict):
        if resp.get("success") is False:
            raise WahelpAPIError(0, "Wahelp ensure_user returned errors", resp)
        data = resp.get("data")
        if isinstance(data, dict) and "id" in data:
            return int(data["id"])
    raise WahelpAPIError(0, "Unexpected ensure_user response", resp)
