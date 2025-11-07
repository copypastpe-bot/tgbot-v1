"""
Utility script to follow Wahelp integration instructions step by step.

Steps:
1. Authorize with login/password -> receive access token.
2. Fetch channels for the specified project (to obtain UUID).

Usage:
    python -m scripts.wahelp_setup --project clients_tg
    python -m scripts.wahelp_setup --project_id 41245
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from typing import Any

import aiohttp
from dotenv import load_dotenv

DEFAULT_BASE_URL = "https://app.wahelp.ru"
PROJECT_ENV_KEYS = {
    "clients_tg": "WAHELP_CLIENTS_PROJECT_ID",
    "clients_wa": "WAHELP_CLIENTS_WA_PROJECT_ID",
    "leads": "WAHELP_LEADS_PROJECT_ID",
}


async def fetch_token(
    session: aiohttp.ClientSession,
    base_url: str,
    login: str,
    password: str,
) -> str:
    url = f"{base_url.rstrip('/')}/api/app/user/login"
    payload = {"login": login, "password": password}
    async with session.post(url, json=payload) as resp:
        data = await resp.json()
        if resp.status >= 400 or not data.get("success"):
            raise RuntimeError(f"Login failed: {resp.status} {data}")
        token = data.get("data", {}).get("access_token")
        if not token:
            raise RuntimeError(f"Login response missing access_token: {data}")
        print("✅ Авторизация успешна. Получен токен.")
        return token


async def fetch_channels(
    session: aiohttp.ClientSession,
    base_url: str,
    token: str,
    project_id: str,
) -> list[dict[str, Any]]:
    url = f"{base_url.rstrip('/')}/api/app/projects/{project_id}/channels"
    headers = {
        "Authorization": f"Bearer {token}",
        "X-Project": project_id,
    }
    async with session.get(url, headers=headers) as resp:
        data = await resp.json()
        if resp.status >= 400 or not data.get("success"):
            raise RuntimeError(f"Fetch channels failed: {resp.status} {data}")
        channels = data.get("data") or []
        print(f"✅ Найдено каналов: {len(channels)}")
        return channels


async def main_async(args: argparse.Namespace) -> None:
    load_dotenv()
    base_url = args.base_url or os.getenv("WAHELP_API_BASE") or DEFAULT_BASE_URL
    login = args.login or os.getenv("WAHELP_LOGIN")
    password = args.password or os.getenv("WAHELP_PASSWORD")
    project_id = args.project_id
    if not project_id and args.project_alias:
        env_key = PROJECT_ENV_KEYS.get(args.project_alias)
        if env_key:
            project_id = os.getenv(env_key)

    if not login or not password:
        raise SystemExit("Укажите WAHELP_LOGIN и WAHELP_PASSWORD в .env или через флаги --login/--password.")
    if not project_id:
        raise SystemExit("Укажите ID проекта: через --project_id или алиас --project clients_tg|clients_wa|leads.")

    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        token = await fetch_token(session, base_url, login, password)
        channels = await fetch_channels(session, base_url, token, project_id)
        print("📋 Список каналов (uuid внутри поля 'uuid'):\n")
        print(json.dumps(channels, indent=2, ensure_ascii=False))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Wahelp API setup helper.")
    parser.add_argument("--login", help="Wahelp login (email)")
    parser.add_argument("--password", help="Wahelp password")
    parser.add_argument("--project_id", help="Project ID (numeric string)")
    parser.add_argument(
        "--project",
        dest="project_alias",
        choices=list(PROJECT_ENV_KEYS.keys()),
        help="Shortcut project name (reads ID from .env)",
    )
    parser.add_argument("--base_url", help="Override Wahelp base URL")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
