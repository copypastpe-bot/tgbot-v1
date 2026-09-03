#!/usr/bin/env python3
"""Сторож канала до Telegram через прокси.

Прокси на голландском сервере стал единственной точкой отказа: если он умрёт,
бот тихо перестанет получать и отправлять сообщения, а владелец узнает об этом
по тишине. Сторож проверяет канал раз в несколько минут и, если тот молчит,
пишет владельцу в Telegram **в обход прокси** - напрямую, через пул адресов,
который оставлен именно как путь отката.

Логика тревоги вынесена в чистую функцию `decide`, чтобы её можно было
проверить тестами без сети и без ожидания реальной аварии.

Запуск (на проде, из каталога бота):
    /opt/telegram-bot/.venv/bin/python scripts/telegram_proxy_watchdog.py
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

# Сколько неудачных проверок подряд считаем аварией, а не рябью на сети.
ALERT_AFTER_FAILURES = 2

# Как часто повторять тревогу, пока авария продолжается.
REPEAT_ALERT_SEC = 3600

# Таймаут одной проверки: канал через прокси отвечает за 0.5 с, так что
# десяти секунд достаточно с большим запасом.
CHECK_TIMEOUT_SEC = 10.0

ACTION_NONE = None
ACTION_DOWN = "down"
ACTION_RECOVERED = "recovered"

EMPTY_STATE: dict[str, Any] = {"failures": 0, "down_since": None, "last_alert": None}


def decide(
    state: dict[str, Any],
    ok: bool,
    now: float,
    *,
    alert_after: int = ALERT_AFTER_FAILURES,
    repeat_sec: float = REPEAT_ALERT_SEC,
) -> tuple[dict[str, Any], str | None]:
    """Решает, что делать по итогам одной проверки.

    Возвращает новое состояние и действие: поднять тревогу, сообщить о
    восстановлении или промолчать.
    """
    if ok:
        alerted = state.get("last_alert") is not None
        return dict(EMPTY_STATE), (ACTION_RECOVERED if alerted else ACTION_NONE)

    failures = int(state.get("failures") or 0) + 1
    down_since = state.get("down_since") or now
    last_alert = state.get("last_alert")
    new_state = {"failures": failures, "down_since": down_since, "last_alert": last_alert}

    if failures < alert_after:
        return new_state, ACTION_NONE

    if last_alert is not None and now - float(last_alert) < repeat_sec:
        return new_state, ACTION_NONE

    new_state["last_alert"] = now
    return new_state, ACTION_DOWN


def load_env(path: Path) -> dict[str, str]:
    """Читает .env настолько просто, насколько он и написан."""
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def read_state(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return dict(EMPTY_STATE)


def write_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")


def humanize_minutes(seconds: float) -> str:
    minutes = max(1, int(round(seconds / 60)))
    if minutes < 60:
        return f"{minutes} мин"
    hours = minutes // 60
    rest = minutes % 60
    return f"{hours} ч {rest} мин" if rest else f"{hours} ч"


def build_alert_text(action: str, state: dict[str, Any], now: float) -> str:
    if action == ACTION_RECOVERED:
        down_since = state.get("down_since")
        how_long = humanize_minutes(now - float(down_since)) if down_since else "недолго"
        return (
            "Связь с Telegram через прокси восстановлена.\n"
            f"Не работала {how_long}."
        )
    down_since = state.get("down_since") or now
    return (
        "Бот не достучался до Telegram через голландский прокси.\n"
        f"Молчит {humanize_minutes(now - float(down_since))}.\n\n"
        "Через прокси ходят все три бота: рабочий, клиентский и админский.\n"
        "Что проверить: сервер Contabo и контейнер telegram-proxy.\n\n"
        "Откат на прямой путь:\n"
        "рабочий и клиентский — закомментировать TELEGRAM_PROXY_URL "
        "в /opt/telegram-bot/.env и /opt/telegram-bot-v2/.env, перезапустить;\n"
        "админский — sudo raketa-admin-bot-update --telegram-proxy-off"
    )


def parse_ip_pool(raw: str) -> list[str]:
    return [part.strip() for part in re.split(r"[,\s;]+", raw or "") if part.strip()]


async def check_through_proxy(token: str, proxy_url: str) -> bool:
    """Проверяет, отвечает ли Telegram именно через прокси."""
    import aiohttp

    timeout = aiohttp.ClientTimeout(total=CHECK_TIMEOUT_SEC)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                f"https://api.telegram.org/bot{token}/getMe", proxy=proxy_url
            ) as resp:
                return resp.status == 200
    except Exception:
        return False


async def notify_bypassing_proxy(token: str, chat_ids: list[str], text: str, ip_pool: list[str]) -> list[str]:
    """Пишет владельцу напрямую, минуя прокси: тревога не должна зависеть от того,
    на что жалуется."""
    import aiohttp

    from telegram_transport import TelegramIPFallbackResolver

    connector_args: dict[str, Any] = {}
    if ip_pool:
        connector_args = {"resolver": TelegramIPFallbackResolver(ip_pool), "ttl_dns_cache": 0}

    delivered: list[str] = []
    timeout = aiohttp.ClientTimeout(total=CHECK_TIMEOUT_SEC * 2)
    connector = aiohttp.TCPConnector(**connector_args)
    try:
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            for chat_id in chat_ids:
                try:
                    async with session.post(
                        f"https://api.telegram.org/bot{token}/sendMessage",
                        json={"chat_id": chat_id, "text": text},
                    ) as resp:
                        if resp.status == 200:
                            delivered.append(chat_id)
                except Exception:
                    continue
    finally:
        await connector.close()
    return delivered


async def main() -> int:
    parser = argparse.ArgumentParser(description="Сторож канала до Telegram через прокси")
    parser.add_argument("--env", default=str(REPO_ROOT / ".env"), help="путь к .env бота")
    parser.add_argument(
        "--state",
        default=str(REPO_ROOT / "logs" / "proxy_watchdog_state.json"),
        help="файл состояния сторожа",
    )
    parser.add_argument("--quiet", action="store_true", help="без вывода при спокойной проверке")
    args = parser.parse_args()

    env = load_env(Path(args.env))
    token = env.get("BOT_TOKEN") or os.getenv("BOT_TOKEN") or ""
    proxy_url = env.get("TELEGRAM_PROXY_URL") or ""
    chat_ids = [part.strip() for part in re.split(r"[,\s;]+", env.get("ADMIN_TG_IDS", "")) if part.strip()]
    ip_pool = parse_ip_pool(env.get("TELEGRAM_API_IPS", ""))

    if not token:
        print("BOT_TOKEN не найден, проверять нечего", file=sys.stderr)
        return 2
    if not proxy_url:
        if not args.quiet:
            print("TELEGRAM_PROXY_URL не задан — бот ходит напрямую, сторож не нужен")
        return 0

    state_path = Path(args.state)
    state = read_state(state_path)
    now = time.time()

    ok = await check_through_proxy(token, proxy_url)
    new_state, action = decide(state, ok, now)
    write_state(state_path, new_state)

    if action is None:
        if not args.quiet:
            print("канал через прокси в порядке" if ok else f"сбой, подряд: {new_state['failures']}")
        return 0

    text = build_alert_text(action, state if action == ACTION_RECOVERED else new_state, now)
    delivered = await notify_bypassing_proxy(token, chat_ids, text, ip_pool)
    print(f"тревога={action} доставлено={len(delivered)} из {len(chat_ids)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
