"""Транспорт до Telegram: выбор живого IP `api.telegram.org` в обход блокировок.

На российском сервере DNS отдаёт для `api.telegram.org` только IPv6-адрес,
который с хоста не маршрутизируется, поэтому адрес подменяется на IP из пула
(`TELEGRAM_API_IPS`). Пул живёт неровно: часть адресов может молчать неделями,
а рабочий — терять пакеты.

Правила выбора:

1. Пока выбранный адрес свеж (окно `recheck_sec`), проба не делается вообще.
2. При истечении окна первым проверяется текущий рабочий адрес — не последним.
3. Проба — полноценное TLS-рукопожатие с нужным SNI, а не просто TCP-коннект:
   блокировка часто пускает соединение и обрывает его на рукопожатии.
4. Рабочему адресу даётся вторая попытка: одиночная потеря пакета не должна
   уводить бота на заведомо мёртвый адрес на всё окно.
5. Остальные адреса проверяются параллельно, поэтому перебор мёртвого пула
   стоит одну пробу по времени, а не сумму проб.
"""

import asyncio
import logging
import socket
import ssl
import time as monotonic_time
from typing import Any, Awaitable, Callable

from aiohttp.abc import AbstractResolver
from aiohttp.resolver import DefaultResolver

logger = logging.getLogger(__name__)

TELEGRAM_API_HOST = "api.telegram.org"

# Сколько раз подряд пробуем текущий рабочий адрес, прежде чем искать замену.
SELECTED_PROBE_ATTEMPTS = 2

# Окно доверия после неудачи всего пула: короткое, чтобы быстро вернуться к жизни.
FALLBACK_TRUST_SEC = 5.0

ProbeFn = Callable[[str, int, float, str], Awaitable[bool]]

_ssl_context: ssl.SSLContext | None = None


def _shared_ssl_context() -> ssl.SSLContext:
    global _ssl_context
    if _ssl_context is None:
        _ssl_context = ssl.create_default_context()
    return _ssl_context


async def tls_probe(ip: str, port: int, timeout: float, host: str = TELEGRAM_API_HOST) -> bool:
    """Проверяет, что через этот IP реально поднимается TLS до `host`."""
    family = socket.AF_INET6 if ":" in ip else socket.AF_INET
    ssl_args: dict[str, Any] = {}
    if port == 443:
        ssl_args = {"ssl": _shared_ssl_context(), "server_hostname": host}
    writer = None
    try:
        _reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host=ip, port=port, family=family, **ssl_args),
            timeout=timeout,
        )
        return True
    except Exception:
        return False
    finally:
        if writer is not None:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass


class TelegramIPFallbackResolver(AbstractResolver):
    def __init__(
        self,
        ip_pool: list[str],
        *,
        probe: ProbeFn | None = None,
        probe_timeout: float = 1.5,
        recheck_sec: float = 30.0,
        host: str = TELEGRAM_API_HOST,
        clock: Callable[[], float] = monotonic_time.monotonic,
    ) -> None:
        self._ip_pool = list(ip_pool)
        self._probe = probe or tls_probe
        self._probe_timeout = probe_timeout
        self._recheck_sec = max(FALLBACK_TRUST_SEC, recheck_sec)
        self._host = host
        self._clock = clock
        self._default: DefaultResolver | None = None
        self._selected_ip: str | None = None
        self._selected_until = 0.0
        self._probe_lock = asyncio.Lock()

    @staticmethod
    def _record_for_ip(host: str, ip: str, port: int) -> dict[str, Any]:
        return {
            "hostname": host,
            "host": ip,
            "port": port,
            "family": socket.AF_INET6 if ":" in ip else socket.AF_INET,
            "proto": 0,
            "flags": socket.AI_NUMERICHOST,
        }

    def _trust(self, ip: str, seconds: float) -> None:
        self._selected_ip = ip
        self._selected_until = self._clock() + seconds

    async def _probe_selected(self, ip: str, port: int) -> bool:
        for attempt in range(SELECTED_PROBE_ATTEMPTS):
            if await self._probe(ip, port, self._probe_timeout, self._host):
                return True
            if attempt + 1 < SELECTED_PROBE_ATTEMPTS:
                logger.info("Telegram API IP %s did not answer, retrying", ip)
        return False

    async def _first_reachable(self, candidates: list[str], port: int, attempts: int = 1) -> str | None:
        if not candidates:
            return None
        for _ in range(attempts):
            results = await asyncio.gather(
                *(self._probe(ip, port, self._probe_timeout, self._host) for ip in candidates),
                return_exceptions=True,
            )
            for ip, ok in zip(candidates, results):
                if ok is True:
                    return ip
        return None

    async def _pick_reachable_ip(self, port: int) -> str:
        if self._selected_ip and self._clock() < self._selected_until:
            return self._selected_ip

        async with self._probe_lock:
            if self._selected_ip and self._clock() < self._selected_until:
                return self._selected_ip

            if self._selected_ip and await self._probe_selected(self._selected_ip, port):
                self._trust(self._selected_ip, self._recheck_sec)
                return self._selected_ip

            if self._selected_ip:
                logger.warning("Telegram API IP %s stopped answering, looking for another", self._selected_ip)

            candidates = [ip for ip in self._ip_pool if ip != self._selected_ip]
            # На холодном старте запасного адреса нет, поэтому одиночную потерю
            # пакета гасим вторым кругом, как и для уже выбранного адреса.
            attempts = 1 if self._selected_ip else SELECTED_PROBE_ATTEMPTS
            found = await self._first_reachable(candidates, port, attempts=attempts)
            if found:
                logger.warning("Telegram API IP selected: %s", found)
                self._trust(found, self._recheck_sec)
                return found

            fallback = self._selected_ip or self._ip_pool[0]
            logger.warning("No reachable Telegram API IP detected, fallback to %s", fallback)
            self._trust(fallback, FALLBACK_TRUST_SEC)
            return fallback

    async def resolve(
        self,
        host: str,
        port: int = 0,
        family: int = socket.AF_UNSPEC,
    ) -> list[dict[str, Any]]:
        if host == self._host and self._ip_pool:
            resolved_port = port or 443
            selected = await self._pick_reachable_ip(resolved_port)
            return [self._record_for_ip(host, selected, resolved_port)]
        if self._default is None:
            self._default = DefaultResolver()
        return await self._default.resolve(host, port, family)

    async def close(self) -> None:
        if self._default is not None:
            await self._default.close()
