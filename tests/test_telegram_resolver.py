"""Тесты выбора живого IP api.telegram.org.

Проба подменяется фейком, поэтому тесты не ходят в сеть и не зависят от того,
что сегодня блокирует провайдер.
"""

import asyncio
import socket
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from telegram_transport import (  # noqa: E402
    FALLBACK_TRUST_SEC,
    TELEGRAM_API_HOST,
    TelegramIPFallbackResolver,
)

GOOD = "149.154.167.220"
DEAD_1 = "149.154.167.91"
DEAD_2 = "91.108.56.130"
POOL = [GOOD, DEAD_1, DEAD_2]


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeProbe:
    """Пишет, кого проверяли, и отвечает по заранее заданным правилам."""

    def __init__(self, alive: set[str], fail_first: set[str] | None = None) -> None:
        self.alive = alive
        self.fail_first = dict.fromkeys(fail_first or set(), True)
        self.calls: list[str] = []
        self.in_flight = 0
        self.max_in_flight = 0

    async def __call__(self, ip: str, port: int, timeout: float, host: str) -> bool:
        self.calls.append(ip)
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            await asyncio.sleep(0.01)
            if self.fail_first.get(ip):
                self.fail_first[ip] = False
                return False
            return ip in self.alive
        finally:
            self.in_flight -= 1


def build(probe: FakeProbe, clock: FakeClock, pool: list[str] | None = None) -> TelegramIPFallbackResolver:
    return TelegramIPFallbackResolver(
        pool if pool is not None else POOL,
        probe=probe,
        probe_timeout=1.5,
        recheck_sec=30.0,
        clock=clock,
    )


def run(coro):
    return asyncio.run(coro)


def test_picks_the_only_live_address():
    probe = FakeProbe(alive={GOOD})
    resolver = build(probe, FakeClock())

    async def scenario():
        records = await resolver.resolve(TELEGRAM_API_HOST, 443)
        return records[0]

    record = run(scenario())
    assert record["host"] == GOOD
    assert record["hostname"] == TELEGRAM_API_HOST
    assert record["port"] == 443
    assert record["family"] == socket.AF_INET


def test_no_probes_while_selection_is_fresh():
    probe = FakeProbe(alive={GOOD})
    clock = FakeClock()
    resolver = build(probe, clock)

    async def scenario():
        await resolver.resolve(TELEGRAM_API_HOST, 443)
        probe.calls.clear()
        clock.advance(29.0)
        await resolver.resolve(TELEGRAM_API_HOST, 443)
        await resolver.resolve(TELEGRAM_API_HOST, 443)

    run(scenario())
    assert probe.calls == []


def test_working_address_is_rechecked_first_and_dead_ones_are_not_touched():
    """Главная правка: раньше рабочий адрес проверялся последним, после всех мёртвых."""
    probe = FakeProbe(alive={GOOD})
    clock = FakeClock()
    resolver = build(probe, clock)

    async def scenario():
        await resolver.resolve(TELEGRAM_API_HOST, 443)
        probe.calls.clear()
        clock.advance(31.0)
        records = await resolver.resolve(TELEGRAM_API_HOST, 443)
        return records[0]["host"]

    assert run(scenario()) == GOOD
    assert probe.calls == [GOOD]


def test_single_lost_packet_does_not_move_bot_to_dead_address():
    """Рабочий адрес теряет пакет примерно в каждой шестой пробе — это не повод уходить."""
    probe = FakeProbe(alive={GOOD})
    clock = FakeClock()
    resolver = build(probe, clock)

    async def scenario():
        await resolver.resolve(TELEGRAM_API_HOST, 443)
        probe.calls.clear()
        probe.fail_first[GOOD] = True
        clock.advance(31.0)
        records = await resolver.resolve(TELEGRAM_API_HOST, 443)
        return records[0]["host"]

    assert run(scenario()) == GOOD
    assert probe.calls == [GOOD, GOOD]
    assert DEAD_1 not in probe.calls
    assert DEAD_2 not in probe.calls


def test_cold_start_with_a_lost_packet_still_lands_on_a_live_address():
    """На старте выбранного адреса ещё нет, поэтому потерянный пакет ловится вторым кругом."""
    probe = FakeProbe(alive={DEAD_2}, fail_first={DEAD_2})
    clock = FakeClock()
    resolver = build(probe, clock)

    async def scenario():
        records = await resolver.resolve(TELEGRAM_API_HOST, 443)
        return records[0]["host"]

    assert run(scenario()) == DEAD_2


def test_switches_when_selected_address_really_dies():
    probe = FakeProbe(alive={GOOD})
    clock = FakeClock()
    resolver = build(probe, clock)

    async def scenario():
        await resolver.resolve(TELEGRAM_API_HOST, 443)
        probe.alive = {DEAD_2}
        clock.advance(31.0)
        records = await resolver.resolve(TELEGRAM_API_HOST, 443)
        return records[0]["host"]

    assert run(scenario()) == DEAD_2


def test_dead_pool_is_probed_in_parallel():
    """Перебор мёртвого пула должен стоить одну пробу по времени, а не сумму проб."""
    probe = FakeProbe(alive=set())
    clock = FakeClock()
    resolver = build(probe, clock)

    async def scenario():
        await resolver.resolve(TELEGRAM_API_HOST, 443)

    run(scenario())
    assert probe.max_in_flight >= 2


def test_when_nobody_answers_keeps_last_known_and_rechecks_soon():
    probe = FakeProbe(alive={GOOD})
    clock = FakeClock()
    resolver = build(probe, clock)

    async def scenario():
        await resolver.resolve(TELEGRAM_API_HOST, 443)
        probe.alive = set()
        clock.advance(31.0)
        first = (await resolver.resolve(TELEGRAM_API_HOST, 443))[0]["host"]

        probe.calls.clear()
        clock.advance(FALLBACK_TRUST_SEC - 1)
        cached = (await resolver.resolve(TELEGRAM_API_HOST, 443))[0]["host"]
        probes_inside_window = list(probe.calls)

        probe.alive = {GOOD}
        clock.advance(2.0)
        recovered = (await resolver.resolve(TELEGRAM_API_HOST, 443))[0]["host"]
        return first, cached, probes_inside_window, recovered

    first, cached, probes_inside_window, recovered = run(scenario())
    assert first == GOOD
    assert cached == GOOD
    assert probes_inside_window == []
    assert recovered == GOOD


def test_single_address_pool_never_loses_it():
    probe = FakeProbe(alive=set())
    clock = FakeClock()
    resolver = build(probe, clock, pool=[GOOD])

    async def scenario():
        records = await resolver.resolve(TELEGRAM_API_HOST, 443)
        return records[0]["host"]

    assert run(scenario()) == GOOD


def test_other_hosts_go_to_the_normal_resolver():
    probe = FakeProbe(alive={GOOD})
    resolver = build(probe, FakeClock())

    class StubDefault:
        def __init__(self) -> None:
            self.asked: list[str] = []

        async def resolve(self, host, port=0, family=socket.AF_UNSPEC):
            self.asked.append(host)
            return [{"hostname": host, "host": "203.0.113.7", "port": port}]

    stub = StubDefault()
    resolver._default = stub

    async def scenario():
        records = await resolver.resolve("wahelp.ru", 443)
        return records[0]["host"]

    assert run(scenario()) == "203.0.113.7"
    assert stub.asked == ["wahelp.ru"]
    assert probe.calls == []
