"""Правила автообмена amoCRM → база бота.

Чистые функции: ни сети, ни базы. Решение о том, что делать с записью клиента,
принимается здесь и проверяется тестами; запись в базу — отдельный тонкий слой
в `bot.py`. Так логику можно менять и проверять, не поднимая ни CRM, ни Postgres.

Что делает обмен (решения владельца 2026-08-28): пополняет базу лидов и
дописывает клиентам адрес, услугу и имя. Всё остальное он не трогает.

Три правила, ради которых модуль и существует:

1. **Понижения статуса не бывает.** Кто уже клиент — остаётся клиентом.
   Автомат, честно применяющий «отказ → лид», за месяц тихо разжалует
   половину постоянных клиентов, и заметно это станет по упавшей рассылке.
2. **Отказная сделка известного человека не меняет ничего.** Постоянный клиент
   узнал цену и не заказал — менять в его карточке нечего.
3. **Бонусы и день рождения не пишем никогда.** Они рождаются в боте: клиент
   сам указал дату, бот сам начислил баллы. Запись из CRM затирала бы свежее
   старым.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class IncomingClient:
    """Что пришло из amoCRM по закрывшейся сделке."""

    phone: Optional[str]                    # +7XXXXXXXXXX
    digits: Optional[str]                   # только цифры — ключ сопоставления
    name: Optional[str] = None
    address: Optional[str] = None
    service: Optional[str] = None


@dataclass(frozen=True)
class ExistingClient:
    """Что уже лежит в базе бота по этому телефону."""

    client_id: int
    status: str                             # lead | client
    name: Optional[str] = None
    address: Optional[str] = None
    service: Optional[str] = None


@dataclass(frozen=True)
class ExchangeDecision:
    """Что робот сделает с записью и почему."""

    action: str                             # create_lead|create_client|update|skip
    fields: dict[str, Any] = field(default_factory=dict)
    reason: str = ""


def decide_exchange(*, outcome: str, incoming: IncomingClient,
                    existing: Optional[ExistingClient]) -> ExchangeDecision:
    """Решить, что делать с записью клиента.

    `outcome` — чем закончилась сделка первичной воронки: `won` («Передано
    в работу», то есть заказ оформлен) или `lost` (отказ).
    """
    if not incoming.digits:
        return ExchangeDecision("skip", reason="в сделке нет телефона")

    if outcome == "lost":
        if existing is not None:
            return ExchangeDecision(
                "skip", reason="отказ по известной записи — не трогаем")
        # Заказа не было, поэтому ни адреса, ни услуги в карточке лида не место.
        return ExchangeDecision("create_lead", fields={
            "status": "lead",
            "phone": incoming.phone,
            "full_name": incoming.name,
        }, reason="новый лид из отказной сделки")

    if existing is None:
        return ExchangeDecision("create_client", fields={
            "status": "client",
            "phone": incoming.phone,
            "full_name": incoming.name,
            "last_order_addr": incoming.address,
            "last_service": incoming.service,
        }, reason="новый клиент из успешной сделки")

    updates: dict[str, Any] = {}
    if existing.status != "client":
        updates["status"] = "client"        # только вверх; вниз пути нет
    if incoming.name and not existing.name:
        updates["full_name"] = incoming.name
    if incoming.address and not existing.address:
        updates["last_order_addr"] = incoming.address
    if incoming.service and incoming.service != existing.service:
        updates["last_service"] = incoming.service

    if not updates:
        return ExchangeDecision("skip", reason="всё уже заполнено")
    return ExchangeDecision("update", fields=updates, reason="дописал недостающее")


__all__ = [
    "ExchangeDecision",
    "ExistingClient",
    "IncomingClient",
    "decide_exchange",
]
