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
from datetime import datetime
from typing import Any, Callable, Optional

# Воронка и этапы, за которыми следит обмен. «Передано в работу» — успешное
# закрытие ПЕРВИЧНОЙ воронки: оно наступает в момент оформления заказа, а не
# после работы. Успех воронки реализации был бы поздно.
AMO_PIPELINE_PRIMARY = 4482751          # Воронка первичной обработки
AMO_STATUS_WON = 142                    # «Передано в работу»
AMO_STATUS_LOST = 143                   # «Закрыто и не реализовано»

# Поля сделки. Берём по идентификаторам, а не по названиям колонок выгрузки:
# правка шаблона выгрузки в CRM больше ни на что не влияет.
AMO_FIELD_ADDRESS = 18639               # Адрес
AMO_FIELD_SERVICE = 271915              # Услуга (множественный список)


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
    if incoming.address and incoming.address != existing.address:
        # Колонка называется last_order_addr — «адрес последнего заказа», и по
        # смыслу обновляется каждым новым. Прежнее «дописываем только пустое»
        # превращало её в «адрес первого заказа»: 2026-08-28 в карточке годами
        # лежало слово «адрес», и с ним ушло первое письмо клиенту.
        updates["last_order_addr"] = incoming.address
    if incoming.service and incoming.service != existing.service:
        updates["last_service"] = incoming.service

    if not updates:
        return ExchangeDecision("skip", reason="всё уже заполнено")
    return ExchangeDecision("update", fields=updates, reason="дописал недостающее")


# Отказные сделки заводятся пачкой по понедельникам (решение владельца
# 2026-08-28). Им никто не пишет сообщений, значит срочности нет: пачку
# владельцу проверять легче, чем ручеёк из двух-трёх записей в день.
WEEKLY_LEADS_WEEKDAY = 0                # понедельник
WEEKLY_LEADS_PERIOD_DAYS = 7


def is_weekly_leads_day(now: datetime) -> bool:
    """Сегодня ли день, когда заводим лидов за прошедшую неделю."""
    return now.weekday() == WEEKLY_LEADS_WEEKDAY


def outcome_of_event(event: dict) -> Optional[str]:
    """Чем закончилась сделка — по самому событию, без запроса в CRM.

    amoCRM присылает в событии смены этапа новый статус и воронку, поэтому
    чужие сделки отсеиваются даром. Это не оптимизация ради красоты: событий
    смены этапа за сутки сотни (каждое движение каждой сделки), и запрашивать
    по каждому карточку значило бы жечь лимиты API впустую.
    """
    for item in event.get("value_after") or []:
        status = (item or {}).get("lead_status") or {}
        if int(status.get("pipeline_id") or 0) != AMO_PIPELINE_PRIMARY:
            continue
        status_id = int(status.get("id") or 0)
        if status_id == AMO_STATUS_WON:
            return "won"
        if status_id == AMO_STATUS_LOST:
            return "lost"
    return None


def field_values(entity: dict, field_id: int) -> list[str]:
    """Значения поля сделки по его идентификатору. Пустые отбрасываем.

    Публичная намеренно: разбор полей сделки один на весь проект (им пользуется
    и разговор с клиентом до работы). Две копии этого разбора рано или поздно
    разошлись бы, и половина писем ушла бы без адреса.
    """
    for field_data in entity.get("custom_fields_values") or []:
        if int(field_data.get("field_id") or 0) != field_id:
            continue
        return [str(value.get("value")) for value in field_data.get("values") or []
                if value.get("value")]
    return []


def _contact_phone(contact: dict) -> str:
    for field_data in (contact or {}).get("custom_fields_values") or []:
        if field_data.get("field_code") != "PHONE":
            continue
        for value in field_data.get("values") or []:
            if value.get("value"):
                return str(value["value"])
    return ""


def prefer_deal_fields(incoming: IncomingClient,
                       deal: Optional[dict]) -> IncomingClient:
    """Адрес и услугу берём из дочерней сделки: там данные заказа, а не клиента.

    В лид адрес попадает автозаполнением из контакта — то есть с прошлого раза.
    В дочернюю сделку его кладёт робот владельца прямо из календаря. Разницу
    видно на живом примере 2026-08-28: в лиде «адрес», в сделке «Академика
    Сахарова 109к2 кв 142».

    Сделки нет — оставляем как было: половина данных лучше, чем ничего.
    """
    if deal is None:
        return incoming
    address = next(iter(field_values(deal, AMO_FIELD_ADDRESS)), None)
    services = field_values(deal, AMO_FIELD_SERVICE)
    return IncomingClient(
        phone=incoming.phone,
        digits=incoming.digits,
        name=incoming.name,
        address=address or incoming.address,
        service=", ".join(services) if services else incoming.service,
    )


def incoming_from_amo(
    lead: dict,
    contact: Optional[dict],
    *,
    normalize_phone: Callable[[str], tuple[Optional[str], Optional[str]]],
) -> IncomingClient:
    """Собрать данные клиента из сделки и её контакта.

    Нормализация телефона приходит параметром: канон телефонов в проекте один
    (`bot._amo_normalize_phone`), и повторять его здесь нельзя — разошедшиеся
    правила нормализации означают дубли записей на одного человека.
    """
    phone, digits = normalize_phone(_contact_phone(contact or {}))
    services = field_values(lead, AMO_FIELD_SERVICE)
    return IncomingClient(
        phone=phone,
        digits=digits,
        name=(contact or {}).get("name") or None,
        address=next(iter(field_values(lead, AMO_FIELD_ADDRESS)), None),
        service=", ".join(services) if services else None,
    )


__all__ = [
    "AMO_PIPELINE_PRIMARY",
    "AMO_STATUS_LOST",
    "AMO_STATUS_WON",
    "ExchangeDecision",
    "ExistingClient",
    "IncomingClient",
    "decide_exchange",
    "WEEKLY_LEADS_PERIOD_DAYS",
    "field_values",
    "incoming_from_amo",
    "is_weekly_leads_day",
    "outcome_of_event",
    "prefer_deal_fields",
]
