"""Адрес заказа: откуда он берётся в момент проведения.

Актуальный адрес владелец пишет в календарь, оттуда админский робот переносит
его в поле «Адрес» открытой сделки воронки реализации. Карточка клиента про
переезды не знает — в ней лежит адрес, по которому ездили когда-то. Поэтому
адрес стал свойством заказа: в момент проведения бот спрашивает у amoCRM
адрес открытой сделки клиента и запоминает его в самом заказе.

Три правила важнее остальных:

1. **Проведение заказа от amoCRM не зависит.** Недоступная CRM, медленный
   ответ, ненайденный контакт — всё это «адреса нет», а не ошибка мастеру.
   Заказ проводится по карточке клиента, как раньше. Отсюда `LOOKUP_TIMEOUT`
   и то, что `fetch_deal_address` не бросает исключений вообще никогда.
2. **Чужую сделку не берём.** Поиск amoCRM — подстрочный по всем полям, а у
   постоянного клиента открытых сделок бывает несколько (старая «ждёт оплаты»
   и сегодняшняя). Контакт сверяем по окончанию номера, сделку выбираем по
   близости даты работы к моменту проведения, а не по свежести: «самая новая»
   на второй заказ подряд указала бы не на ту.
3. **Неверный адрес хуже отсутствующего.** «Адреса нет» честно откатывается на
   карточку клиента, а неверный выглядит правдоподобно, и заметят его поздно.
   Поэтому сомнительные сделки лучше выбросить, чем взять.

Ни базы, ни `bot.py` модуль не знает: сюда приходит клиент amoCRM (любой объект
с двумя нужными методами), отсюда уходит строка адреса или ничего.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime
from typing import Any, Mapping, Optional

from .amo_exchange import (
    AMO_FIELD_ADDRESS,
    AMO_STATUS_LOST,
    AMO_STATUS_WON,
    field_values,
)
from .client_messaging import AMO_FIELD_ORDER_DATETIME, AMO_PIPELINE_REALIZATION

logger = logging.getLogger(__name__)

LOOKUP_TIMEOUT = 8.0                 # секунд на весь поход в amoCRM при проведении заказа
MAX_DEAL_DISTANCE = 2 * 24 * 3600    # двое суток вокруг проведения — дальше это другой заказ
MAX_LEADS_TO_FETCH = 50              # столько самых новых сделок клиента успеваем прочитать


def phone_query(phone: Optional[str]) -> Optional[str]:
    """Десять цифр для поиска контакта в amoCRM.

    Код страны отбрасываем: иначе «+7» и «8» на один и тот же номер дают два
    разных запроса, и по одному из них amoCRM ничего не находит. Всё, что на
    российский номер не похоже, поиском не считаем — искать по семи цифрам
    значит найти половину базы.
    """
    digits = re.sub(r"[^0-9]", "", phone or "")
    if len(digits) == 11 and digits[0] in ("7", "8"):
        return digits[1:]
    if len(digits) == 10:
        return digits
    return None


def contact_matches_phone(contact: Mapping[str, Any], phone10: str) -> bool:
    """Тот ли это человек: есть ли у контакта номер, оканчивающийся на phone10.

    amoCRM ищет подстроку по всем полям карточки, а не по телефону, — десять
    цифр могут встретиться в комментарии к чужому контакту. Чужого отсеиваем
    здесь, до того как прочитаем его сделки.
    """
    if not phone10 or not contact:
        return False
    for field_data in contact.get("custom_fields_values") or []:
        if not isinstance(field_data, Mapping):
            continue
        if str(field_data.get("field_code") or "").upper() != "PHONE":
            continue
        for value in field_data.get("values") or []:
            if not isinstance(value, Mapping):
                continue
            digits = re.sub(r"[^0-9]", "", str(value.get("value") or ""))
            if digits and digits.endswith(phone10):
                return True
    return False


def contact_lead_ids(contact: Mapping[str, Any]) -> list[int]:
    """Номера сделок контакта — так, как их отдала amoCRM.

    Порядок сохраняем, мусор пропускаем: испорченная запись в списке не должна
    стоить нам всех остальных сделок клиента.
    """
    ids: list[int] = []
    for lead in ((contact.get("_embedded") or {}).get("leads") or []):
        if not isinstance(lead, Mapping):
            continue
        try:
            ids.append(int(lead["id"]))
        except (KeyError, TypeError, ValueError):
            continue
    return ids


def pick_open_deal(deals: list[Mapping[str, Any]], *,
                   now: datetime) -> Optional[Mapping[str, Any]]:
    """Открытая сделка реализации, парная проводимому заказу.

    Считаем сделку парной по дате работы: мастер проводит заказ сразу после
    работы, значит нужная сделка — та, чья дата ближе всего к этой минуте.
    Старая «Заказ выполнен, ждёт оплаты» тоже открыта, но ездили не по ней,
    а заказ на следующую неделю уже заведён и тоже мешает.

    Дальше `MAX_DEAL_DISTANCE` от момента проведения сделка выбывает совсем,
    а не отходит в конец очереди: заказ на следующий месяц — это другой заказ
    по другому адресу. Иначе ловушка выглядит так: сегодняшнюю сделку владелец
    уже закрыл или завёл без даты, зато у постоянного клиента открыта сделка
    на месяц вперёд — и она бы выиграла. Взять её значит напечатать в отчёте
    правдоподобный, но неверный адрес; отсутствие адреса честнее — оно молча
    откатывается на карточку клиента.

    При равном расстоянии выигрывает прошедшая: работу уже сделали, а не
    только собираются. Сделки без даты (или с датой, испорченной ручной
    правкой) — последняя надежда, среди них берём самую свежую.

    `now` обязан быть с часовым поясом: сдвиг на три часа меняет выбор между
    сделками одного дня, и ошибка эта была бы тихой.
    """
    if now.tzinfo is None:
        raise ValueError("pick_open_deal: now должен быть с часовым поясом")
    now_ts = now.timestamp()
    dated: list[tuple[float, int, Mapping[str, Any]]] = []
    undated: list[tuple[int, Mapping[str, Any]]] = []
    for deal in deals:
        if not isinstance(deal, Mapping):
            continue
        if _as_int(deal.get("pipeline_id")) != AMO_PIPELINE_REALIZATION:
            continue
        if _as_int(deal.get("status_id")) in (AMO_STATUS_WON, AMO_STATUS_LOST):
            continue
        order_ts = _order_timestamp(deal)
        if order_ts is None:
            undated.append((_as_int(deal.get("created_at")), deal))
            continue
        distance = abs(order_ts - now_ts)
        if distance > MAX_DEAL_DISTANCE:
            continue
        dated.append((distance, 0 if order_ts <= now_ts else 1, deal))
    if dated:
        return min(dated, key=lambda item: (item[0], item[1]))[2]
    if undated:
        return max(undated, key=lambda item: item[0])[1]
    return None


def address_of_deal(deal: Optional[Mapping[str, Any]]) -> Optional[str]:
    """Адрес из карточки сделки. Нет поля — нет адреса, гадать не о чем."""
    if not deal:
        return None
    value = next(iter(field_values(dict(deal), AMO_FIELD_ADDRESS)), None)
    return str(value).strip() or None if value else None


def resolve_order_address(*, deal_address: Optional[str],
                          card_address: Optional[str],
                          last_order_addr: Optional[str]) -> Optional[str]:
    """Адрес заказа: сделка → карточка → адрес прошлого заказа → ничего.

    Календарь — источник правды, карточка и прошлый заказ нужны ровно на тот
    случай, когда CRM не ответила: отчёт без адреса хуже отчёта с адресом
    вчерашней точности.
    """
    for value in (deal_address, card_address, last_order_addr):
        cleaned = str(value).strip() if value else ""
        if cleaned:
            return cleaned
    return None


async def fetch_deal_address(client: Any, phone: Optional[str], *, now: datetime,
                             timeout: float = LOOKUP_TIMEOUT) -> Optional[str]:
    """Адрес открытой сделки реализации по телефону клиента.

    Исключений не бросает никогда: мастер ждёт «Готово ✅», и упавшая или
    задумавшаяся CRM не имеет права ни сломать проведение, ни задержать его
    дольше `timeout`. Всё, что пошло не так, — строка в журнале и «адреса нет».

    В журнал уходит только тип ошибки и код ответа: тело ответа amoCRM
    повторяет параметры запроса, а в них — телефон клиента.
    """
    phone10 = phone_query(phone)
    if not phone10:
        logger.info("адрес заказа: телефон %s не годится для поиска в amoCRM",
                    _mask(phone))
        return None
    try:
        return await asyncio.wait_for(_lookup(client, phone10, now=now), timeout)
    except asyncio.TimeoutError:
        logger.warning("адрес заказа: amoCRM не ответила за %s с (%s)",
                       timeout, _mask(phone10))
        return None
    except Exception as err:  # noqa: BLE001
        logger.warning("адрес заказа: amoCRM не дала ответа (%s): %s %s",
                       _mask(phone10), type(err).__name__, getattr(err, "status", None))
        return None


async def _lookup(client: Any, phone10: str, *,
                  now: datetime) -> Optional[str]:
    """Два запроса: контакты по номеру, затем их сделки одной пачкой.

    Сделок берём не больше `MAX_LEADS_TO_FETCH` самых новых (номера в amoCRM
    растут): у клиента с тридцатью заказами полный список стоил бы трёх
    запросов, а восьми секунд на это нет.
    """
    contacts = [contact for contact in await client.find_contacts_by_phone(phone10)
                if isinstance(contact, Mapping) and contact_matches_phone(contact, phone10)]
    if not contacts:
        logger.info("адрес заказа: контакта с номером %s в amoCRM нет", _mask(phone10))
        return None
    lead_ids: list[int] = []
    for contact in contacts:
        lead_ids.extend(contact_lead_ids(contact))
    if not lead_ids:
        logger.info("адрес заказа: у контакта %s нет сделок", _mask(phone10))
        return None
    if len(lead_ids) > MAX_LEADS_TO_FETCH:
        newest = set(sorted(lead_ids, reverse=True)[:MAX_LEADS_TO_FETCH])
        lead_ids = [lead_id for lead_id in lead_ids if lead_id in newest]
    deals = list(await client.fetch_leads_by_ids(lead_ids))
    deal = pick_open_deal(deals, now=now)
    address = address_of_deal(deal)
    if not address:
        logger.info("адрес заказа: открытой сделки с адресом нет (%s)", _mask(phone10))
        return None
    logger.info("адрес заказа для %s: сделка %s", _mask(phone10), (deal or {}).get("id"))
    return address


def _order_timestamp(deal: Mapping[str, Any]) -> Optional[float]:
    """Дата работы из сделки. Поле правили руками — считаем, что даты нет."""
    raw = next(iter(field_values(dict(deal), AMO_FIELD_ORDER_DATETIME)), None)
    if raw is None:
        return None
    try:
        return float(int(str(raw).strip()))
    except (TypeError, ValueError):
        return None


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _mask(phone: Optional[str]) -> str:
    """В журнал — только последние четыре цифры: номер клиента там не место."""
    digits = re.sub(r"[^0-9]", "", phone or "")
    return f"…{digits[-4:]}" if len(digits) >= 4 else "…"


__all__ = [
    "LOOKUP_TIMEOUT",
    "MAX_DEAL_DISTANCE",
    "MAX_LEADS_TO_FETCH",
    "address_of_deal",
    "contact_lead_ids",
    "contact_matches_phone",
    "fetch_deal_address",
    "phone_query",
    "pick_open_deal",
    "resolve_order_address",
]
