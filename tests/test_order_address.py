"""Адрес заказа: откуда он берётся при проведении.

Два правила здесь важнее остальных, потому что нарушение любого из них
мастер заметит сразу, а владелец — поздно:

1. **Проведение заказа от amoCRM не зависит.** Недоступная CRM, медленный
   ответ, ненайденный контакт — всё это «адреса нет», а не ошибка мастеру.
   Заказ проводится по карточке клиента, как раньше.
2. **Чужую сделку не берём.** amoCRM ищет контакт подстрокой по всем полям,
   а у постоянного клиента открытых сделок бывает несколько (старая «ждёт
   оплаты» и сегодняшняя). Контакт сверяем по окончанию номера, сделку
   выбираем по близости даты работы к моменту проведения.
"""

import asyncio
import time
import unittest
from datetime import datetime, timedelta, timezone

from notifications.order_address import (
    contact_lead_ids,
    contact_matches_phone,
    deal_address,
    fetch_deal_address,
    phone_query,
    pick_open_deal,
    resolve_order_address,
)

MSK = timezone(timedelta(hours=3))
NOW = datetime(2026, 9, 6, 15, 0, tzinfo=MSK)

REALIZATION = 4482787
OTHER_PIPELINE = 1111111
STAGE_CONFIRMED = 41463838
STAGE_WON = 142
STAGE_LOST = 143

FIELD_ADDRESS = 18639
FIELD_ORDER_AT = 18701


def _deal(deal_id, *, pipeline=REALIZATION, status=STAGE_CONFIRMED,
          address=None, order_at=None, created_at=1_725_000_000):
    fields = []
    if address is not None:
        fields.append({"field_id": FIELD_ADDRESS, "values": [{"value": address}]})
    if order_at is not None:
        raw = order_at if isinstance(order_at, str) else str(int(order_at.timestamp()))
        fields.append({"field_id": FIELD_ORDER_AT, "values": [{"value": raw}]})
    return {
        "id": deal_id,
        "pipeline_id": pipeline,
        "status_id": status,
        "created_at": created_at,
        "custom_fields_values": fields,
    }


def _contact(contact_id, *phones, leads=()):
    fields = []
    if phones:
        fields.append({"field_code": "PHONE",
                       "values": [{"value": phone} for phone in phones]})
    return {
        "id": contact_id,
        "custom_fields_values": fields,
        "_embedded": {"leads": [{"id": lead_id} for lead_id in leads]},
    }


class PhoneQueryTests(unittest.TestCase):
    """amoCRM ищет по десяти цифрам без кода страны — иначе «+7» и «8» дают
    два разных запроса на один и тот же номер."""

    def test_plus_seven_becomes_ten_digits(self):
        self.assertEqual(phone_query("+79001234567"), "9001234567")

    def test_leading_eight_becomes_ten_digits(self):
        self.assertEqual(phone_query("89001234567"), "9001234567")

    def test_ten_digits_stay_as_is(self):
        self.assertEqual(phone_query("9001234567"), "9001234567")

    def test_formatting_is_ignored(self):
        self.assertEqual(phone_query("8 (900) 123-45-67"), "9001234567")

    def test_unusable_phone_gives_nothing(self):
        for phone in (None, "", "   ", "1234567", "+19001234567", "123456789012"):
            self.assertIsNone(phone_query(phone), phone)


class ContactMatchesPhoneTests(unittest.TestCase):
    """Поиск amoCRM — подстрочный по всем полям, а не по телефону. Контакт,
    у которого номер не тот, — чужой, каким бы путём он ни нашёлся."""

    def test_matches_by_phone_ending(self):
        contact = _contact(10, "+7 900 123-45-67")
        self.assertTrue(contact_matches_phone(contact, "9001234567"))

    def test_other_number_is_rejected(self):
        contact = _contact(10, "+79007654321")
        self.assertFalse(contact_matches_phone(contact, "9001234567"))

    def test_contact_without_phone_is_rejected(self):
        self.assertFalse(contact_matches_phone(_contact(10), "9001234567"))

    def test_any_of_several_phones_is_enough(self):
        contact = _contact(10, "+79007654321", "8-900-123-45-67")
        self.assertTrue(contact_matches_phone(contact, "9001234567"))


class ContactLeadIdsTests(unittest.TestCase):
    def test_collects_ids_in_order(self):
        self.assertEqual(contact_lead_ids(_contact(10, leads=(5, 3, 9))), [5, 3, 9])

    def test_empty_embedded_gives_nothing(self):
        self.assertEqual(contact_lead_ids({"id": 10}), [])
        self.assertEqual(contact_lead_ids({"id": 10, "_embedded": {}}), [])

    def test_garbage_entries_are_skipped(self):
        contact = {"id": 10, "_embedded": {"leads": [
            {"id": 5}, {"name": "без id"}, {"id": "abc"}, {"id": "9"},
        ]}}
        self.assertEqual(contact_lead_ids(contact), [5, 9])


class PickOpenDealTests(unittest.TestCase):
    """Сделка выбирается по дате работы, а не по свежести: у постоянного
    клиента «самая новая» на второй заказ подряд указала бы не на ту."""

    def test_other_pipeline_is_skipped(self):
        deals = [_deal(1, pipeline=OTHER_PIPELINE, order_at=NOW)]
        self.assertIsNone(pick_open_deal(deals, now=NOW))

    def test_closed_deals_are_skipped(self):
        deals = [_deal(1, status=STAGE_WON, order_at=NOW),
                 _deal(2, status=STAGE_LOST, order_at=NOW)]
        self.assertIsNone(pick_open_deal(deals, now=NOW))

    def test_nearest_date_wins_over_old_unpaid_deal(self):
        """Старая сделка «Заказ выполнен, ждёт оплаты» тоже открыта —
        но ездили не по ней."""
        old = _deal(1, order_at=NOW - timedelta(days=30), created_at=100)
        today = _deal(2, order_at=NOW - timedelta(hours=1), created_at=50)
        self.assertEqual(pick_open_deal([old, today], now=NOW)["id"], 2)

    def test_future_deal_loses_to_todays(self):
        """Заказ на следующую неделю уже заведён — сегодняшний ближе."""
        today = _deal(1, order_at=NOW - timedelta(hours=2))
        next_week = _deal(2, order_at=NOW + timedelta(days=7))
        self.assertEqual(pick_open_deal([next_week, today], now=NOW)["id"], 1)

    def test_equal_distance_prefers_past(self):
        future = _deal(1, order_at=NOW + timedelta(hours=2))
        past = _deal(2, order_at=NOW - timedelta(hours=2))
        self.assertEqual(pick_open_deal([future, past], now=NOW)["id"], 2)

    def test_dated_deal_beats_undated(self):
        undated = _deal(1, created_at=999)
        dated = _deal(2, order_at=NOW + timedelta(days=3), created_at=1)
        self.assertEqual(pick_open_deal([undated, dated], now=NOW)["id"], 2)

    def test_among_undated_newest_wins(self):
        older = _deal(1, created_at=100)
        newer = _deal(2, created_at=200)
        self.assertEqual(pick_open_deal([older, newer], now=NOW)["id"], 2)

    def test_broken_date_does_not_break_choice(self):
        """Поле правили руками: сделка считается сделкой без даты, проход не падает."""
        broken = _deal(1, order_at="abc", created_at=500)
        dated = _deal(2, order_at=NOW, created_at=1)
        self.assertEqual(pick_open_deal([broken, dated], now=NOW)["id"], 2)
        self.assertEqual(pick_open_deal([broken], now=NOW)["id"], 1)

    def test_nothing_suitable_gives_none(self):
        self.assertIsNone(pick_open_deal([], now=NOW))
        self.assertIsNone(pick_open_deal([_deal(1, pipeline=OTHER_PIPELINE)], now=NOW))


class DealAddressTests(unittest.TestCase):
    def test_address_is_stripped(self):
        deal = _deal(1, address="  Менделеева д 15а, кв 99  ")
        self.assertEqual(deal_address(deal), "Менделеева д 15а, кв 99")

    def test_empty_field_gives_none(self):
        self.assertIsNone(deal_address(_deal(1, address="")))
        self.assertIsNone(deal_address(_deal(1)))

    def test_missing_deal_gives_none(self):
        self.assertIsNone(deal_address(None))


class ResolveOrderAddressTests(unittest.TestCase):
    """Сделка → карточка → адрес последнего заказа → ничего. Календарь —
    источник правды, карточка — запасной вариант, когда CRM не ответила."""

    def test_deal_address_comes_first(self):
        self.assertEqual(
            resolve_order_address(deal_address="Из календаря",
                                  card_address="Из карточки",
                                  last_order_addr="Прошлый заказ"),
            "Из календаря")

    def test_card_when_deal_is_missing(self):
        self.assertEqual(
            resolve_order_address(deal_address=None, card_address="Из карточки",
                                  last_order_addr="Прошлый заказ"),
            "Из карточки")

    def test_last_order_address_when_card_is_empty(self):
        self.assertEqual(
            resolve_order_address(deal_address=None, card_address="",
                                  last_order_addr="Прошлый заказ"),
            "Прошлый заказ")

    def test_nothing_gives_none(self):
        self.assertIsNone(resolve_order_address(deal_address=None, card_address=None,
                                                last_order_addr=None))

    def test_blank_counts_as_empty(self):
        self.assertEqual(
            resolve_order_address(deal_address="   ", card_address=" Из карточки ",
                                  last_order_addr=None),
            "Из карточки")


class FakeAmo:
    """Клиент amoCRM с журналом вызовов: два запроса, которые делает поиск."""

    def __init__(self, *, contacts=(), deals=(), error=None, delay=0.0):
        self.contacts = list(contacts)
        self.deals = list(deals)
        self.error = error
        self.delay = delay
        self.calls = []

    async def find_contacts_by_phone(self, phone10):
        self.calls.append(("contacts", phone10))
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.error is not None:
            raise self.error
        return list(self.contacts)

    async def fetch_leads_by_ids(self, lead_ids):
        self.calls.append(("leads", list(lead_ids)))
        return list(self.deals)


class FetchDealAddressTests(unittest.IsolatedAsyncioTestCase):
    """Поход в amoCRM при проведении заказа никогда не ломает проведение."""

    async def test_returns_address_of_open_deal(self):
        amo = FakeAmo(
            contacts=[_contact(10, "+79001234567", leads=(1, 2))],
            deals=[_deal(1, status=STAGE_WON, address="Старый адрес",
                         order_at=NOW - timedelta(days=40)),
                   _deal(2, address="Менделеева д 15а, кв 99", order_at=NOW)],
        )
        address = await fetch_deal_address(amo, "+79001234567", now=NOW)
        self.assertEqual(address, "Менделеева д 15а, кв 99")
        self.assertEqual(amo.calls, [("contacts", "9001234567"), ("leads", [1, 2])])

    async def test_unparseable_phone_skips_crm(self):
        amo = FakeAmo(contacts=[_contact(10, "+79001234567", leads=(1,))])
        self.assertIsNone(await fetch_deal_address(amo, "нет телефона", now=NOW))
        self.assertEqual(amo.calls, [])

    async def test_foreign_contact_is_ignored(self):
        """Подстрочный поиск нашёл кого-то другого — его сделки не читаем."""
        amo = FakeAmo(contacts=[_contact(10, "+79007654321", leads=(1,))],
                      deals=[_deal(1, address="Чужой адрес", order_at=NOW)])
        self.assertIsNone(await fetch_deal_address(amo, "+79001234567", now=NOW))
        self.assertEqual(amo.calls, [("contacts", "9001234567")])

    async def test_contact_without_deals_stops_early(self):
        amo = FakeAmo(contacts=[_contact(10, "+79001234567")])
        self.assertIsNone(await fetch_deal_address(amo, "+79001234567", now=NOW))
        self.assertEqual(amo.calls, [("contacts", "9001234567")])

    async def test_crm_error_gives_none_and_does_not_raise(self):
        amo = FakeAmo(error=RuntimeError("amoCRM API error 500"))
        self.assertIsNone(await fetch_deal_address(amo, "+79001234567", now=NOW))

    async def test_slow_crm_is_cut_by_timeout(self):
        """Мастер ждёт «Готово ✅» — зависшая CRM не должна держать его минуту."""
        amo = FakeAmo(contacts=[_contact(10, "+79001234567", leads=(1,))],
                      deals=[_deal(1, address="Адрес", order_at=NOW)], delay=5.0)
        started = time.monotonic()
        address = await fetch_deal_address(amo, "+79001234567", now=NOW, timeout=0.05)
        self.assertIsNone(address)
        self.assertLess(time.monotonic() - started, 1.0)


if __name__ == "__main__":
    unittest.main()
