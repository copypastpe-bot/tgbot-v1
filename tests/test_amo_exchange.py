"""Правила автообмена amoCRM → база бота.

Два правила здесь важнее остальных, потому что нарушение любого из них робот
не заметит, а владелец заметит поздно:

1. **Понижения статуса не бывает.** Автомат, честно применяющий «отказ → лид»,
   за месяц тихо разжалует половину постоянных клиентов.
2. **Отказная сделка известного человека не меняет ничего.** Постоянный клиент
   узнал цену и не заказал — это не повод портить его карточку.

И третье, про направление данных: бонусы и день рождения рождаются в боте,
обмен их не пишет никогда — иначе затрёт свежее старым.
"""

import unittest
from datetime import datetime, timedelta

from notifications.amo_exchange import (
    ExistingClient,
    IncomingClient,
    decide_exchange,
    incoming_from_amo,
    is_weekly_leads_day,
    outcome_of_event,
)


class DecideExchangeTests(unittest.TestCase):
    def _incoming(self, **kwargs):
        base = dict(phone="+79001234567", digits="79001234567",
                    name="Дарья", address="Панина 7к2", service="Чистка мебели")
        base.update(kwargs)
        return IncomingClient(**base)

    def test_lost_deal_creates_lead_when_unknown(self):
        decision = decide_exchange(outcome="lost", incoming=self._incoming(),
                                   existing=None)
        self.assertEqual(decision.action, "create_lead")
        self.assertEqual(decision.fields["status"], "lead")

    def test_lost_deal_never_touches_existing_client(self):
        """Постоянный клиент узнал цену и не заказал — карточку не трогаем."""
        existing = ExistingClient(client_id=7, status="client", name="Дарья",
                                  address=None, service=None)
        decision = decide_exchange(outcome="lost", incoming=self._incoming(),
                                   existing=existing)
        self.assertEqual(decision.action, "skip")

    def test_lost_deal_never_touches_existing_lead(self):
        existing = ExistingClient(client_id=7, status="lead", name=None,
                                  address=None, service=None)
        decision = decide_exchange(outcome="lost", incoming=self._incoming(),
                                   existing=existing)
        self.assertEqual(decision.action, "skip")

    def test_won_deal_creates_client_when_unknown(self):
        decision = decide_exchange(outcome="won", incoming=self._incoming(),
                                   existing=None)
        self.assertEqual(decision.action, "create_client")
        self.assertEqual(decision.fields["status"], "client")
        self.assertEqual(decision.fields["last_order_addr"], "Панина 7к2")
        self.assertEqual(decision.fields["last_service"], "Чистка мебели")

    def test_won_deal_promotes_lead_to_client(self):
        existing = ExistingClient(client_id=7, status="lead", name=None,
                                  address=None, service=None)
        decision = decide_exchange(outcome="won", incoming=self._incoming(),
                                   existing=existing)
        self.assertEqual(decision.action, "update")
        self.assertEqual(decision.fields["status"], "client")

    def test_client_is_never_demoted(self):
        """Понижения не бывает: статус в правках даже не появляется."""
        existing = ExistingClient(client_id=7, status="client", name="Дарья",
                                  address="Старый адрес", service="Уборка")
        decision = decide_exchange(outcome="won", incoming=self._incoming(),
                                   existing=existing)
        self.assertNotIn("status", decision.fields)

    def test_name_filled_only_when_empty(self):
        existing = ExistingClient(client_id=7, status="client", name="Своё имя",
                                  address=None, service=None)
        decision = decide_exchange(outcome="won", incoming=self._incoming(),
                                   existing=existing)
        self.assertNotIn("full_name", decision.fields)

    def test_name_is_filled_when_missing(self):
        existing = ExistingClient(client_id=7, status="client", name=None,
                                  address="Панина 7к2", service="Чистка мебели")
        decision = decide_exchange(outcome="won", incoming=self._incoming(),
                                   existing=existing)
        self.assertEqual(decision.fields["full_name"], "Дарья")

    def test_address_filled_only_when_empty(self):
        """Адрес заказа дописываем, чужой не переписываем (правила обмена)."""
        existing = ExistingClient(client_id=7, status="client", name="Дарья",
                                  address="Старый адрес", service=None)
        decision = decide_exchange(outcome="won", incoming=self._incoming(),
                                   existing=existing)
        self.assertNotIn("last_order_addr", decision.fields)

    def test_service_is_always_refreshed(self):
        existing = ExistingClient(client_id=7, status="client", name="Дарья",
                                  address="Панина 7к2", service="Уборка")
        decision = decide_exchange(outcome="won", incoming=self._incoming(),
                                   existing=existing)
        self.assertEqual(decision.fields["last_service"], "Чистка мебели")

    def test_nothing_to_change_is_skipped(self):
        existing = ExistingClient(client_id=7, status="client", name="Дарья",
                                  address="Панина 7к2", service="Чистка мебели")
        decision = decide_exchange(outcome="won", incoming=self._incoming(),
                                   existing=existing)
        self.assertEqual(decision.action, "skip")

    def test_deal_without_phone_is_skipped(self):
        decision = decide_exchange(outcome="won",
                                   incoming=self._incoming(phone=None, digits=None),
                                   existing=None)
        self.assertEqual(decision.action, "skip")
        self.assertIn("телефон", decision.reason)

    def test_bonuses_and_birthday_are_never_written(self):
        """Бонусы и день рождения ведёт бот — обмен их не трогает никогда."""
        decision = decide_exchange(outcome="won", incoming=self._incoming(),
                                   existing=None)
        self.assertNotIn("bonus_balance", decision.fields)
        self.assertNotIn("birthday", decision.fields)

    def test_lead_gets_no_order_fields(self):
        """Человек ничего не заказывал — адресу и услуге в его карточке не место."""
        decision = decide_exchange(outcome="lost", incoming=self._incoming(),
                                   existing=None)
        self.assertNotIn("last_order_addr", decision.fields)
        self.assertNotIn("last_service", decision.fields)


def _normalize_phone(raw: str):
    """Заглушка нормализации для тестов.

    Настоящая живёт в bot.py и передаётся разбору параметром: канон телефонов
    в проекте один, а тесты по здешнему обычаю не тянут bot.py целиком.
    """
    digits = "".join(ch for ch in raw or "" if ch.isdigit())
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    if len(digits) == 10:
        digits = "7" + digits
    return ("+" + digits, digits) if digits else (None, None)


class ParseAmoTests(unittest.TestCase):
    LEAD = {
        "id": 31570885,
        "pipeline_id": 4482751,
        "status_id": 142,
        "custom_fields_values": [
            {"field_id": 18639, "values": [{"value": "Панина 7к2"}]},
            {"field_id": 271915, "values": [{"value": "Чистка мебели"}]},
        ],
    }
    CONTACT = {
        "id": 55,
        "name": "Дарья",
        "custom_fields_values": [
            {"field_code": "PHONE", "values": [{"value": "8 (900) 123-45-67"}]},
        ],
    }

    def _parse(self, lead=None, contact=None):
        return incoming_from_amo(lead or self.LEAD,
                                 self.CONTACT if contact is None else contact,
                                 normalize_phone=_normalize_phone)

    def test_reads_phone_name_address_and_service(self):
        incoming = self._parse()
        self.assertEqual(incoming.digits, "79001234567")
        self.assertEqual(incoming.phone, "+79001234567")
        self.assertEqual(incoming.name, "Дарья")
        self.assertEqual(incoming.address, "Панина 7к2")
        self.assertEqual(incoming.service, "Чистка мебели")

    def test_multiselect_services_are_joined(self):
        lead = {**self.LEAD, "custom_fields_values": [
            {"field_id": 271915, "values": [{"value": "Чистка мебели"},
                                            {"value": "Мойка окон"}]},
        ]}
        self.assertEqual(self._parse(lead).service, "Чистка мебели, Мойка окон")

    def test_contact_without_phone_gives_empty_digits(self):
        incoming = self._parse(contact={"id": 55, "name": "Дарья"})
        self.assertIsNone(incoming.digits)

    def test_missing_contact_does_not_crash(self):
        """Сделка без контакта — обычное дело, разбор не должен падать."""
        incoming = self._parse(contact={})
        self.assertIsNone(incoming.digits)
        self.assertIsNone(incoming.name)

    def test_empty_fields_become_none(self):
        lead = {**self.LEAD, "custom_fields_values": [
            {"field_id": 18639, "values": [{"value": ""}]},
        ]}
        incoming = self._parse(lead)
        self.assertIsNone(incoming.address)
        self.assertIsNone(incoming.service)


class OutcomeFromEventTests(unittest.TestCase):
    """Разбор события смены этапа.

    Форма события взята с живого аккаунта 2026-08-28: amoCRM кладёт в
    `value_after` новый статус вместе с воронкой, поэтому чужие сделки видно
    сразу, без запроса карточки.
    """

    def _event(self, status_id, pipeline_id=4482751):
        return {
            "id": "abc",
            "type": "lead_status_changed",
            "entity_id": 31570885,
            "value_after": [{"lead_status": {"id": status_id,
                                             "pipeline_id": pipeline_id}}],
        }

    def test_won_is_recognised(self):
        self.assertEqual(outcome_of_event(self._event(142)), "won")

    def test_lost_is_recognised(self):
        self.assertEqual(outcome_of_event(self._event(143)), "lost")

    def test_other_pipeline_is_ignored(self):
        self.assertIsNone(outcome_of_event(self._event(142, pipeline_id=7108250)))

    def test_intermediate_status_is_ignored(self):
        self.assertIsNone(outcome_of_event(self._event(41463535)))

    def test_event_without_value_after_is_ignored(self):
        self.assertIsNone(outcome_of_event({"id": "abc", "entity_id": 1}))

    def test_broken_event_does_not_crash(self):
        self.assertIsNone(outcome_of_event({"value_after": [None, {}]}))


class WeeklyLeadsDayTests(unittest.TestCase):
    """Отказные заводятся пачкой по понедельникам — решение владельца."""

    def test_monday_is_the_day(self):
        self.assertTrue(is_weekly_leads_day(datetime(2026, 8, 31)))   # понедельник

    def test_other_days_are_not(self):
        for day in range(1, 7):                                       # вт–вс
            moment = datetime(2026, 8, 31) + timedelta(days=day)
            self.assertFalse(is_weekly_leads_day(moment), moment.strftime("%A"))


if __name__ == "__main__":
    unittest.main()
