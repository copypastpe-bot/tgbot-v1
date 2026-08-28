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

from notifications.amo_exchange import (
    ExistingClient,
    IncomingClient,
    decide_exchange,
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


if __name__ == "__main__":
    unittest.main()
