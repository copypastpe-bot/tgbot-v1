"""Правила разговора с клиентом до работы.

Два правила здесь важнее остальных, потому что нарушение любого из них робот
не заметит, а владелец заметит поздно:

1. **Непонятое не гадаем.** Робот различает ровно «да» и «нет», потому что сам
   просит ответить одним словом. Всё остальное — «перенесите на среду»,
   «не знаю» — уходит владельцу целиком: угадывать смысл дороже, чем лишнее
   сообщение.
2. **Владельца зовём один раз на заказ.** Сторож молчунов ходит раз в десять
   минут; забудь он про уже отправленный сигнал — владелец получал бы одно
   и то же сообщение каждые десять минут до самой работы.
"""

import unittest
from datetime import datetime, timedelta, timezone

from notifications.client_messaging import (
    AMO_PIPELINE_REALIZATION,
    AMO_STAGE_CONFIRMED,
    AMO_STAGE_ORDER_CREATED,
    ASK_BEFORE,
    SILENCE_LIMIT,
    child_deal_id,
    decide_on_answer,
    letter_payload,
    order_from_lead,
    pick_realization_deal,
    owner_alert_text,
    parse_answer,
    plan_confirmation,
    should_call_owner,
    should_move_deal,
)

MSK = timezone(timedelta(hours=3))


class ParseAnswerTests(unittest.TestCase):
    def test_yes_variants(self):
        for text in ("Да", "да", "ДА", "да ", "да!", "Да, жду"):
            self.assertEqual(parse_answer(text), "yes", text)

    def test_no_variants(self):
        for text in ("Нет", "нет", "НЕТ", "нет!", "Нет, отмена"):
            self.assertEqual(parse_answer(text), "no", text)

    def test_anything_else_is_unclear(self):
        """Непонятое не гадаем — оно уходит владельцу."""
        for text in ("перенесите на среду", "5", "", "ok", "давайте"):
            self.assertEqual(parse_answer(text), "unclear", text)

    def test_uncertainty_is_not_a_refusal(self):
        """«Не знаю» — это не отказ, а разговор: им занимается владелец.

        Разница видна в отчёте: посчитай робот такое отказом, владелец увидел бы
        завышенное число отказавшихся клиентов и стал бы искать несуществующую
        проблему.
        """
        for text in ("не знаю", "не уверен", "не смогу сказать"):
            self.assertEqual(parse_answer(text), "unclear", text)

    def test_stop_is_not_an_answer(self):
        """STOP — отписка, ей занимается другая ветка."""
        self.assertEqual(parse_answer("STOP"), "unclear")

    def test_missing_text_is_unclear(self):
        self.assertEqual(parse_answer(None), "unclear")


class PlanConfirmationTests(unittest.TestCase):
    NOW = datetime(2026, 8, 28, 12, 0, tzinfo=MSK)

    def test_question_is_scheduled_a_day_before(self):
        order_at = self.NOW + timedelta(days=3)
        plan = plan_confirmation(order_at=order_at, now=self.NOW)
        self.assertTrue(plan.send_confirmation)
        self.assertEqual(plan.ask_at, order_at - ASK_BEFORE)

    def test_urgent_order_gets_no_question(self):
        """Заказ меньше чем за сутки: только подтверждение, вопроса нет."""
        plan = plan_confirmation(order_at=self.NOW + timedelta(hours=5), now=self.NOW)
        self.assertTrue(plan.send_confirmation)
        self.assertIsNone(plan.ask_at)

    def test_order_exactly_a_day_ahead_gets_no_question(self):
        """Ровно сутки: спрашивать уже поздно — вопрос ушёл бы этой же секундой."""
        plan = plan_confirmation(order_at=self.NOW + ASK_BEFORE, now=self.NOW)
        self.assertTrue(plan.send_confirmation)
        self.assertIsNone(plan.ask_at)

    def test_order_without_date_gets_nothing(self):
        plan = plan_confirmation(order_at=None, now=self.NOW)
        self.assertFalse(plan.send_confirmation)
        self.assertIsNone(plan.ask_at)

    def test_past_order_gets_nothing(self):
        """Сделку могли оформить задним числом — писать про вчера незачем."""
        plan = plan_confirmation(order_at=self.NOW - timedelta(hours=1), now=self.NOW)
        self.assertFalse(plan.send_confirmation)

    def test_every_decision_explains_itself(self):
        """Причина попадает в журнал: без неё репетицию не проверить."""
        for order_at in (None, self.NOW - timedelta(hours=1),
                         self.NOW + timedelta(hours=5), self.NOW + timedelta(days=3)):
            self.assertTrue(plan_confirmation(order_at=order_at, now=self.NOW).reason)


class SilenceTests(unittest.TestCase):
    NOW = datetime(2026, 8, 28, 12, 0, tzinfo=MSK)

    def test_owner_is_called_after_three_hours(self):
        asked_at = self.NOW - SILENCE_LIMIT - timedelta(minutes=1)
        self.assertTrue(should_call_owner(asked_at=asked_at, notified_at=None,
                                          now=self.NOW))

    def test_owner_is_not_called_too_early(self):
        asked_at = self.NOW - timedelta(hours=1)
        self.assertFalse(should_call_owner(asked_at=asked_at, notified_at=None,
                                           now=self.NOW))

    def test_owner_is_called_only_once(self):
        asked_at = self.NOW - timedelta(hours=5)
        self.assertFalse(should_call_owner(asked_at=asked_at,
                                           notified_at=self.NOW - timedelta(hours=1),
                                           now=self.NOW))

    def test_unasked_order_never_calls_owner(self):
        """Вопрос ещё не ушёл — молчания нет, звать не о чем."""
        self.assertFalse(should_call_owner(asked_at=None, notified_at=None,
                                           now=self.NOW))


class ChildDealTests(unittest.TestCase):
    """Связь лид → сделка реализации. Проверено на живых данных 2026-08-28:
    робот владельца заводит сделку той же минутой, а amoCRM оставляет в лиде
    примечание `lead_auto_created` со ссылкой на неё."""

    def _note(self, deal_id, created_at=100):
        return {"note_type": "lead_auto_created", "created_at": created_at,
                "params": {"type": "child", "lead_type": "child",
                           "lead_id": deal_id, "link": {"id": deal_id, "type": 2}}}

    def test_finds_child_deal(self):
        self.assertEqual(child_deal_id([self._note(31570357)]), 31570357)

    def test_ignores_other_notes(self):
        notes = [{"note_type": "call_out", "params": {"duration": 9}},
                 {"note_type": "amomail_message", "params": {}},
                 self._note(31570357)]
        self.assertEqual(child_deal_id(notes), 31570357)

    def test_takes_the_freshest_link(self):
        """Заказ могли завести дважды — двигать надо последнюю сделку."""
        notes = [self._note(111, created_at=100), self._note(222, created_at=200)]
        self.assertEqual(child_deal_id(notes), 222)

    def test_no_link_means_no_guess(self):
        self.assertIsNone(child_deal_id([]))
        self.assertIsNone(child_deal_id([{"note_type": "call_in", "params": {}}]))


class PickRealizationDealTests(unittest.TestCase):
    """Запасной путь, когда ссылки в примечании нет: открытая сделка с той же
    датой работы."""

    def _deal(self, deal_id, order_at_raw, status_id=AMO_STAGE_ORDER_CREATED,
              pipeline_id=AMO_PIPELINE_REALIZATION):
        return {"id": deal_id, "pipeline_id": pipeline_id, "status_id": status_id,
                "custom_fields_values": [{"field_id": 18701,
                                          "values": [{"value": order_at_raw}]}]}

    def test_matches_by_order_datetime(self):
        deals = [self._deal(1, "1787000000"), self._deal(2, "1788174000")]
        self.assertEqual(pick_realization_deal(deals, order_at_raw="1788174000"), 2)

    def test_ignores_deals_that_moved_on(self):
        """Сделка уже подтверждена — значит это чужой, более ранний заказ."""
        deals = [self._deal(1, "1788174000", status_id=AMO_STAGE_CONFIRMED)]
        self.assertIsNone(pick_realization_deal(deals, order_at_raw="1788174000"))

    def test_ignores_other_pipelines(self):
        deals = [self._deal(1, "1788174000", pipeline_id=4482751)]
        self.assertIsNone(pick_realization_deal(deals, order_at_raw="1788174000"))

    def test_without_date_nothing_is_guessed(self):
        """Нет даты — нет признака. Пусть сделку подвинет человек."""
        deals = [self._deal(1, "1788174000")]
        self.assertIsNone(pick_realization_deal(deals, order_at_raw=None))

    def test_foreign_contact_is_skipped(self):
        deal = self._deal(1, "1788174000")
        deal["_embedded"] = {"contacts": [{"id": 999}]}
        self.assertIsNone(pick_realization_deal(deals=[deal],
                                                order_at_raw="1788174000",
                                                contact_ids={37926153}))


class OrderFromLeadTests(unittest.TestCase):
    def _lead(self, *fields):
        return {"custom_fields_values": list(fields)}

    def test_reads_datetime_and_address(self):
        order = order_from_lead(self._lead(
            {"field_id": 18701, "values": [{"value": 1788174000}]},
            {"field_id": 18639, "values": [{"value": "Панина 7к2"}]},
        ), tz=MSK)
        self.assertEqual(order.address, "Панина 7к2")
        self.assertEqual(order.order_at,
                         datetime.fromtimestamp(1788174000, tz=MSK))

    def test_missing_fields_are_empty(self):
        order = order_from_lead(self._lead(), tz=MSK)
        self.assertIsNone(order.order_at)
        self.assertIsNone(order.address)

    def test_broken_datetime_does_not_crash(self):
        """Поле могли заполнить руками: письмо не уйдёт, но проход не упадёт."""
        order = order_from_lead(self._lead(
            {"field_id": 18701, "values": [{"value": "завтра днём"}]},
        ), tz=MSK)
        self.assertIsNone(order.order_at)

    def test_letter_payload_is_human_readable(self):
        payload = letter_payload(
            order_at=datetime(2026, 8, 31, 14, 0, tzinfo=MSK),
            address="Панина 7к2")
        self.assertEqual(payload["date"], "31.08.2026")
        self.assertEqual(payload["time"], "14:00")
        self.assertEqual(payload["address"], "Панина 7к2")


class DecideOnAnswerTests(unittest.TestCase):
    def test_yes_confirms_the_order(self):
        self.assertEqual(decide_on_answer(answer="yes", status="planned"), "confirm")

    def test_late_yes_still_confirms(self):
        """Решение владельца 7: «Да» после сигнала всё равно двигает сделку."""
        self.assertEqual(decide_on_answer(answer="yes", status="owner_notified"),
                         "confirm")

    def test_no_calls_owner(self):
        self.assertEqual(decide_on_answer(answer="no", status="planned"), "call_owner")

    def test_unclear_calls_owner(self):
        self.assertEqual(decide_on_answer(answer="unclear", status="planned"),
                         "call_owner")

    def test_owner_is_not_called_twice_about_one_order(self):
        """Владельца уже позвали — дальше он разговаривает сам."""
        for answer in ("no", "unclear"):
            self.assertEqual(decide_on_answer(answer=answer, status="owner_notified"),
                             "ignore", answer)

    def test_settled_order_ignores_anything(self):
        for status in ("confirmed", "refused"):
            self.assertEqual(decide_on_answer(answer="yes", status=status),
                             "ignore", status)


class MoveDealTests(unittest.TestCase):
    def test_deal_moves_only_from_order_created(self):
        self.assertTrue(should_move_deal(AMO_STAGE_ORDER_CREATED))

    def test_deal_that_went_further_is_not_touched(self):
        """Сделку мог двинуть человек: робот не откатывает чужую работу."""
        self.assertFalse(should_move_deal(AMO_STAGE_CONFIRMED))
        self.assertFalse(should_move_deal(142))
        self.assertFalse(should_move_deal(None))


class OwnerAlertTests(unittest.TestCase):
    ORDER_AT = datetime(2026, 8, 31, 14, 0, tzinfo=MSK)

    def _text(self, kind, answer_text="перенесите на среду"):
        return owner_alert_text(
            kind=kind,
            name="Дарья",
            phone="+79001234567",
            order_at=self.ORDER_AT,
            answer_text=answer_text,
            lead_link="https://example.amocrm.ru/leads/detail/123",
        )

    def test_owner_sees_full_phone(self):
        """Бот приватный, владелец — единственный получатель, и ему нужно
        позвонить клиенту, не открывая CRM (его решение 2026-08-26)."""
        self.assertIn("+79001234567", self._text("refused"))

    def test_owner_sees_answer_in_full(self):
        """Непонятое не пересказываем: владелец читает слова клиента сам."""
        text = self._text("unclear", answer_text="а можно перенести на среду вечером?")
        self.assertIn("а можно перенести на среду вечером?", text)

    def test_owner_sees_order_date_and_link(self):
        text = self._text("refused")
        self.assertIn("31.08.2026", text)
        self.assertIn("14:00", text)
        self.assertIn("https://example.amocrm.ru/leads/detail/123", text)

    def test_silence_alert_says_what_happened(self):
        text = owner_alert_text(kind="silence", name="Дарья", phone="+79001234567",
                                order_at=self.ORDER_AT, answer_text=None,
                                lead_link=None)
        self.assertIn("не ответил", text.lower())
        self.assertIn("+79001234567", text)

    def test_kinds_are_distinguishable(self):
        """Владелец должен с первой строки понимать, что случилось."""
        heads = {self._text(kind).splitlines()[0]
                 for kind in ("refused", "unclear", "silence")}
        self.assertEqual(len(heads), 3)


if __name__ == "__main__":
    unittest.main()
