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
    decide_on_answer,
    is_order_created_event,
    letter_payload,
    order_from_lead,
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


class OrderCreatedEventTests(unittest.TestCase):
    def _event(self, pipeline_id, status_id):
        return {"value_after": [{"lead_status": {"id": status_id,
                                                 "pipeline_id": pipeline_id}}]}

    def test_order_created_in_realization_is_ours(self):
        self.assertTrue(is_order_created_event(
            self._event(AMO_PIPELINE_REALIZATION, AMO_STAGE_ORDER_CREATED)))

    def test_other_stage_is_not_ours(self):
        self.assertFalse(is_order_created_event(
            self._event(AMO_PIPELINE_REALIZATION, AMO_STAGE_CONFIRMED)))

    def test_same_stage_number_in_another_pipeline_is_not_ours(self):
        """Номера этапов в разных воронках свои: без проверки воронки робот
        писал бы клиентам чужих сделок."""
        self.assertFalse(is_order_created_event(
            self._event(4482751, AMO_STAGE_ORDER_CREATED)))

    def test_event_without_status_is_not_ours(self):
        self.assertFalse(is_order_created_event({}))
        self.assertFalse(is_order_created_event({"value_after": [{}]}))


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
