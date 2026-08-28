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
    ASK_BEFORE,
    SILENCE_LIMIT,
    parse_answer,
    plan_confirmation,
    should_call_owner,
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


if __name__ == "__main__":
    unittest.main()
