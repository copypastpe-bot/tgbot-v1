import json
import unittest
from decimal import Decimal
from pathlib import Path

from cleaning.constants import (
    CLEANING_GIFT_CERT_LABEL,
    CLEANING_PAYMENT_METHODS,
)
from cleaning.orders import (
    PaymentPart,
    calculate_bonus_earned,
    calculate_bonus_max,
    cashbook_rows_from_payments,
    get_min_order_amount,
    is_wire_payment_method,
    parse_amount,
    payments_balance_diff,
    validate_payment_parts,
)
from cleaning.format import (
    format_dividend_alert,
    format_order_provided_alert,
)


D = Decimal


class PaymentHelpersTests(unittest.TestCase):
    def test_parse_amount_accepts_spaces_and_comma(self):
        self.assertEqual(parse_amount("1 234,50"), D("1234.50"))
        self.assertEqual(parse_amount("1234.5"), D("1234.5"))
        self.assertEqual(parse_amount("0"), D("0"))

    def test_parse_amount_rejects_garbage(self):
        self.assertIsNone(parse_amount("abc"))
        self.assertIsNone(parse_amount(""))
        self.assertIsNone(parse_amount("-100"))  # отрицательные не принимаем

    def test_payments_balance_diff_zero_when_matches(self):
        parts = [
            PaymentPart(method="Наличные", amount=D("3000")),
            PaymentPart(method="Карта", amount=D("2000")),
        ]
        # сумма чека 5500, списано бонусов 500 → ожидаем 5000 оплатой
        self.assertEqual(payments_balance_diff(parts, total=D("5500"), bonuses_used=D("500")), D("0"))

    def test_payments_balance_diff_returns_signed_delta(self):
        parts = [PaymentPart(method="Наличные", amount=D("4500"))]
        # ожидалось 5000, оплачено 4500 → недостача -500
        self.assertEqual(payments_balance_diff(parts, total=D("5000"), bonuses_used=D("0")), D("-500"))

        parts = [PaymentPart(method="Наличные", amount=D("5500"))]
        # переплата +500
        self.assertEqual(payments_balance_diff(parts, total=D("5000"), bonuses_used=D("0")), D("500"))

    def test_cashbook_rows_skip_gift_certificate(self):
        parts = [
            PaymentPart(method="Наличные", amount=D("3000")),
            PaymentPart(method=CLEANING_GIFT_CERT_LABEL, amount=D("2000")),
            PaymentPart(method="Карта", amount=D("1000")),
        ]
        rows = cashbook_rows_from_payments(parts)
        # сертификат вырезан, остальные — попадают
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0], ("Наличные", D("3000")))
        self.assertEqual(rows[1], ("Карта", D("1000")))

    def test_cashbook_rows_all_gift_cert_returns_empty(self):
        parts = [PaymentPart(method=CLEANING_GIFT_CERT_LABEL, amount=D("5000"))]
        self.assertEqual(cashbook_rows_from_payments(parts), [])

    def test_cashbook_rows_drops_zero_amounts(self):
        parts = [
            PaymentPart(method="Наличные", amount=D("0")),
            PaymentPart(method="Карта", amount=D("100")),
        ]
        rows = cashbook_rows_from_payments(parts)
        self.assertEqual(rows, [("Карта", D("100"))])


class AlertFormatterTests(unittest.TestCase):
    def test_format_order_alert_contains_key_lines(self):
        text = format_order_provided_alert(
            order_id=42,
            foreman_name="Петров",
            client_phone="+7 904 000 00 00",
            client_name="Иванов",
            address="ул. Ленина 10",
            total_amount=D("12000"),
            payments=[
                ("Наличные", D("10000")),
                (CLEANING_GIFT_CERT_LABEL, D("2000")),
            ],
            expenses=[
                ("Химия", D("800")),
                ("ЗП клинеров", D("5000")),
                ("ГСМ", D("400")),
            ],
            bonuses_used=D("0"),
            bonuses_earned=D("600"),
            profit=D("5800"),
            balance_after=D("87540"),
        )
        self.assertIn("Уборка проведена", text)
        self.assertIn("Петров", text)
        self.assertIn("+7 904 000 00 00", text)
        self.assertIn("Иванов", text)
        self.assertIn("ул. Ленина 10", text)
        self.assertIn("12 000", text)
        self.assertIn("Наличные", text)
        self.assertIn("Подарочный сертификат", text)
        self.assertIn("Химия", text)
        self.assertIn("ЗП клинеров", text)
        self.assertIn("ГСМ", text)
        self.assertIn("600", text)         # начислено бонусов
        self.assertIn("5 800", text)       # прибыль
        self.assertIn("87 540", text)      # баланс

    def test_format_dividend_alert(self):
        text = format_dividend_alert(
            amount=D("25000"),
            recipient="Иван",
            balance_after=D("62540"),
        )
        self.assertIn("DIV", text)
        self.assertIn("25 000", text)
        self.assertIn("Иван", text)
        self.assertIn("62 540", text)


class ConstantsContractTests(unittest.TestCase):
    def test_gift_cert_label_not_in_cashbook_methods(self):
        # Защита: сертификат не должен случайно попасть в список «обычных» методов
        self.assertNotIn(CLEANING_GIFT_CERT_LABEL, CLEANING_PAYMENT_METHODS)


class CleaningBusinessRulesTests(unittest.TestCase):
    def test_min_order_amount_default_and_env_override(self):
        self.assertEqual(get_min_order_amount({}), D("5500"))
        self.assertEqual(
            get_min_order_amount({"CLEANING_MIN_ORDER_AMOUNT": "6200"}),
            D("6200"),
        )

    def test_bonus_max_matches_rate_balance_and_min_cash(self):
        env = {
            "MAX_BONUS_SPEND_RATE_PERCENT": "50",
            "MIN_CASH": "2500",
        }
        self.assertEqual(calculate_bonus_max(D("10000"), 9000, env), D("5000"))
        self.assertEqual(calculate_bonus_max(D("6000"), 9000, env), D("3000"))
        self.assertEqual(calculate_bonus_max(D("6000"), 1000, env), D("1000"))

    def test_wire_payment_has_no_bonus_accrual(self):
        env = {"BONUS_RATE_PERCENT": "5"}
        self.assertEqual(calculate_bonus_earned("Расчётный", D("10000"), env), 0)
        self.assertEqual(calculate_bonus_earned("Наличные", D("10000"), env), 500)

    def test_wire_payment_cannot_be_mixed(self):
        self.assertTrue(is_wire_payment_method("Расчётный"))
        parts = [
            PaymentPart("Расчётный", D("5000")),
            PaymentPart("Карта", D("5000")),
        ]
        ok, error = validate_payment_parts(parts, D("10000"), D("0"))
        self.assertFalse(ok)
        self.assertIn("Расчётный", error)

    def test_non_wire_payment_parts_must_match_due_amount(self):
        parts = [
            PaymentPart("Наличные", D("3000")),
            PaymentPart("Карта", D("2000")),
        ]
        ok, error = validate_payment_parts(parts, D("6000"), D("1000"))
        self.assertTrue(ok)
        self.assertEqual(error, "")

        ok, error = validate_payment_parts(parts, D("7000"), D("1000"))
        self.assertFalse(ok)
        self.assertIn("не сходится", error)


class CleaningNotificationRulesTests(unittest.TestCase):
    def test_cleaning_notification_events_are_defined(self):
        raw = json.loads(Path("docs/notification_rules.json").read_text(encoding="utf-8"))
        events = {event["key"]: event for event in raw["events"]}
        self.assertEqual(
            events["cleaning_order_completed_summary"]["timing"]["delay_minutes"],
            0,
        )
        self.assertEqual(
            events["cleaning_order_completed_wire"]["timing"]["delay_minutes"],
            0,
        )
        self.assertEqual(
            events["cleaning_order_rating_reminder"]["timing"]["delay_minutes"],
            1440,
        )


if __name__ == "__main__":
    unittest.main()
