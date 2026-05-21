import unittest
from decimal import Decimal

from cleaning.constants import (
    CLEANING_GIFT_CERT_LABEL,
    CLEANING_PAYMENT_METHODS,
)
from cleaning.orders import (
    PaymentPart,
    cashbook_rows_from_payments,
    parse_amount,
    payments_balance_diff,
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


if __name__ == "__main__":
    unittest.main()
