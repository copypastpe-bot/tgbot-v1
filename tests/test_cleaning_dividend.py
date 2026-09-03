"""Правила выплаты прибыли из кассы клининга.

Живые деньги: клинер выдаёт наличные троим получателям поровну. Проверяем
ровно то, что может стоить денег — хватает ли кассы, делится ли сумма и что
именно уходит в кассовый чат.
"""

import unittest
from decimal import Decimal

from cleaning.dividend import (
    DEFAULT_RECIPIENTS,
    PAYOUT_BAD_AMOUNT,
    PAYOUT_NOT_DIVISIBLE,
    PAYOUT_NOT_ENOUGH,
    PAYOUT_OK,
    check_payout,
    largest_divisible_not_above,
    parse_recipients,
    split_equally,
)
from cleaning.format import (
    format_dividend_payout_alert,
    format_dividend_payout_confirm,
    format_dividend_cancel_alert,
)


class RecipientsTests(unittest.TestCase):
    def test_default_when_setting_is_empty(self):
        self.assertEqual(parse_recipients(""), list(DEFAULT_RECIPIENTS))
        self.assertEqual(parse_recipients(None), list(DEFAULT_RECIPIENTS))

    def test_reads_names_from_setting(self):
        self.assertEqual(parse_recipients("Оля, Дима ,Женя"), ["Оля", "Дима", "Женя"])

    def test_drops_empty_pieces(self):
        self.assertEqual(parse_recipients("Оля,,Дима,"), ["Оля", "Дима"])


class SplitTests(unittest.TestCase):
    def test_splits_equally(self):
        self.assertEqual(
            split_equally(Decimal("9999"), 3),
            [Decimal("3333"), Decimal("3333"), Decimal("3333")],
        )

    def test_splits_kopecks_when_they_divide_exactly(self):
        self.assertEqual(
            split_equally(Decimal("9999.99"), 3),
            [Decimal("3333.33")] * 3,
        )

    def test_refuses_to_split_with_a_remainder(self):
        """Копейка, потерянная при делении, — это расхождение кассы с чатом."""
        self.assertIsNone(split_equally(Decimal("10000"), 3))

    def test_hint_is_in_whole_roubles(self):
        """Подсказка идёт на руки наличными — копейки в ней бесполезны."""
        self.assertEqual(largest_divisible_not_above(Decimal("10000"), 3), Decimal("9999"))
        self.assertEqual(largest_divisible_not_above(Decimal("10000.50"), 3), Decimal("9999"))
        self.assertEqual(largest_divisible_not_above(Decimal("9999"), 3), Decimal("9999"))
        self.assertEqual(largest_divisible_not_above(Decimal("0.01"), 3), Decimal("0"))


class CheckPayoutTests(unittest.TestCase):
    def test_zero_and_negative_are_rejected(self):
        for bad in (Decimal("0"), Decimal("-100")):
            status, shares = check_payout(
                amount=bad, balance=Decimal("10000"), recipients=["Оля", "Дима", "Женя"]
            )
            self.assertEqual(status, PAYOUT_BAD_AMOUNT)
            self.assertEqual(shares, [])

    def test_not_enough_money_in_the_till(self):
        status, _ = check_payout(
            amount=Decimal("20000"),
            balance=Decimal("13793"),
            recipients=["Оля", "Дима", "Женя"],
        )
        self.assertEqual(status, PAYOUT_NOT_ENOUGH)

    def test_shortage_is_reported_before_divisibility(self):
        """Сказать «не делится» про сумму, которой всё равно нет, — сбить с толку."""
        status, _ = check_payout(
            amount=Decimal("20000"),
            balance=Decimal("13793"),
            recipients=["Оля", "Дима", "Женя"],
        )
        self.assertEqual(status, PAYOUT_NOT_ENOUGH)

    def test_whole_till_may_be_paid_out(self):
        status, shares = check_payout(
            amount=Decimal("9999"),
            balance=Decimal("9999"),
            recipients=["Оля", "Дима", "Женя"],
        )
        self.assertEqual(status, PAYOUT_OK)
        self.assertEqual(shares, [Decimal("3333")] * 3)

    def test_amount_that_does_not_divide(self):
        status, shares = check_payout(
            amount=Decimal("10000"),
            balance=Decimal("13793"),
            recipients=["Оля", "Дима", "Женя"],
        )
        self.assertEqual(status, PAYOUT_NOT_DIVISIBLE)
        self.assertEqual(shares, [])

    def test_two_recipients_divide_in_half(self):
        """Состав получателей — настройка, правило деления от их числа не зависит."""
        status, shares = check_payout(
            amount=Decimal("1000"), balance=Decimal("5000"), recipients=["Оля", "Женя"]
        )
        self.assertEqual(status, PAYOUT_OK)
        self.assertEqual(shares, [Decimal("500"), Decimal("500")])


class PayoutMessagesTests(unittest.TestCase):
    def test_chat_alert_matches_the_agreed_shape(self):
        text = format_dividend_payout_alert(
            recipients=["Оля", "Дима", "Женя"],
            shares=[Decimal("3333")] * 3,
            balance_after=Decimal("10460"),
        )
        self.assertEqual(
            text,
            "Выплата прибыли:\n"
            "Оля — 3 333₽\n"
            "Дима — 3 333₽\n"
            "Женя — 3 333₽\n"
            "Остаток в кассе: 10 460₽",
        )

    def test_confirm_shows_the_split_and_what_is_left(self):
        text = format_dividend_payout_confirm(
            total=Decimal("9999"),
            recipients=["Оля", "Дима", "Женя"],
            shares=[Decimal("3333")] * 3,
            balance=Decimal("13793"),
        )
        self.assertIn("9 999₽", text)
        self.assertIn("Оля — 3 333₽", text)
        self.assertIn("останется 3 794₽", text)

    def test_cancel_alert_names_the_payout_and_new_balance(self):
        text = format_dividend_cancel_alert(
            payout_id=17, amount=Decimal("9999"), balance_after=Decimal("20459")
        )
        self.assertIn("#17", text)
        self.assertIn("9 999₽", text)
        self.assertIn("20 459₽", text)


if __name__ == "__main__":
    unittest.main()
