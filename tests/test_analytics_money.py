import unittest
from decimal import Decimal

from analytics_app.money import (
    CashbookRow,
    summarize_cashbook_rows,
    summarize_cleaning_rows,
)


class AnalyticsMoneyTests(unittest.TestCase):
    def test_main_cash_excludes_withdrawals_dividends_deleted_and_opening_from_pnl(self):
        rows = [
            CashbookRow(kind="income", method="Наличные", amount=Decimal("1000"), comment="Заказ #1"),
            CashbookRow(kind="expense", method="прочее", amount=Decimal("200"), comment="Материалы"),
            CashbookRow(kind="expense", method="Наличные", amount=Decimal("300"), comment="[WDR] Изъятие"),
            CashbookRow(kind="expense", method="DIV", amount=Decimal("400"), comment="[DIV] Дивиденды"),
            CashbookRow(kind="income", method="Наличные", amount=Decimal("500"), comment="Стартовый остаток"),
            CashbookRow(kind="expense", method="прочее", amount=Decimal("900"), comment="Удалено", is_deleted=True),
        ]

        summary = summarize_cashbook_rows(rows)

        self.assertEqual(summary.income, Decimal("1000"))
        self.assertEqual(summary.expense, Decimal("200"))
        self.assertEqual(summary.profit, Decimal("800"))
        self.assertEqual(summary.service_expense, Decimal("700"))
        self.assertEqual(summary.income_by_method, {"Наличные": Decimal("1000")})

    def test_cleaning_excludes_capital_movements_from_pnl(self):
        rows = [
            CashbookRow(kind="income", method="Наличные", amount=Decimal("1000"), comment="Уборка"),
            CashbookRow(kind="expense", method="Химия", amount=Decimal("100"), comment="Химия"),
            CashbookRow(kind="deposit", method="Вклад", amount=Decimal("500"), comment="Вклад"),
            CashbookRow(kind="withdrawal", method="Изъятие", amount=Decimal("200"), comment="Изъятие"),
            CashbookRow(kind="dividend", method="DIV", amount=Decimal("300"), comment="DIV"),
        ]

        summary = summarize_cleaning_rows(rows)

        self.assertEqual(summary.income, Decimal("1000"))
        self.assertEqual(summary.expense, Decimal("100"))
        self.assertEqual(summary.profit, Decimal("900"))
        self.assertEqual(summary.expense_by_group, {"Химия": Decimal("100")})


if __name__ == "__main__":
    unittest.main()
