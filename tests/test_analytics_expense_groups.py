import unittest
from decimal import Decimal

from analytics_app.expense_groups import (
    ExpenseGroup,
    classify_expense,
    group_expenses,
)
from analytics_app.management import ExpenseRow


class AnalyticsExpenseGroupTests(unittest.TestCase):
    def test_classifies_known_keywords(self):
        self.assertEqual(classify_expense(method="прочее", comment="Купили химию"), "Материалы/химия")
        self.assertEqual(classify_expense(method="прочее", comment="Авито реклама"), "Реклама/маркетинг")
        self.assertEqual(classify_expense(method="прочее", comment="Бензин мастер"), "Топливо/транспорт")
        self.assertEqual(classify_expense(method="DIV", comment="[DIV] Дивиденды"), "Дивиденды/изъятия")
        self.assertEqual(classify_expense(method="Наличные", comment="[WDR] Изъятие"), "Дивиденды/изъятия")

    def test_uncategorized_remains_visible(self):
        self.assertEqual(classify_expense(method="прочее", comment="непонятный расход"), "Без категории")
        self.assertEqual(classify_expense(method="СБП", comment="непонятный расход"), "Без категории")

    def test_group_expenses_returns_amount_share_count_and_examples(self):
        rows = [
            ExpenseRow(id=1, happened_at=None, amount=Decimal("1000"), method="прочее", comment="Химия"),
            ExpenseRow(id=2, happened_at=None, amount=Decimal("500"), method="прочее", comment="Химия перчатки"),
            ExpenseRow(id=3, happened_at=None, amount=Decimal("500"), method="прочее", comment="Непонятно"),
        ]

        groups = group_expenses(rows)

        self.assertEqual(groups[0], ExpenseGroup(name="Материалы/химия", amount=Decimal("1500"), share=Decimal("0.75"), count=2, examples=["Химия", "Химия перчатки"]))
        self.assertEqual(groups[1].name, "Без категории")
        self.assertEqual(groups[1].share, Decimal("0.25"))


if __name__ == "__main__":
    unittest.main()
