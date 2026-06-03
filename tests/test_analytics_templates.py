import unittest
from decimal import Decimal

from analytics_app.money import MoneySummary
from analytics_app.templates import render_dashboard, render_login


class AnalyticsTemplateTests(unittest.TestCase):
    def test_login_contains_form(self):
        html = render_login(error=False)

        self.assertIn("<form", html)
        self.assertIn('name="login"', html)
        self.assertIn('name="password"', html)

    def test_dashboard_contains_sections_and_chart_payload(self):
        html = render_dashboard(
            title="Основная касса",
            active_section="main",
            period_label="01.06.2026-30.06.2026",
            summary=MoneySummary(income=Decimal("1000"), expense=Decimal("200"), profit=Decimal("800")),
            balance=Decimal("500"),
            ledger=[],
            income_by_method={"Наличные": Decimal("1000")},
            expense_by_group={"Без категории": Decimal("200")},
            extra_note="Расходы основной кассы без строгих категорий.",
        )

        self.assertIn("Основная касса", html)
        self.assertIn("Клининг", html)
        self.assertIn("chart-data", html)
        self.assertIn("Расходы основной кассы без строгих категорий.", html)


if __name__ == "__main__":
    unittest.main()
