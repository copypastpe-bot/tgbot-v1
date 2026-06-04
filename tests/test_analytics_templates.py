import unittest
from decimal import Decimal

from analytics_app.management import ManagementDashboard
from analytics_app.money import MoneySummary
from analytics_app.templates import render_dashboard, render_login, render_management_dashboard


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

    def test_management_dashboard_contains_decision_sections(self):
        dashboard = ManagementDashboard(
            gross_checks=Decimal("8000"),
            live_money=Decimal("7000"),
            bonuses_spent=Decimal("1000"),
            bonuses_earned=Decimal("300"),
            bonus_loss_percent=Decimal("0.13"),
            salary_total=Decimal("2800"),
            salary_percent=Decimal("0.35"),
            salary_base=Decimal("2000"),
            salary_fuel=Decimal("300"),
            salary_upsell=Decimal("500"),
            other_expenses=Decimal("1000"),
            operating_profit=Decimal("3200"),
            charts={
                "waterfall": {"labels": ["Чеки"], "values": [8000]},
                "expense_groups": {"labels": [], "values": []},
                "salary_by_master": {"labels": [], "values": []},
                "time_series": {"labels": [], "datasets": []},
            },
        )

        html = render_management_dashboard(
            title="Основная касса",
            active_section="main",
            period_label="01.06.2026-30.06.2026",
            date_from="2026-06-01",
            date_to="2026-06-30",
            balance=Decimal("500"),
            dashboard=dashboard,
            ledger=[],
            extra_note="",
        )

        self.assertIn("Управленческий обзор", html)
        self.assertIn("Зарплата от чеков", html)
        self.assertIn("Бонусная потеря", html)
        self.assertIn("Крупные расходы", html)
        self.assertIn('name="date_from"', html)
        self.assertIn('name="date_to"', html)
        self.assertIn("management-chart-data", html)


if __name__ == "__main__":
    unittest.main()
