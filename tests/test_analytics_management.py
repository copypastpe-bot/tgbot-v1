import unittest
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from analytics_app.management import (
    ExpenseRow,
    OrderMetricRow,
    PayrollMetricRow,
    build_management_dashboard,
)


class AnalyticsManagementTests(unittest.TestCase):
    def test_dashboard_calculates_salary_and_bonus_pressure(self):
        orders = [
            OrderMetricRow(
                id=1,
                created_at=datetime(2026, 6, 1, tzinfo=ZoneInfo("UTC")),
                master_id=10,
                master_name="Анна",
                amount_total=Decimal("5000"),
                amount_cash=Decimal("4000"),
                bonus_spent=Decimal("1000"),
                bonus_earned=Decimal("400"),
            ),
            OrderMetricRow(
                id=2,
                created_at=datetime(2026, 6, 2, tzinfo=ZoneInfo("UTC")),
                master_id=11,
                master_name="Борис",
                amount_total=Decimal("3000"),
                amount_cash=Decimal("3000"),
                bonus_spent=Decimal("0"),
                bonus_earned=Decimal("300"),
            ),
        ]
        payroll = [
            PayrollMetricRow(
                order_id=1,
                master_id=10,
                master_name="Анна",
                base_pay=Decimal("1000"),
                fuel_pay=Decimal("150"),
                upsell_pay=Decimal("0"),
                total_pay=Decimal("1150"),
            ),
            PayrollMetricRow(
                order_id=2,
                master_id=11,
                master_name="Борис",
                base_pay=Decimal("1000"),
                fuel_pay=Decimal("150"),
                upsell_pay=Decimal("500"),
                total_pay=Decimal("1650"),
            ),
        ]
        expenses = [
            ExpenseRow(
                id=1,
                happened_at=datetime(2026, 6, 2, tzinfo=ZoneInfo("UTC")),
                amount=Decimal("700"),
                method="прочее",
                comment="Химия",
            ),
            ExpenseRow(
                id=2,
                happened_at=datetime(2026, 6, 3, tzinfo=ZoneInfo("UTC")),
                amount=Decimal("300"),
                method="прочее",
                comment="Авито реклама",
            ),
        ]

        dashboard = build_management_dashboard(
            orders=orders,
            payroll=payroll,
            expenses=expenses,
            group_by="day",
        )

        self.assertEqual(dashboard.gross_checks, Decimal("8000"))
        self.assertEqual(dashboard.live_money, Decimal("7000"))
        self.assertEqual(dashboard.bonuses_spent, Decimal("1000"))
        self.assertEqual(dashboard.bonuses_earned, Decimal("700"))
        self.assertEqual(dashboard.bonus_loss_percent, Decimal("0.13"))
        self.assertEqual(dashboard.salary_total, Decimal("2800"))
        self.assertEqual(dashboard.salary_percent, Decimal("0.35"))
        self.assertEqual(dashboard.salary_base, Decimal("2000"))
        self.assertEqual(dashboard.salary_fuel, Decimal("300"))
        self.assertEqual(dashboard.salary_upsell, Decimal("500"))
        self.assertEqual(dashboard.other_expenses, Decimal("1000"))
        self.assertEqual(dashboard.operating_profit, Decimal("3200"))
        self.assertEqual(dashboard.waterfall["gross_checks"], Decimal("8000"))
        self.assertEqual(dashboard.waterfall["operating_profit"], Decimal("3200"))
        self.assertEqual(
            [row.master_name for row in dashboard.master_salaries],
            ["Борис", "Анна"],
        )
        boris, anna = dashboard.master_salaries
        self.assertEqual(boris.gross_checks, Decimal("3000"))
        self.assertEqual(boris.live_money, Decimal("3000"))
        self.assertEqual(boris.salary, Decimal("1650"))
        self.assertEqual(boris.salary_percent, Decimal("0.55"))
        self.assertEqual(anna.gross_checks, Decimal("5000"))
        self.assertEqual(anna.live_money, Decimal("4000"))
        self.assertEqual(anna.salary, Decimal("1150"))
        self.assertEqual(anna.salary_percent, Decimal("0.23"))

    def test_top_expenses_sorted_descending(self):
        expenses = [
            ExpenseRow(
                id=1,
                happened_at=None,
                amount=Decimal("10"),
                method="прочее",
                comment="small",
            ),
            ExpenseRow(
                id=2,
                happened_at=None,
                amount=Decimal("50"),
                method="прочее",
                comment="large",
            ),
            ExpenseRow(
                id=3,
                happened_at=None,
                amount=Decimal("30"),
                method="прочее",
                comment="mid",
            ),
        ]

        dashboard = build_management_dashboard(
            orders=[],
            payroll=[],
            expenses=expenses,
            group_by="day",
        )

        self.assertEqual([row.id for row in dashboard.top_expenses], [2, 3, 1])
