import unittest
import re
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from analytics_app.queries import (
    build_cleaning_dashboard,
    build_main_cash_dashboard,
)


class FakeConn:
    def __init__(self, rows=None, scalar=Decimal("0"), fetch_results=None, fetchval_results=None):
        self.rows = rows or []
        self.scalar = scalar
        self.fetch_results = list(fetch_results or [])
        self.fetchval_results = list(fetchval_results or [])
        self.fetch_calls = []
        self.fetchval_calls = []

    async def fetch(self, sql, *args):
        self.fetch_calls.append((sql, args))
        if self.fetch_results:
            return self.fetch_results.pop(0)
        return self.rows

    async def fetchval(self, sql, *args):
        self.fetchval_calls.append((sql, args))
        if self.fetchval_results:
            return self.fetchval_results.pop(0)
        return self.scalar


class AnalyticsQueryTests(unittest.IsolatedAsyncioTestCase):
    async def test_main_dashboard_uses_select_only_and_returns_summary(self):
        cashbook_rows = [
            {
                "id": 1,
                "happened_at": datetime(2026, 6, 1, tzinfo=ZoneInfo("UTC")),
                "kind": "income",
                "method": "Наличные",
                "amount": Decimal("1000"),
                "comment": "Заказ",
                "order_id": 1,
                "master_id": 2,
                "is_deleted": False,
            }
        ]
        conn = FakeConn(
            fetch_results=[[], [], cashbook_rows],
            scalar=Decimal("1000"),
        )

        dashboard = await build_main_cash_dashboard(
            conn,
            start_utc=datetime(2026, 6, 1, tzinfo=ZoneInfo("UTC")),
            end_utc=datetime(2026, 7, 1, tzinfo=ZoneInfo("UTC")),
        )

        self.assertEqual(dashboard["summary"].income, Decimal("1000"))
        self.assertEqual(dashboard["balance"], Decimal("1000"))
        self.assertEqual(len(dashboard["ledger"]), 1)
        sql_text = "\n".join(call[0] for call in conn.fetch_calls + conn.fetchval_calls).lower()
        self.assertIn("select", sql_text)
        self.assertIsNone(re.search(r"\binsert\b", sql_text))
        self.assertIsNone(re.search(r"\bupdate\b", sql_text))
        self.assertIsNone(re.search(r"\bdelete\b", sql_text))

    async def test_cleaning_dashboard_returns_gift_total(self):
        conn = FakeConn(rows=[], scalar=Decimal("500"))

        dashboard = await build_cleaning_dashboard(
            conn,
            start_utc=datetime(2026, 6, 1, tzinfo=ZoneInfo("UTC")),
            end_utc=datetime(2026, 7, 1, tzinfo=ZoneInfo("UTC")),
        )

        self.assertEqual(dashboard["gift_total"], Decimal("500"))

    async def test_main_dashboard_fetches_orders_payroll_expenses_and_balance(self):
        conn = FakeConn(
            fetch_results=[
                [
                    {
                        "id": 1,
                        "created_at": datetime(2026, 6, 1, tzinfo=ZoneInfo("UTC")),
                        "master_id": 10,
                        "master_name": "Анна",
                        "amount_total": Decimal("5000"),
                        "amount_cash": Decimal("4000"),
                        "bonus_spent": Decimal("1000"),
                        "bonus_earned": Decimal("400"),
                    }
                ],
                [
                    {
                        "order_id": 1,
                        "master_id": 10,
                        "master_name": "Анна",
                        "base_pay": Decimal("1000"),
                        "fuel_pay": Decimal("150"),
                        "upsell_pay": Decimal("0"),
                        "total_pay": Decimal("1150"),
                    }
                ],
                [
                    {
                        "id": 7,
                        "happened_at": datetime(2026, 6, 2, tzinfo=ZoneInfo("UTC")),
                        "kind": "expense",
                        "method": "прочее",
                        "amount": Decimal("700"),
                        "comment": "Химия",
                        "order_id": None,
                        "master_id": None,
                        "is_deleted": False,
                    }
                ],
            ],
            fetchval_results=[Decimal("1234")],
        )

        dashboard = await build_main_cash_dashboard(
            conn,
            start_utc=datetime(2026, 6, 1, tzinfo=ZoneInfo("UTC")),
            end_utc=datetime(2026, 7, 1, tzinfo=ZoneInfo("UTC")),
        )

        self.assertEqual(dashboard["management"].gross_checks, Decimal("5000"))
        self.assertEqual(dashboard["management"].salary_total, Decimal("1150"))
        self.assertEqual(dashboard["management"].other_expenses, Decimal("700"))
        self.assertEqual(dashboard["balance"], Decimal("1234"))
        self.assertEqual(len(conn.fetch_calls), 3)


if __name__ == "__main__":
    unittest.main()
