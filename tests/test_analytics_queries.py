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
    def __init__(self, rows, scalar=Decimal("0")):
        self.rows = rows
        self.scalar = scalar
        self.fetch_calls = []
        self.fetchval_calls = []

    async def fetch(self, sql, *args):
        self.fetch_calls.append((sql, args))
        return self.rows

    async def fetchval(self, sql, *args):
        self.fetchval_calls.append((sql, args))
        return self.scalar


class AnalyticsQueryTests(unittest.IsolatedAsyncioTestCase):
    async def test_main_dashboard_uses_select_only_and_returns_summary(self):
        conn = FakeConn(
            rows=[
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
            ],
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


if __name__ == "__main__":
    unittest.main()
