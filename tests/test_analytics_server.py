import unittest
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from aiohttp.test_utils import AioHTTPTestCase

from analytics_app.auth import make_password_hash
from analytics_app.config import AnalyticsConfig
from analytics_app.management import ManagementDashboard
from analytics_app.money import MoneySummary
from analytics_app.server import create_app


class FakePool:
    async def close(self):
        return None


class FakeAcquire:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, exc_type, exc, tb):
        return None


def fake_management_dashboard():
    return ManagementDashboard(
        gross_checks=Decimal("1000"),
        live_money=Decimal("900"),
        bonuses_spent=Decimal("100"),
        bonuses_earned=Decimal("90"),
        bonus_loss_percent=Decimal("0.10"),
        salary_total=Decimal("300"),
        salary_percent=Decimal("0.30"),
        salary_base=Decimal("200"),
        salary_fuel=Decimal("50"),
        salary_upsell=Decimal("50"),
        other_expenses=Decimal("100"),
        operating_profit=Decimal("500"),
        cash_income=Decimal("1000"),
        cash_expense=Decimal("500"),
        cash_profit=Decimal("500"),
        div_paid=Decimal("50"),
        charts={
            "waterfall": {"labels": ["Доходы"], "values": [1000]},
            "expense_groups": {"labels": [], "values": []},
            "salary_by_master": {"labels": [], "values": []},
            "time_series": {"labels": [], "datasets": []},
        },
    )


class FakeQueryService:
    def __init__(self):
        self.pool = FakePool()

    def acquire(self):
        return FakeAcquire()

    async def main(self, conn, *, start_utc, end_utc, group_by):
        return {
            "summary": MoneySummary(income=Decimal("1000"), expense=Decimal("200"), profit=Decimal("800")),
            "balance": Decimal("500"),
            "management": fake_management_dashboard(),
            "ledger": [
                {
                    "happened_at": datetime(2026, 6, 1, tzinfo=ZoneInfo("UTC")),
                    "kind": "income",
                    "method": "Наличные",
                    "amount": Decimal("1000"),
                    "comment": "Заказ",
                }
            ],
        }

    async def cleaning(self, conn, *, start_utc, end_utc, group_by):
        return {
            "summary": MoneySummary(income=Decimal("2000"), expense=Decimal("500"), profit=Decimal("1500")),
            "balance": Decimal("1500"),
            "gift_total": Decimal("0"),
            "management": fake_management_dashboard(),
            "ledger": [],
        }


class AnalyticsServerTests(AioHTTPTestCase):
    async def get_application(self):
        cfg = AnalyticsConfig(
            db_dsn="postgres://unused",
            session_secret="session-secret",
            users={"owner": make_password_hash("password", salt="fixedsalt", iterations=1000)},
        )
        return create_app(cfg, query_service=FakeQueryService())

    async def test_root_redirects_to_login_when_unauthenticated(self):
        resp = await self.client.get("/", allow_redirects=False)

        self.assertEqual(resp.status, 302)
        self.assertEqual(resp.headers["Location"], "/login")

    async def test_login_sets_session_and_root_renders(self):
        resp = await self.client.post(
            "/login",
            data={"login": "owner", "password": "password"},
            allow_redirects=False,
        )

        self.assertEqual(resp.status, 302)
        self.assertIn("analytics_session", resp.cookies)

        cookie = resp.cookies["analytics_session"].value
        page = await self.client.get("/", cookies={"analytics_session": cookie})
        text = await page.text()

        self.assertEqual(page.status, 200)
        self.assertIn("Основная касса", text)
        self.assertIn("Кассовая прибыль", text)
        self.assertIn("Заказы и зарплата", text)

    async def test_cleaning_requires_auth_and_renders_after_login(self):
        login = await self.client.post(
            "/login",
            data={"login": "owner", "password": "password"},
            allow_redirects=False,
        )
        cookie = login.cookies["analytics_session"].value

        page = await self.client.get("/cleaning", cookies={"analytics_session": cookie})
        text = await page.text()

        self.assertEqual(page.status, 200)
        self.assertIn("Клининг", text)


if __name__ == "__main__":
    unittest.main()
