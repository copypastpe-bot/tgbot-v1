import unittest
from datetime import date, datetime
from zoneinfo import ZoneInfo

from analytics_app.dates import DateRange, resolve_date_range


class AnalyticsDateTests(unittest.TestCase):
    def test_month_range_uses_moscow_bounds(self):
        today = date(2026, 6, 3)
        rng = resolve_date_range({"period": "month"}, today=today)

        self.assertIsInstance(rng, DateRange)
        self.assertEqual(rng.start_date, date(2026, 6, 1))
        self.assertEqual(rng.end_date, date(2026, 6, 30))
        self.assertEqual(rng.group_by, "day")
        self.assertEqual(rng.start_utc, datetime(2026, 5, 31, 21, 0, tzinfo=ZoneInfo("UTC")))
        self.assertEqual(rng.end_utc, datetime(2026, 6, 30, 21, 0, tzinfo=ZoneInfo("UTC")))

    def test_custom_range_and_group_by(self):
        rng = resolve_date_range(
            {"start": "2026-05-01", "end": "2026-05-31", "group": "month"},
            today=date(2026, 6, 3),
        )

        self.assertEqual(rng.start_date, date(2026, 5, 1))
        self.assertEqual(rng.end_date, date(2026, 5, 31))
        self.assertEqual(rng.group_by, "month")

    def test_invalid_period_defaults_to_today(self):
        rng = resolve_date_range({"period": "bad"}, today=date(2026, 6, 3))

        self.assertEqual(rng.start_date, date(2026, 6, 3))
        self.assertEqual(rng.end_date, date(2026, 6, 3))


if __name__ == "__main__":
    unittest.main()
