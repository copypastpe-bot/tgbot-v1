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

    def test_date_from_date_to_aliases_are_accepted(self):
        rng = resolve_date_range(
            {"date_from": "2026-04-01", "date_to": "2026-04-15"},
            today=date(2026, 6, 3),
        )

        self.assertEqual(rng.start_date, date(2026, 4, 1))
        self.assertEqual(rng.end_date, date(2026, 4, 15))
        self.assertEqual(rng.group_by, "day")

    def test_group_by_auto_week_for_medium_ranges(self):
        rng = resolve_date_range(
            {"date_from": "2026-01-01", "date_to": "2026-03-31"},
            today=date(2026, 6, 3),
        )

        self.assertEqual(rng.group_by, "week")

    def test_group_by_auto_month_for_long_ranges(self):
        rng = resolve_date_range(
            {"date_from": "2026-01-01", "date_to": "2026-12-31"},
            today=date(2026, 6, 3),
        )

        self.assertEqual(rng.group_by, "month")


if __name__ == "__main__":
    unittest.main()
