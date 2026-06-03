from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Mapping
from zoneinfo import ZoneInfo


MOSCOW_TZ = ZoneInfo("Europe/Moscow")
UTC = ZoneInfo("UTC")


@dataclass(frozen=True)
class DateRange:
    start_date: date
    end_date: date
    start_utc: datetime
    end_utc: datetime
    group_by: str


def _parse_date(raw: str | None) -> date | None:
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def _bounds(start_date: date, end_date: date) -> tuple[datetime, datetime]:
    start_local = datetime.combine(start_date, time.min, tzinfo=MOSCOW_TZ)
    end_local = datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=MOSCOW_TZ)
    return start_local.astimezone(UTC), end_local.astimezone(UTC)


def _auto_group_by(start_date: date, end_date: date) -> str:
    days = (end_date - start_date).days + 1
    if days > 180:
        return "month"
    if days > 45:
        return "week"
    return "day"


def resolve_date_range(params: Mapping[str, str], *, today: date | None = None) -> DateRange:
    current = today or datetime.now(MOSCOW_TZ).date()
    explicit_group = params.get("group") if params.get("group") in {"day", "week", "month"} else None
    custom_start = _parse_date(params.get("start") or params.get("date_from"))
    custom_end = _parse_date(params.get("end") or params.get("date_to"))
    if custom_start and custom_end and custom_start <= custom_end:
        start_date, end_date = custom_start, custom_end
        group_by = explicit_group or _auto_group_by(start_date, end_date)
        start_utc, end_utc = _bounds(start_date, end_date)
        return DateRange(start_date, end_date, start_utc, end_utc, group_by)

    period = (params.get("period") or "today").lower()
    if period == "week":
        start_date = current - timedelta(days=current.weekday())
        end_date = start_date + timedelta(days=6)
    elif period == "month":
        start_date = current.replace(day=1)
        end_date = current.replace(day=calendar.monthrange(current.year, current.month)[1])
    elif period == "year":
        start_date = date(current.year, 1, 1)
        end_date = date(current.year, 12, 31)
    else:
        start_date = current
        end_date = current

    if explicit_group:
        group_by = explicit_group
    else:
        group_by = _auto_group_by(start_date, end_date)

    start_utc, end_utc = _bounds(start_date, end_date)
    return DateRange(start_date, end_date, start_utc, end_utc, group_by)
