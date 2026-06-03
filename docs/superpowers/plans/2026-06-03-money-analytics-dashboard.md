# Money Analytics Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a separate read-only web dashboard for main cash desk and cleaning money analytics.

**Architecture:** Add a focused `analytics_app/` package that runs independently from `bot.py` using `aiohttp`, `asyncpg`, server-rendered HTML, and embedded chart payloads. Keep all database access in query modules and all auth/session code in auth/config modules so the app can be deployed as its own systemd service behind nginx on `analytics.dastydev.ru`.

**Tech Stack:** Python 3.10+, aiohttp, asyncpg, python-dotenv, stdlib `hashlib`/`hmac` password verification, Chart.js from CDN, unittest.

---

## File Structure

Create:

- `analytics_app/__init__.py` — package marker.
- `analytics_app/__main__.py` — `python -m analytics_app` entrypoint.
- `analytics_app/config.py` — environment parsing for DB DSN, bind host/port, session secret, and user hashes.
- `analytics_app/auth.py` — password hashing/verification and signed cookie helpers.
- `analytics_app/dates.py` — period parsing and grouping helpers.
- `analytics_app/money.py` — pure calculation helpers for classifying cashbook rows and summaries.
- `analytics_app/queries.py` — async PostgreSQL reads for main cash and cleaning dashboards.
- `analytics_app/server.py` — aiohttp app factory, routes, auth middleware, static routing.
- `analytics_app/templates.py` — server-rendered HTML layout and page fragments.
- `analytics_app/static/app.css` — dashboard styling.
- `analytics_app/static/app.js` — Chart.js initialization and filter handling.
- `tests/test_analytics_auth.py` — auth hash/session tests.
- `tests/test_analytics_dates.py` — period/grouping tests.
- `tests/test_analytics_money.py` — read-only calculation tests.
- `tests/test_analytics_server.py` — route/auth smoke tests with fake query service.

Modify:

- `requirements.txt` only if the implementation needs an extra dependency. The preferred plan uses current dependencies and does not add packages.
- `.env.example` only if the repository already tracks it later in the task. The current `.gitignore` explicitly ignores `.env.example` except a negation rule, but no tracked `.env.example` is present in this repo, so do not create one in this implementation.

Do not modify:

- `bot.py`
- `cleaning/`
- production notification rules

---

### Task 1: Analytics Package Skeleton And Config

**Files:**
- Create: `analytics_app/__init__.py`
- Create: `analytics_app/__main__.py`
- Create: `analytics_app/config.py`
- Test: `tests/test_analytics_config.py`

- [ ] **Step 1: Write failing config tests**

Create `tests/test_analytics_config.py`:

```python
import unittest

from analytics_app.config import AnalyticsConfig, load_config


class AnalyticsConfigTests(unittest.TestCase):
    def test_load_config_uses_analytics_dsn_over_bot_dsn(self):
        env = {
            "DB_DSN": "postgres://bot",
            "ANALYTICS_DB_DSN": "postgres://analytics",
            "ANALYTICS_SESSION_SECRET": "secret",
            "ANALYTICS_USERS": "owner:hash1,partner:hash2",
        }

        cfg = load_config(env)

        self.assertEqual(cfg.db_dsn, "postgres://analytics")
        self.assertEqual(cfg.bind_host, "127.0.0.1")
        self.assertEqual(cfg.bind_port, 8090)
        self.assertEqual(cfg.users, {"owner": "hash1", "partner": "hash2"})

    def test_load_config_falls_back_to_db_dsn(self):
        cfg = load_config(
            {
                "DB_DSN": "postgres://bot",
                "ANALYTICS_SESSION_SECRET": "secret",
                "ANALYTICS_USERS": "owner:hash1",
            }
        )

        self.assertEqual(cfg.db_dsn, "postgres://bot")

    def test_load_config_requires_dsn_secret_and_users(self):
        with self.assertRaisesRegex(ValueError, "DB_DSN"):
            load_config({"ANALYTICS_SESSION_SECRET": "secret", "ANALYTICS_USERS": "owner:hash"})

        with self.assertRaisesRegex(ValueError, "ANALYTICS_SESSION_SECRET"):
            load_config({"DB_DSN": "postgres://bot", "ANALYTICS_USERS": "owner:hash"})

        with self.assertRaisesRegex(ValueError, "ANALYTICS_USERS"):
            load_config({"DB_DSN": "postgres://bot", "ANALYTICS_SESSION_SECRET": "secret"})

    def test_config_type_is_explicit(self):
        cfg = load_config(
            {
                "DB_DSN": "postgres://bot",
                "ANALYTICS_SESSION_SECRET": "secret",
                "ANALYTICS_USERS": "owner:hash1",
                "ANALYTICS_BIND_HOST": "0.0.0.0",
                "ANALYTICS_BIND_PORT": "9000",
            }
        )

        self.assertIsInstance(cfg, AnalyticsConfig)
        self.assertEqual(cfg.bind_host, "0.0.0.0")
        self.assertEqual(cfg.bind_port, 9000)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run config tests and verify failure**

Run:

```bash
python -m unittest tests.test_analytics_config -v
```

Expected: FAIL with `ModuleNotFoundError` for `analytics_app`.

- [ ] **Step 3: Implement package skeleton and config**

Create `analytics_app/__init__.py`:

```python
"""Read-only web analytics dashboard for tgbot-v1 financial data."""
```

Create `analytics_app/config.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from os import environ
from typing import Mapping


@dataclass(frozen=True)
class AnalyticsConfig:
    db_dsn: str
    session_secret: str
    users: dict[str, str]
    bind_host: str = "127.0.0.1"
    bind_port: int = 8090


def _parse_users(raw: str) -> dict[str, str]:
    users: dict[str, str] = {}
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        if ":" not in item:
            raise ValueError("ANALYTICS_USERS entries must use login:hash")
        login, password_hash = item.split(":", 1)
        login = login.strip()
        password_hash = password_hash.strip()
        if not login or not password_hash:
            raise ValueError("ANALYTICS_USERS entries must use non-empty login:hash")
        users[login] = password_hash
    if not users:
        raise ValueError("ANALYTICS_USERS must contain at least one user")
    return users


def load_config(env: Mapping[str, str] | None = None) -> AnalyticsConfig:
    source = environ if env is None else env
    db_dsn = (source.get("ANALYTICS_DB_DSN") or source.get("DB_DSN") or "").strip()
    if not db_dsn:
        raise ValueError("DB_DSN or ANALYTICS_DB_DSN is required")

    session_secret = (source.get("ANALYTICS_SESSION_SECRET") or "").strip()
    if not session_secret:
        raise ValueError("ANALYTICS_SESSION_SECRET is required")

    users_raw = (source.get("ANALYTICS_USERS") or "").strip()
    if not users_raw:
        raise ValueError("ANALYTICS_USERS is required")

    bind_host = (source.get("ANALYTICS_BIND_HOST") or "127.0.0.1").strip()
    bind_port = int((source.get("ANALYTICS_BIND_PORT") or "8090").strip())
    return AnalyticsConfig(
        db_dsn=db_dsn,
        session_secret=session_secret,
        users=_parse_users(users_raw),
        bind_host=bind_host,
        bind_port=bind_port,
    )
```

Create `analytics_app/__main__.py`:

```python
from __future__ import annotations

from aiohttp import web

from .config import load_config
from .server import create_app


def main() -> None:
    cfg = load_config()
    app = create_app(cfg)
    web.run_app(app, host=cfg.bind_host, port=cfg.bind_port)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run config tests and verify pass**

Run:

```bash
python -m unittest tests.test_analytics_config -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git add analytics_app/__init__.py analytics_app/__main__.py analytics_app/config.py tests/test_analytics_config.py
git commit -m "feat: add analytics app config"
```

---

### Task 2: Authentication And Signed Sessions

**Files:**
- Create: `analytics_app/auth.py`
- Test: `tests/test_analytics_auth.py`

- [ ] **Step 1: Write failing auth tests**

Create `tests/test_analytics_auth.py`:

```python
import unittest

from analytics_app.auth import (
    make_password_hash,
    sign_session,
    unsign_session,
    verify_password,
)


class AnalyticsAuthTests(unittest.TestCase):
    def test_password_hash_verifies_correct_password(self):
        password_hash = make_password_hash("secret-password", salt="fixedsalt", iterations=1000)

        self.assertTrue(verify_password("secret-password", password_hash))
        self.assertFalse(verify_password("wrong-password", password_hash))

    def test_invalid_hash_does_not_verify(self):
        self.assertFalse(verify_password("secret-password", "bad-hash"))

    def test_session_sign_and_unsign_round_trip(self):
        cookie = sign_session("owner", secret="session-secret", now=1_700_000_000)

        self.assertEqual(unsign_session(cookie, secret="session-secret", now=1_700_000_100), "owner")

    def test_session_rejects_wrong_secret_and_expired_cookie(self):
        cookie = sign_session("owner", secret="session-secret", now=1_700_000_000)

        self.assertIsNone(unsign_session(cookie, secret="wrong-secret", now=1_700_000_100))
        self.assertIsNone(unsign_session(cookie, secret="session-secret", now=1_700_100_000, max_age_seconds=3600))


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run auth tests and verify failure**

Run:

```bash
python -m unittest tests.test_analytics_auth -v
```

Expected: FAIL with `ModuleNotFoundError` or missing function errors.

- [ ] **Step 3: Implement password and session helpers**

Create `analytics_app/auth.py`:

```python
from __future__ import annotations

import base64
import hashlib
import hmac
import os
import time


HASH_PREFIX = "pbkdf2_sha256"
DEFAULT_ITERATIONS = 260_000


def _b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64decode(raw: str) -> bytes:
    padding = "=" * (-len(raw) % 4)
    return base64.urlsafe_b64decode((raw + padding).encode("ascii"))


def make_password_hash(
    password: str,
    *,
    salt: str | None = None,
    iterations: int = DEFAULT_ITERATIONS,
) -> str:
    if not salt:
        salt = _b64encode(os.urandom(16))
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        iterations,
    )
    return f"{HASH_PREFIX}${iterations}${salt}${_b64encode(digest)}"


def verify_password(password: str, password_hash: str) -> bool:
    try:
        prefix, iterations_raw, salt, expected = password_hash.split("$", 3)
        if prefix != HASH_PREFIX:
            return False
        iterations = int(iterations_raw)
        actual = make_password_hash(password, salt=salt, iterations=iterations).split("$", 3)[3]
        return hmac.compare_digest(actual, expected)
    except Exception:
        return False


def _session_signature(login: str, issued_at: int, secret: str) -> str:
    payload = f"{login}:{issued_at}".encode("utf-8")
    digest = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).digest()
    return _b64encode(digest)


def sign_session(login: str, *, secret: str, now: int | None = None) -> str:
    issued_at = int(time.time() if now is None else now)
    signature = _session_signature(login, issued_at, secret)
    payload = f"{login}:{issued_at}:{signature}".encode("utf-8")
    return _b64encode(payload)


def unsign_session(
    cookie_value: str,
    *,
    secret: str,
    now: int | None = None,
    max_age_seconds: int = 60 * 60 * 24 * 14,
) -> str | None:
    try:
        login, issued_raw, signature = _b64decode(cookie_value).decode("utf-8").split(":", 2)
        issued_at = int(issued_raw)
    except Exception:
        return None

    current = int(time.time() if now is None else now)
    if current - issued_at > max_age_seconds:
        return None

    expected = _session_signature(login, issued_at, secret)
    if not hmac.compare_digest(signature, expected):
        return None
    return login
```

- [ ] **Step 4: Run auth tests and verify pass**

Run:

```bash
python -m unittest tests.test_analytics_auth -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git add analytics_app/auth.py tests/test_analytics_auth.py
git commit -m "feat: add analytics authentication helpers"
```

---

### Task 3: Period Parsing And Money Calculation Helpers

**Files:**
- Create: `analytics_app/dates.py`
- Create: `analytics_app/money.py`
- Test: `tests/test_analytics_dates.py`
- Test: `tests/test_analytics_money.py`

- [ ] **Step 1: Write failing date tests**

Create `tests/test_analytics_dates.py`:

```python
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
```

- [ ] **Step 2: Write failing money tests**

Create `tests/test_analytics_money.py`:

```python
import unittest
from decimal import Decimal

from analytics_app.money import (
    CashbookRow,
    summarize_cashbook_rows,
    summarize_cleaning_rows,
)


class AnalyticsMoneyTests(unittest.TestCase):
    def test_main_cash_excludes_withdrawals_dividends_deleted_and_opening_from_pnl(self):
        rows = [
            CashbookRow(kind="income", method="Наличные", amount=Decimal("1000"), comment="Заказ #1"),
            CashbookRow(kind="expense", method="прочее", amount=Decimal("200"), comment="Материалы"),
            CashbookRow(kind="expense", method="Наличные", amount=Decimal("300"), comment="[WDR] Изъятие"),
            CashbookRow(kind="expense", method="DIV", amount=Decimal("400"), comment="[DIV] Дивиденды"),
            CashbookRow(kind="income", method="Наличные", amount=Decimal("500"), comment="Стартовый остаток"),
            CashbookRow(kind="expense", method="прочее", amount=Decimal("900"), comment="Удалено", is_deleted=True),
        ]

        summary = summarize_cashbook_rows(rows)

        self.assertEqual(summary.income, Decimal("1000"))
        self.assertEqual(summary.expense, Decimal("200"))
        self.assertEqual(summary.profit, Decimal("800"))
        self.assertEqual(summary.service_expense, Decimal("700"))
        self.assertEqual(summary.income_by_method, {"Наличные": Decimal("1000")})

    def test_cleaning_excludes_capital_movements_from_pnl(self):
        rows = [
            CashbookRow(kind="income", method="Наличные", amount=Decimal("1000"), comment="Уборка"),
            CashbookRow(kind="expense", method="Химия", amount=Decimal("100"), comment="Химия"),
            CashbookRow(kind="deposit", method="Вклад", amount=Decimal("500"), comment="Вклад"),
            CashbookRow(kind="withdrawal", method="Изъятие", amount=Decimal("200"), comment="Изъятие"),
            CashbookRow(kind="dividend", method="DIV", amount=Decimal("300"), comment="DIV"),
        ]

        summary = summarize_cleaning_rows(rows)

        self.assertEqual(summary.income, Decimal("1000"))
        self.assertEqual(summary.expense, Decimal("100"))
        self.assertEqual(summary.profit, Decimal("900"))
        self.assertEqual(summary.expense_by_group, {"Химия": Decimal("100")})
```

- [ ] **Step 3: Run tests and verify failure**

Run:

```bash
python -m unittest tests.test_analytics_dates tests.test_analytics_money -v
```

Expected: FAIL with missing modules.

- [ ] **Step 4: Implement date helpers**

Create `analytics_app/dates.py`:

```python
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


def resolve_date_range(params: Mapping[str, str], *, today: date | None = None) -> DateRange:
    current = today or datetime.now(MOSCOW_TZ).date()
    group_by = params.get("group") if params.get("group") in {"day", "week", "month"} else "day"
    custom_start = _parse_date(params.get("start"))
    custom_end = _parse_date(params.get("end"))
    if custom_start and custom_end and custom_start <= custom_end:
        start_date, end_date = custom_start, custom_end
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
        group_by = "month"
    else:
        start_date = current
        end_date = current

    start_utc, end_utc = _bounds(start_date, end_date)
    return DateRange(start_date, end_date, start_utc, end_utc, group_by)
```

- [ ] **Step 5: Implement money helpers**

Create `analytics_app/money.py`:

```python
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal


ZERO = Decimal("0")
DIVIDEND_METHOD = "DIV"


@dataclass(frozen=True)
class CashbookRow:
    kind: str
    method: str
    amount: Decimal
    comment: str = ""
    is_deleted: bool = False


@dataclass(frozen=True)
class MoneySummary:
    income: Decimal = ZERO
    expense: Decimal = ZERO
    profit: Decimal = ZERO
    service_expense: Decimal = ZERO
    income_by_method: dict[str, Decimal] = field(default_factory=dict)
    expense_by_group: dict[str, Decimal] = field(default_factory=dict)


def _add(bucket: dict[str, Decimal], key: str, amount: Decimal) -> None:
    label = key or "Без метода"
    bucket[label] = bucket.get(label, ZERO) + amount


def is_opening_balance(row: CashbookRow) -> bool:
    return row.kind == "opening_balance" or row.comment.lower().startswith("стартовый остаток")


def is_withdrawal(row: CashbookRow) -> bool:
    comment = row.comment.lower()
    return row.kind == "expense" and (
        comment.startswith("[wdr]")
        or comment.startswith("изъят")
        or (row.method == "Наличные" and "изъят" in comment)
    )


def is_dividend(row: CashbookRow) -> bool:
    comment = row.comment.lower()
    return row.method == DIVIDEND_METHOD or comment.startswith("[div]")


def summarize_cashbook_rows(rows: list[CashbookRow]) -> MoneySummary:
    income = ZERO
    expense = ZERO
    service_expense = ZERO
    income_by_method: dict[str, Decimal] = {}
    expense_by_group: dict[str, Decimal] = {}

    for row in rows:
        if row.is_deleted:
            continue
        if is_opening_balance(row):
            continue
        if row.kind == "income":
            income += row.amount
            _add(income_by_method, row.method, row.amount)
            continue
        if row.kind == "expense" and (is_withdrawal(row) or is_dividend(row)):
            service_expense += row.amount
            continue
        if row.kind == "expense":
            expense += row.amount
            _add(expense_by_group, "Без категории", row.amount)

    return MoneySummary(
        income=income,
        expense=expense,
        profit=income - expense,
        service_expense=service_expense,
        income_by_method=income_by_method,
        expense_by_group=expense_by_group,
    )


def summarize_cleaning_rows(rows: list[CashbookRow]) -> MoneySummary:
    income = ZERO
    expense = ZERO
    income_by_method: dict[str, Decimal] = {}
    expense_by_group: dict[str, Decimal] = {}

    for row in rows:
        if row.is_deleted:
            continue
        if row.kind == "income":
            income += row.amount
            _add(income_by_method, row.method, row.amount)
        elif row.kind == "expense":
            expense += row.amount
            _add(expense_by_group, row.method or "Без категории", row.amount)

    return MoneySummary(
        income=income,
        expense=expense,
        profit=income - expense,
        income_by_method=income_by_method,
        expense_by_group=expense_by_group,
    )
```

- [ ] **Step 6: Run date and money tests and verify pass**

Run:

```bash
python -m unittest tests.test_analytics_dates tests.test_analytics_money -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

Run:

```bash
git add analytics_app/dates.py analytics_app/money.py tests/test_analytics_dates.py tests/test_analytics_money.py
git commit -m "feat: add analytics money calculations"
```

---

### Task 4: Read-Only Query Layer

**Files:**
- Create: `analytics_app/queries.py`
- Test: `tests/test_analytics_queries.py`

- [ ] **Step 1: Write query contract tests using a fake connection**

Create `tests/test_analytics_queries.py`:

```python
import unittest
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
        self.assertNotIn("insert", sql_text)
        self.assertNotIn("update", sql_text)
        self.assertNotIn("delete", sql_text)

    async def test_cleaning_dashboard_returns_gift_total(self):
        conn = FakeConn(rows=[], scalar=Decimal("500"))

        dashboard = await build_cleaning_dashboard(
            conn,
            start_utc=datetime(2026, 6, 1, tzinfo=ZoneInfo("UTC")),
            end_utc=datetime(2026, 7, 1, tzinfo=ZoneInfo("UTC")),
        )

        self.assertEqual(dashboard["gift_total"], Decimal("500"))
```

- [ ] **Step 2: Run query tests and verify failure**

Run:

```bash
python -m unittest tests.test_analytics_queries -v
```

Expected: FAIL with missing `analytics_app.queries`.

- [ ] **Step 3: Implement query layer**

Create `analytics_app/queries.py`:

```python
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from .money import CashbookRow, summarize_cashbook_rows, summarize_cleaning_rows


def _decimal(value: Any) -> Decimal:
    return Decimal(value or 0)


def _row_to_cashbook(row: Any) -> CashbookRow:
    return CashbookRow(
        kind=row["kind"] or "",
        method=row["method"] or "",
        amount=_decimal(row["amount"]),
        comment=row["comment"] or "",
        is_deleted=bool(row.get("is_deleted") if hasattr(row, "get") else row["is_deleted"]),
    )


async def build_main_cash_dashboard(conn, *, start_utc: datetime, end_utc: datetime) -> dict[str, Any]:
    rows = await conn.fetch(
        """
        SELECT id, happened_at, kind, method, amount,
               COALESCE(comment, '') AS comment,
               order_id, master_id,
               COALESCE(is_deleted, false) AS is_deleted
        FROM cashbook_entries
        WHERE happened_at >= $1
          AND happened_at <  $2
        ORDER BY happened_at DESC, id DESC
        LIMIT 500
        """,
        start_utc,
        end_utc,
    )
    balance = await conn.fetchval(
        """
        SELECT
          COALESCE(SUM(CASE WHEN kind='income' THEN amount ELSE 0 END),0)
          -
          COALESCE(SUM(CASE WHEN kind='expense'
                             AND NOT (comment ILIKE '[WDR]%'
                                      OR comment ILIKE 'изъят%'
                                      OR method = 'DIV')
                            THEN amount ELSE 0 END),0)
        FROM cashbook_entries
        WHERE COALESCE(is_deleted,false)=false
        """
    )
    summary = summarize_cashbook_rows([_row_to_cashbook(row) for row in rows])
    return {
        "summary": summary,
        "balance": _decimal(balance),
        "ledger": rows,
    }


async def build_cleaning_dashboard(conn, *, start_utc: datetime, end_utc: datetime) -> dict[str, Any]:
    rows = await conn.fetch(
        """
        SELECT id, happened_at, kind, method, amount,
               COALESCE(comment, '') AS comment,
               order_id,
               (deleted_at IS NOT NULL) AS is_deleted
        FROM cleaning_cashbook
        WHERE happened_at >= $1
          AND happened_at <  $2
        ORDER BY happened_at DESC, id DESC
        LIMIT 500
        """,
        start_utc,
        end_utc,
    )
    gift_total = await conn.fetchval(
        """
        SELECT COALESCE(SUM(p.amount), 0)::numeric(12,2)
        FROM cleaning_order_payments p
        JOIN cleaning_orders o ON o.id = p.order_id
        WHERE p.method = 'Подарочный сертификат'
          AND o.happened_at >= $1
          AND o.happened_at <  $2
          AND o.deleted_at IS NULL
        """,
        start_utc,
        end_utc,
    )
    balance = await conn.fetchval(
        """
        SELECT COALESCE(SUM(
          CASE
            WHEN kind IN ('income', 'deposit') THEN amount
            WHEN kind IN ('expense', 'dividend', 'withdrawal') THEN -amount
            ELSE 0
          END
        ), 0)::numeric(12,2)
        FROM cleaning_cashbook
        WHERE deleted_at IS NULL
        """
    )
    summary = summarize_cleaning_rows([_row_to_cashbook(row) for row in rows])
    return {
        "summary": summary,
        "balance": _decimal(balance),
        "gift_total": _decimal(gift_total),
        "ledger": rows,
    }
```

- [ ] **Step 4: Run query tests and verify pass**

Run:

```bash
python -m unittest tests.test_analytics_queries -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git add analytics_app/queries.py tests/test_analytics_queries.py
git commit -m "feat: add analytics query layer"
```

---

### Task 5: HTML Templates, CSS, And Frontend Script

**Files:**
- Create: `analytics_app/templates.py`
- Create: `analytics_app/static/app.css`
- Create: `analytics_app/static/app.js`
- Test: `tests/test_analytics_templates.py`

- [ ] **Step 1: Write template smoke tests**

Create `tests/test_analytics_templates.py`:

```python
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
```

- [ ] **Step 2: Run template tests and verify failure**

Run:

```bash
python -m unittest tests.test_analytics_templates -v
```

Expected: FAIL with missing `analytics_app.templates`.

- [ ] **Step 3: Implement templates**

Create `analytics_app/templates.py`:

```python
from __future__ import annotations

import html
import json
from decimal import Decimal
from typing import Any

from .money import MoneySummary


def money(value: Decimal) -> str:
    return f"{value:,.0f}".replace(",", " ") + " ₽"


def _json_amounts(values: dict[str, Decimal]) -> str:
    return json.dumps(
        {"labels": list(values.keys()), "values": [float(v) for v in values.values()]},
        ensure_ascii=False,
    )


def render_login(*, error: bool) -> str:
    error_html = '<div class="error">Неверный логин или пароль</div>' if error else ""
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Analytics Login</title>
  <link rel="stylesheet" href="/static/app.css">
</head>
<body class="login-page">
  <main class="login-panel">
    <h1>Финансы</h1>
    <form method="post" action="/login">
      {error_html}
      <label>Логин<input name="login" autocomplete="username"></label>
      <label>Пароль<input name="password" type="password" autocomplete="current-password"></label>
      <button type="submit">Войти</button>
    </form>
  </main>
</body>
</html>"""


def _ledger_rows(ledger: list[Any]) -> str:
    rows: list[str] = []
    for row in ledger[:200]:
        happened_at = row["happened_at"]
        date_text = happened_at.strftime("%d.%m.%Y %H:%M") if happened_at else ""
        sign = "+" if row["kind"] == "income" else "-"
        rows.append(
            "<tr>"
            f"<td>{html.escape(date_text)}</td>"
            f"<td>{html.escape(str(row['kind'] or ''))}</td>"
            f"<td>{html.escape(str(row['method'] or ''))}</td>"
            f"<td>{html.escape(str(row['comment'] or ''))}</td>"
            f"<td class=\"amount\">{sign}{money(Decimal(row['amount'] or 0))}</td>"
            "</tr>"
        )
    if not rows:
        return '<tr><td colspan="5" class="muted">Операций за период нет</td></tr>'
    return "\n".join(rows)


def render_dashboard(
    *,
    title: str,
    active_section: str,
    period_label: str,
    summary: MoneySummary,
    balance: Decimal,
    ledger: list[Any],
    income_by_method: dict[str, Decimal],
    expense_by_group: dict[str, Decimal],
    extra_note: str,
) -> str:
    active_main = "active" if active_section == "main" else ""
    active_cleaning = "active" if active_section == "cleaning" else ""
    chart_data = {
        "income": json.loads(_json_amounts(income_by_method)),
        "expense": json.loads(_json_amounts(expense_by_group)),
    }
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <link rel="stylesheet" href="/static/app.css">
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
  <aside class="sidebar">
    <div class="brand">Финансы</div>
    <a class="{active_main}" href="/">Основная касса</a>
    <a class="{active_cleaning}" href="/cleaning">Клининг</a>
    <a href="/logout">Выйти</a>
  </aside>
  <main class="content">
    <header class="topbar">
      <div>
        <h1>{html.escape(title)}</h1>
        <p>{html.escape(period_label)}</p>
      </div>
      <form class="filters" method="get">
        <button name="period" value="today">Сегодня</button>
        <button name="period" value="week">Неделя</button>
        <button name="period" value="month">Месяц</button>
        <button name="period" value="year">Год</button>
      </form>
    </header>
    <section class="metrics">
      <div><span>Приход</span><strong>{money(summary.income)}</strong></div>
      <div><span>Расход</span><strong>{money(summary.expense)}</strong></div>
      <div><span>Прибыль</span><strong>{money(summary.profit)}</strong></div>
      <div><span>Остаток</span><strong>{money(balance)}</strong></div>
    </section>
    <p class="note">{html.escape(extra_note)}</p>
    <section class="charts">
      <div><h2>Доходы</h2><canvas id="incomeChart"></canvas></div>
      <div><h2>Расходы</h2><canvas id="expenseChart"></canvas></div>
    </section>
    <section>
      <h2>Детализация операций</h2>
      <input class="table-search" id="ledgerSearch" placeholder="Поиск по комментарию">
      <table id="ledgerTable">
        <thead><tr><th>Дата</th><th>Тип</th><th>Метод</th><th>Комментарий</th><th>Сумма</th></tr></thead>
        <tbody>{_ledger_rows(ledger)}</tbody>
      </table>
    </section>
  </main>
  <script id="chart-data" type="application/json">{html.escape(json.dumps(chart_data, ensure_ascii=False))}</script>
  <script src="/static/app.js"></script>
</body>
</html>"""
```

- [ ] **Step 4: Implement CSS**

Create `analytics_app/static/app.css`:

```css
* { box-sizing: border-box; }
body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: #18202f; background: #f5f7fb; }
a { color: inherit; text-decoration: none; }
.sidebar { position: fixed; inset: 0 auto 0 0; width: 220px; padding: 20px 14px; background: #18202f; color: white; }
.brand { font-size: 20px; font-weight: 700; margin-bottom: 22px; }
.sidebar a { display: block; padding: 10px 12px; border-radius: 6px; margin-bottom: 6px; color: #dbe4f0; }
.sidebar a.active, .sidebar a:hover { background: #2d3a52; color: white; }
.content { margin-left: 220px; padding: 24px; }
.topbar { display: flex; justify-content: space-between; gap: 16px; align-items: flex-start; margin-bottom: 18px; }
h1 { margin: 0 0 4px; font-size: 28px; }
h2 { margin: 0 0 12px; font-size: 18px; }
p { margin: 0; color: #667085; }
.filters { display: flex; gap: 8px; flex-wrap: wrap; }
button { border: 0; background: #18202f; color: white; border-radius: 6px; padding: 9px 12px; cursor: pointer; }
.metrics { display: grid; grid-template-columns: repeat(4, minmax(160px, 1fr)); gap: 12px; margin-bottom: 12px; }
.metrics div, .charts div, section { background: white; border: 1px solid #e1e6ef; border-radius: 8px; padding: 16px; }
.metrics span { display: block; color: #667085; margin-bottom: 8px; }
.metrics strong { font-size: 24px; }
.note { margin: 0 0 14px; }
.charts { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 12px; }
canvas { max-height: 260px; }
.table-search { width: 100%; border: 1px solid #ccd4e0; border-radius: 6px; padding: 10px 12px; margin-bottom: 10px; }
table { width: 100%; border-collapse: collapse; background: white; }
th, td { border-bottom: 1px solid #edf0f5; padding: 10px 8px; text-align: left; vertical-align: top; }
th { color: #667085; font-weight: 600; }
.amount { text-align: right; white-space: nowrap; }
.muted { color: #667085; text-align: center; }
.login-page { min-height: 100vh; display: grid; place-items: center; }
.login-panel { width: min(380px, calc(100vw - 32px)); background: white; border: 1px solid #e1e6ef; border-radius: 8px; padding: 24px; }
.login-panel h1 { margin-bottom: 18px; }
label { display: block; color: #344054; margin-bottom: 12px; }
label input { width: 100%; border: 1px solid #ccd4e0; border-radius: 6px; padding: 10px 12px; margin-top: 6px; }
.error { background: #fff1f0; color: #b42318; border: 1px solid #ffd0cc; border-radius: 6px; padding: 10px; margin-bottom: 12px; }
@media (max-width: 880px) {
  .sidebar { position: static; width: auto; }
  .content { margin-left: 0; padding: 16px; }
  .topbar { display: block; }
  .filters { margin-top: 12px; }
  .metrics, .charts { grid-template-columns: 1fr; }
}
```

- [ ] **Step 5: Implement frontend script**

Create `analytics_app/static/app.js`:

```javascript
(function () {
  const raw = document.getElementById("chart-data");
  if (raw && window.Chart) {
    const data = JSON.parse(raw.textContent);
    const makeChart = (id, title, payload) => {
      const node = document.getElementById(id);
      if (!node) return;
      new Chart(node, {
        type: "bar",
        data: {
          labels: payload.labels,
          datasets: [{ label: title, data: payload.values, backgroundColor: "#4f7cff" }]
        },
        options: { responsive: true, plugins: { legend: { display: false } } }
      });
    };
    makeChart("incomeChart", "Доходы", data.income);
    makeChart("expenseChart", "Расходы", data.expense);
  }

  const search = document.getElementById("ledgerSearch");
  const table = document.getElementById("ledgerTable");
  if (search && table) {
    search.addEventListener("input", function () {
      const needle = search.value.toLowerCase();
      for (const row of table.querySelectorAll("tbody tr")) {
        row.style.display = row.textContent.toLowerCase().includes(needle) ? "" : "none";
      }
    });
  }
})();
```

- [ ] **Step 6: Run template tests and verify pass**

Run:

```bash
python -m unittest tests.test_analytics_templates -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

Run:

```bash
git add analytics_app/templates.py analytics_app/static/app.css analytics_app/static/app.js tests/test_analytics_templates.py
git commit -m "feat: add analytics dashboard templates"
```

---

### Task 6: Aiohttp Server, Routes, And Auth Middleware

**Files:**
- Create: `analytics_app/server.py`
- Test: `tests/test_analytics_server.py`
- Modify: `analytics_app/__main__.py` if import paths need adjustment.

- [ ] **Step 1: Write server smoke tests**

Create `tests/test_analytics_server.py`:

```python
import unittest
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from aiohttp.test_utils import AioHTTPTestCase
from aiohttp import web

from analytics_app.auth import make_password_hash
from analytics_app.config import AnalyticsConfig
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


class FakeQueryService:
    def __init__(self):
        self.pool = FakePool()

    def acquire(self):
        return FakeAcquire()

    async def main(self, conn, *, start_utc, end_utc):
        return {
            "summary": MoneySummary(income=Decimal("1000"), expense=Decimal("200"), profit=Decimal("800")),
            "balance": Decimal("500"),
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

    async def cleaning(self, conn, *, start_utc, end_utc):
        return {
            "summary": MoneySummary(income=Decimal("2000"), expense=Decimal("500"), profit=Decimal("1500")),
            "balance": Decimal("1500"),
            "gift_total": Decimal("0"),
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
```

- [ ] **Step 2: Run server tests and verify failure**

Run:

```bash
python -m unittest tests.test_analytics_server -v
```

Expected: FAIL with missing `analytics_app.server`.

- [ ] **Step 3: Implement server**

Create `analytics_app/server.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable

import asyncpg
from aiohttp import web

from .auth import sign_session, unsign_session, verify_password
from .config import AnalyticsConfig
from .dates import resolve_date_range
from .queries import build_cleaning_dashboard, build_main_cash_dashboard
from .templates import render_dashboard, render_login


SESSION_COOKIE = "analytics_session"


@dataclass
class QueryService:
    pool: asyncpg.Pool

    def acquire(self):
        return self.pool.acquire()

    async def main(self, conn, *, start_utc, end_utc):
        return await build_main_cash_dashboard(conn, start_utc=start_utc, end_utc=end_utc)

    async def cleaning(self, conn, *, start_utc, end_utc):
        return await build_cleaning_dashboard(conn, start_utc=start_utc, end_utc=end_utc)


@web.middleware
async def auth_middleware(request: web.Request, handler: Callable[[web.Request], Awaitable[web.StreamResponse]]):
    if request.path in {"/login"} or request.path.startswith("/static/"):
        return await handler(request)
    cfg: AnalyticsConfig = request.app["config"]
    login = unsign_session(request.cookies.get(SESSION_COOKIE, ""), secret=cfg.session_secret)
    if not login or login not in cfg.users:
        raise web.HTTPFound("/login")
    request["login"] = login
    return await handler(request)


async def login_get(request: web.Request) -> web.Response:
    return web.Response(text=render_login(error=False), content_type="text/html")


async def login_post(request: web.Request) -> web.Response:
    cfg: AnalyticsConfig = request.app["config"]
    data = await request.post()
    login = str(data.get("login") or "")
    password = str(data.get("password") or "")
    password_hash = cfg.users.get(login)
    if not password_hash or not verify_password(password, password_hash):
        return web.Response(text=render_login(error=True), content_type="text/html", status=401)

    resp = web.HTTPFound("/")
    resp.set_cookie(
        SESSION_COOKIE,
        sign_session(login, secret=cfg.session_secret),
        httponly=True,
        secure=True,
        samesite="Lax",
        max_age=60 * 60 * 24 * 14,
    )
    raise resp


async def logout(request: web.Request) -> web.Response:
    resp = web.HTTPFound("/login")
    resp.del_cookie(SESSION_COOKIE)
    raise resp


def _period_label(rng) -> str:
    return f"{rng.start_date:%d.%m.%Y}-{rng.end_date:%d.%m.%Y}"


async def main_cash(request: web.Request) -> web.Response:
    rng = resolve_date_range(request.query)
    query_service = request.app["query_service"]
    async with query_service.acquire() as conn:
        data = await query_service.main(conn, start_utc=rng.start_utc, end_utc=rng.end_utc)
    summary = data["summary"]
    html = render_dashboard(
        title="Основная касса",
        active_section="main",
        period_label=_period_label(rng),
        summary=summary,
        balance=data["balance"],
        ledger=data["ledger"],
        income_by_method=summary.income_by_method,
        expense_by_group=summary.expense_by_group,
        extra_note="Расходы основной кассы без строгих категорий: используйте поиск по комментарию.",
    )
    return web.Response(text=html, content_type="text/html")


async def cleaning(request: web.Request) -> web.Response:
    rng = resolve_date_range(request.query)
    query_service = request.app["query_service"]
    async with query_service.acquire() as conn:
        data = await query_service.cleaning(conn, start_utc=rng.start_utc, end_utc=rng.end_utc)
    summary = data["summary"]
    html = render_dashboard(
        title="Клининг",
        active_section="cleaning",
        period_label=_period_label(rng),
        summary=summary,
        balance=data["balance"],
        ledger=data["ledger"],
        income_by_method=summary.income_by_method,
        expense_by_group=summary.expense_by_group,
        extra_note=f"Подарочные сертификаты за период: {data['gift_total']} ₽.",
    )
    return web.Response(text=html, content_type="text/html")


async def _close_pool(app: web.Application) -> None:
    query_service = app["query_service"]
    pool = getattr(query_service, "pool", None)
    if pool is not None:
        await pool.close()


def create_app(cfg: AnalyticsConfig, *, query_service=None) -> web.Application:
    app = web.Application(middlewares=[auth_middleware])
    app["config"] = cfg
    if query_service is None:
        async def init_pool(app: web.Application) -> None:
            pool = await asyncpg.create_pool(cfg.db_dsn, min_size=1, max_size=5)
            app["query_service"] = QueryService(pool)
        app.on_startup.append(init_pool)
    else:
        app["query_service"] = query_service
    app.on_cleanup.append(_close_pool)

    static_dir = Path(__file__).with_name("static")
    app.router.add_static("/static/", static_dir)
    app.router.add_get("/login", login_get)
    app.router.add_post("/login", login_post)
    app.router.add_get("/logout", logout)
    app.router.add_get("/", main_cash)
    app.router.add_get("/cleaning", cleaning)
    return app
```

- [ ] **Step 4: Run server tests and verify pass**

Run:

```bash
python -m unittest tests.test_analytics_server -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git add analytics_app/server.py tests/test_analytics_server.py analytics_app/__main__.py
git commit -m "feat: add analytics web server"
```

---

### Task 7: Local Verification And Runtime Smoke

**Files:**
- No new files required.

- [ ] **Step 1: Run all analytics unit tests**

Run:

```bash
python -m unittest \
  tests.test_analytics_config \
  tests.test_analytics_auth \
  tests.test_analytics_dates \
  tests.test_analytics_money \
  tests.test_analytics_queries \
  tests.test_analytics_templates \
  tests.test_analytics_server \
  -v
```

Expected: PASS.

- [ ] **Step 2: Run existing focused tests to guard adjacent flows**

Run:

```bash
python -m unittest tests.test_cleaning_helpers tests.test_amocrm tests.test_amocrm_api tests.test_amocrm_webhook_route -v
```

Expected: PASS.

- [ ] **Step 3: Run py_compile**

Run:

```bash
python -m py_compile analytics_app/*.py
```

Expected: exits 0.

- [ ] **Step 4: Generate a password hash for manual local smoke**

Run:

```bash
python - <<'PY'
from analytics_app.auth import make_password_hash
print(make_password_hash("owner-password"))
PY
```

Expected: prints a value starting with `pbkdf2_sha256$`.

- [ ] **Step 5: Start local app if a DB DSN is available**

Run with real local/prod-tunnel DSN only when available:

```bash
ANALYTICS_SESSION_SECRET=local-secret \
ANALYTICS_USERS='owner:<hash-from-step-4>' \
ANALYTICS_BIND_PORT=8090 \
DB_DSN='<existing-dsn>' \
python -m analytics_app
```

Expected: app starts on `http://127.0.0.1:8090`.

- [ ] **Step 6: Commit verification-only adjustments if any were required**

If no code changes were required after tests, do not create a commit. If a fix was required, commit only the changed implementation/test files:

```bash
git add analytics_app tests
git commit -m "fix: stabilize analytics dashboard smoke"
```

---

### Task 8: Deployment Notes For Production Setup

**Files:**
- Create: `docs/analytics_deploy.md`

- [ ] **Step 1: Write deployment runbook**

Create `docs/analytics_deploy.md`:

```markdown
# Analytics Dashboard Deploy Runbook

Production URL:

```text
https://analytics.dastydev.ru
```

DNS:

```text
analytics.dastydev.ru A 91.200.150.68
```

Runtime:

- service path: `/opt/telegram-analytics`
- bind: `127.0.0.1:8090`
- systemd unit: `telegram-analytics.service`
- app command: `python -m analytics_app`

Required environment:

```text
ANALYTICS_DB_DSN=<read-only postgres dsn, or DB_DSN if read-only role is not ready>
ANALYTICS_SESSION_SECRET=<long random secret>
ANALYTICS_USERS=owner:<pbkdf2_hash>,partner:<pbkdf2_hash>
ANALYTICS_BIND_HOST=127.0.0.1
ANALYTICS_BIND_PORT=8090
```

Generate password hash:

```bash
python - <<'PY'
from analytics_app.auth import make_password_hash
print(make_password_hash("replace-this-password"))
PY
```

Nginx shape:

```nginx
server {
    server_name analytics.dastydev.ru;

    location / {
        proxy_pass http://127.0.0.1:8090;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Production verification:

```bash
getent hosts analytics.dastydev.ru
sudo nginx -t
sudo systemctl status telegram-analytics.service --no-pager
curl -I -fsS https://analytics.dastydev.ru/login
```

Before declaring ready, compare dashboard totals for one period against direct SQL.
```

- [ ] **Step 2: Commit deployment runbook**

Run:

```bash
git add docs/analytics_deploy.md
git commit -m "docs: add analytics deploy runbook"
```

---

## Self-Review Checklist

- Spec coverage: the plan implements separate read-only service, login/password, main cash first, cleaning navigation, direct DB reads, main cash without strict expense categories, cleaning expense categories, and `analytics.dastydev.ru` deployment notes.
- Placeholder scan: the plan contains no `TBD`, no unconstrained "add tests" steps, and no unspecified files.
- Type consistency: `AnalyticsConfig`, `MoneySummary`, `CashbookRow`, `DateRange`, and `QueryService` are introduced before use in later tasks.
- Scope: web editing, recategorization, public registration, combined totals, and deployment execution are outside implementation MVP; deployment runbook is included for the next operational step.
