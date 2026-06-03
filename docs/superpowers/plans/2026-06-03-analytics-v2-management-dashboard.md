# Analytics v2 Management Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the cashbook-first analytics page with an owner-level management dashboard showing gross checks, live money, salary load, bonus loss, grouped expenses, largest expenses, and actionable charts.

**Architecture:** Keep `analytics_app` as a separate aiohttp service. Add pure calculation modules for expense grouping and management metrics, extend SQL queries to fetch orders/payroll/cashbook data, then render a richer server-side dashboard with Chart.js payloads. Preserve the raw ledger as drill-down below analytics sections.

**Tech Stack:** Python 3.10+, aiohttp, asyncpg, unittest, server-rendered HTML, vanilla JS, Chart.js.

---

## File Structure

- Create `analytics_app/expense_groups.py`: deterministic expense classification rules from method/comment.
- Create `analytics_app/management.py`: dataclasses and pure functions for ratios, buckets, salary analytics, bonus analytics, top expenses, waterfall, and chart payloads.
- Modify `analytics_app/dates.py`: support `date_from`/`date_to` aliases and automatic bucket choice.
- Modify `analytics_app/queries.py`: fetch order, payroll, cashbook, and cleaning rows needed by v2.
- Modify `analytics_app/server.py`: route main/cleaning pages through v2 dashboard data.
- Modify `analytics_app/templates.py`: render management dashboard, date form, analytics sections, charts, and drill-down tables.
- Modify `analytics_app/static/app.js`: render multiple chart types and table filters.
- Modify `analytics_app/static/app.css`: dashboard layout for dense management analytics.
- Add/modify tests:
  - `tests/test_analytics_dates.py`
  - `tests/test_analytics_expense_groups.py`
  - `tests/test_analytics_management.py`
  - `tests/test_analytics_queries.py`
  - `tests/test_analytics_templates.py`
  - `tests/test_analytics_server.py`

---

### Task 1: Period Controls And Bucket Selection

**Files:**
- Modify: `analytics_app/dates.py`
- Test: `tests/test_analytics_dates.py`

- [ ] **Step 1: Add failing tests for `date_from`/`date_to` and automatic buckets**

Append these tests to `tests/test_analytics_dates.py`:

```python
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
```

- [ ] **Step 2: Run date tests and verify failure**

Run:

```bash
python3.11 -m unittest tests.test_analytics_dates
```

Expected: tests fail because `date_from`/`date_to` are ignored and custom ranges default to `day`.

- [ ] **Step 3: Implement aliases and automatic bucket choice**

In `analytics_app/dates.py`, add:

```python
def _auto_group_by(start_date: date, end_date: date) -> str:
    days = (end_date - start_date).days + 1
    if days > 180:
        return "month"
    if days > 45:
        return "week"
    return "day"
```

Update `resolve_date_range()` so custom dates read both old and new parameter names:

```python
    explicit_group = params.get("group") if params.get("group") in {"day", "week", "month"} else None
    custom_start = _parse_date(params.get("start") or params.get("date_from"))
    custom_end = _parse_date(params.get("end") or params.get("date_to"))
    if custom_start and custom_end and custom_start <= custom_end:
        start_date, end_date = custom_start, custom_end
        group_by = explicit_group or _auto_group_by(start_date, end_date)
        start_utc, end_utc = _bounds(start_date, end_date)
        return DateRange(start_date, end_date, start_utc, end_utc, group_by)
```

Keep preset behavior, but use `explicit_group` where present and `_auto_group_by()` otherwise:

```python
    if explicit_group:
        group_by = explicit_group
    else:
        group_by = _auto_group_by(start_date, end_date)
```

- [ ] **Step 4: Run date tests and verify pass**

Run:

```bash
python3.11 -m unittest tests.test_analytics_dates
```

Expected: all date tests pass.

- [ ] **Step 5: Commit**

```bash
git add analytics_app/dates.py tests/test_analytics_dates.py
git commit -m "feat: add analytics custom date buckets"
```

---

### Task 2: Expense Grouping Rules

**Files:**
- Create: `analytics_app/expense_groups.py`
- Test: `tests/test_analytics_expense_groups.py`

- [ ] **Step 1: Write failing tests for deterministic classification**

Create `tests/test_analytics_expense_groups.py`:

```python
import unittest
from decimal import Decimal

from analytics_app.expense_groups import (
    ExpenseGroup,
    classify_expense,
    group_expenses,
)
from analytics_app.management import ExpenseRow


class AnalyticsExpenseGroupTests(unittest.TestCase):
    def test_classifies_known_keywords(self):
        self.assertEqual(classify_expense(method="прочее", comment="Купили химию"), "Материалы/химия")
        self.assertEqual(classify_expense(method="прочее", comment="Авито реклама"), "Реклама/маркетинг")
        self.assertEqual(classify_expense(method="прочее", comment="Бензин мастер"), "Топливо/транспорт")
        self.assertEqual(classify_expense(method="DIV", comment="[DIV] Дивиденды"), "Дивиденды/изъятия")
        self.assertEqual(classify_expense(method="Наличные", comment="[WDR] Изъятие"), "Дивиденды/изъятия")

    def test_uncategorized_remains_visible(self):
        self.assertEqual(classify_expense(method="прочее", comment="непонятный расход"), "Без категории")

    def test_group_expenses_returns_amount_share_count_and_examples(self):
        rows = [
            ExpenseRow(id=1, happened_at=None, amount=Decimal("1000"), method="прочее", comment="Химия"),
            ExpenseRow(id=2, happened_at=None, amount=Decimal("500"), method="прочее", comment="Химия перчатки"),
            ExpenseRow(id=3, happened_at=None, amount=Decimal("500"), method="прочее", comment="Непонятно"),
        ]

        groups = group_expenses(rows)

        self.assertEqual(groups[0], ExpenseGroup(name="Материалы/химия", amount=Decimal("1500"), share=Decimal("0.75"), count=2, examples=["Химия", "Химия перчатки"]))
        self.assertEqual(groups[1].name, "Без категории")
        self.assertEqual(groups[1].share, Decimal("0.25"))


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
python3.11 -m unittest tests.test_analytics_expense_groups
```

Expected: import failure because `analytics_app.expense_groups` and `ExpenseRow` do not exist.

- [ ] **Step 3: Add the initial `ExpenseRow` data model in `analytics_app/management.py`**

Create `analytics_app/management.py` with:

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class ExpenseRow:
    id: int
    happened_at: datetime | None
    amount: Decimal
    method: str
    comment: str
```

- [ ] **Step 4: Implement grouping**

Create `analytics_app/expense_groups.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP

from .management import ExpenseRow


@dataclass(frozen=True)
class ExpenseGroup:
    name: str
    amount: Decimal
    share: Decimal
    count: int
    examples: list[str]


RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Дивиденды/изъятия", ("[div]", " div", "[wdr]", "изъят")),
    ("Топливо/транспорт", ("бенз", "топлив", "дорог", "такси")),
    ("Материалы/химия", ("хим", "средств", "расходник", "перчат", "пакет")),
    ("Реклама/маркетинг", ("реклам", "авито", "директ", "таргет", "лид")),
    ("Аренда/коммунальные", ("аренд", "офис", "склад", "коммун")),
    ("Связь/софт", ("связ", "телефон", "интернет", "софт", "crm", "wahelp")),
    ("Возвраты/коррекции", ("возврат", "коррект")),
)


def classify_expense(*, method: str, comment: str) -> str:
    haystack = f"{method or ''} {comment or ''}".lower()
    if (method or "").upper() == "DIV":
        return "Дивиденды/изъятия"
    for label, needles in RULES:
        if any(needle in haystack for needle in needles):
            return label
    if method and method.lower() not in {"прочее", "наличные", "карта"}:
        return method
    return "Без категории"


def _ratio(value: Decimal, total: Decimal) -> Decimal:
    if total == 0:
        return Decimal("0")
    return (value / total).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def group_expenses(rows: list[ExpenseRow]) -> list[ExpenseGroup]:
    total = sum((row.amount for row in rows), Decimal("0"))
    buckets: dict[str, list[ExpenseRow]] = {}
    for row in rows:
        label = classify_expense(method=row.method, comment=row.comment)
        buckets.setdefault(label, []).append(row)

    groups = [
        ExpenseGroup(
            name=name,
            amount=sum((row.amount for row in items), Decimal("0")),
            share=_ratio(sum((row.amount for row in items), Decimal("0")), total),
            count=len(items),
            examples=[row.comment for row in items[:3] if row.comment],
        )
        for name, items in buckets.items()
    ]
    return sorted(groups, key=lambda item: item.amount, reverse=True)
```

- [ ] **Step 5: Run tests and verify pass**

Run:

```bash
python3.11 -m unittest tests.test_analytics_expense_groups
```

Expected: all expense grouping tests pass.

- [ ] **Step 6: Commit**

```bash
git add analytics_app/expense_groups.py analytics_app/management.py tests/test_analytics_expense_groups.py
git commit -m "feat: group analytics expenses"
```

---

### Task 3: Pure Management Metrics

**Files:**
- Modify: `analytics_app/management.py`
- Test: `tests/test_analytics_management.py`

- [ ] **Step 1: Write failing tests for salary, bonus, top expenses, and waterfall**

Create `tests/test_analytics_management.py`:

```python
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
            OrderMetricRow(id=1, created_at=datetime(2026, 6, 1, tzinfo=ZoneInfo("UTC")), master_id=10, master_name="Анна", amount_total=Decimal("5000"), amount_cash=Decimal("4000"), bonus_spent=Decimal("1000"), bonus_earned=Decimal("400")),
            OrderMetricRow(id=2, created_at=datetime(2026, 6, 2, tzinfo=ZoneInfo("UTC")), master_id=11, master_name="Борис", amount_total=Decimal("3000"), amount_cash=Decimal("3000"), bonus_spent=Decimal("0"), bonus_earned=Decimal("300")),
        ]
        payroll = [
            PayrollMetricRow(order_id=1, master_id=10, master_name="Анна", base_pay=Decimal("1000"), fuel_pay=Decimal("150"), upsell_pay=Decimal("0"), total_pay=Decimal("1150")),
            PayrollMetricRow(order_id=2, master_id=11, master_name="Борис", base_pay=Decimal("1000"), fuel_pay=Decimal("150"), upsell_pay=Decimal("500"), total_pay=Decimal("1650")),
        ]
        expenses = [
            ExpenseRow(id=1, happened_at=datetime(2026, 6, 2, tzinfo=ZoneInfo("UTC")), amount=Decimal("700"), method="прочее", comment="Химия"),
            ExpenseRow(id=2, happened_at=datetime(2026, 6, 3, tzinfo=ZoneInfo("UTC")), amount=Decimal("300"), method="прочее", comment="Авито реклама"),
        ]

        dashboard = build_management_dashboard(orders=orders, payroll=payroll, expenses=expenses, group_by="day")

        self.assertEqual(dashboard.gross_checks, Decimal("8000"))
        self.assertEqual(dashboard.live_money, Decimal("7000"))
        self.assertEqual(dashboard.bonuses_spent, Decimal("1000"))
        self.assertEqual(dashboard.bonus_loss_percent, Decimal("0.13"))
        self.assertEqual(dashboard.salary_total, Decimal("2800"))
        self.assertEqual(dashboard.salary_percent, Decimal("0.35"))
        self.assertEqual(dashboard.other_expenses, Decimal("1000"))
        self.assertEqual(dashboard.operating_profit, Decimal("3200"))
        self.assertEqual(dashboard.waterfall["gross_checks"], Decimal("8000"))
        self.assertEqual(dashboard.waterfall["operating_profit"], Decimal("3200"))

    def test_top_expenses_sorted_descending(self):
        expenses = [
            ExpenseRow(id=1, happened_at=None, amount=Decimal("10"), method="прочее", comment="small"),
            ExpenseRow(id=2, happened_at=None, amount=Decimal("50"), method="прочее", comment="large"),
            ExpenseRow(id=3, happened_at=None, amount=Decimal("30"), method="прочее", comment="mid"),
        ]

        dashboard = build_management_dashboard(orders=[], payroll=[], expenses=expenses, group_by="day")

        self.assertEqual([row.id for row in dashboard.top_expenses], [2, 3, 1])
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
python3.11 -m unittest tests.test_analytics_management
```

Expected: import failure for missing dataclasses/functions.

- [ ] **Step 3: Implement dataclasses and metric function**

Extend `analytics_app/management.py`:

```python
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

from .expense_groups import ExpenseGroup, group_expenses

ZERO = Decimal("0")


@dataclass(frozen=True)
class OrderMetricRow:
    id: int
    created_at: datetime
    master_id: int | None
    master_name: str
    amount_total: Decimal
    amount_cash: Decimal
    bonus_spent: Decimal
    bonus_earned: Decimal


@dataclass(frozen=True)
class PayrollMetricRow:
    order_id: int
    master_id: int | None
    master_name: str
    base_pay: Decimal
    fuel_pay: Decimal
    upsell_pay: Decimal
    total_pay: Decimal


@dataclass(frozen=True)
class MasterSalarySummary:
    master_id: int | None
    master_name: str
    orders_count: int
    gross_checks: Decimal
    live_money: Decimal
    salary: Decimal
    salary_percent: Decimal


@dataclass(frozen=True)
class ManagementDashboard:
    gross_checks: Decimal
    live_money: Decimal
    bonuses_spent: Decimal
    bonuses_earned: Decimal
    bonus_loss_percent: Decimal
    salary_total: Decimal
    salary_percent: Decimal
    salary_base: Decimal
    salary_fuel: Decimal
    salary_upsell: Decimal
    other_expenses: Decimal
    operating_profit: Decimal
    expense_groups: list[ExpenseGroup] = field(default_factory=list)
    top_expenses: list[ExpenseRow] = field(default_factory=list)
    master_salaries: list[MasterSalarySummary] = field(default_factory=list)
    waterfall: dict[str, Decimal] = field(default_factory=dict)
    charts: dict[str, object] = field(default_factory=dict)


def ratio(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator == 0:
        return ZERO
    return (numerator / denominator).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _build_master_salaries(orders: list[OrderMetricRow], payroll: list[PayrollMetricRow], gross_checks: Decimal) -> list[MasterSalarySummary]:
    order_bucket: dict[int | None, list[OrderMetricRow]] = {}
    payroll_bucket: dict[int | None, list[PayrollMetricRow]] = {}
    names: dict[int | None, str] = {}
    for row in orders:
        order_bucket.setdefault(row.master_id, []).append(row)
        names[row.master_id] = row.master_name or "Без мастера"
    for row in payroll:
        payroll_bucket.setdefault(row.master_id, []).append(row)
        names[row.master_id] = row.master_name or "Без мастера"
    result: list[MasterSalarySummary] = []
    for master_id in set(order_bucket) | set(payroll_bucket):
        master_orders = order_bucket.get(master_id, [])
        master_payroll = payroll_bucket.get(master_id, [])
        salary = sum((row.total_pay for row in master_payroll), ZERO)
        result.append(
            MasterSalarySummary(
                master_id=master_id,
                master_name=names.get(master_id) or "Без мастера",
                orders_count=len(master_orders),
                gross_checks=sum((row.amount_total for row in master_orders), ZERO),
                live_money=sum((row.amount_cash for row in master_orders), ZERO),
                salary=salary,
                salary_percent=ratio(salary, gross_checks),
            )
        )
    return sorted(result, key=lambda item: item.salary, reverse=True)


def build_management_dashboard(
    *,
    orders: list[OrderMetricRow],
    payroll: list[PayrollMetricRow],
    expenses: list[ExpenseRow],
    group_by: str,
) -> ManagementDashboard:
    gross_checks = sum((row.amount_total for row in orders), ZERO)
    live_money = sum((row.amount_cash for row in orders), ZERO)
    bonuses_spent = sum((row.bonus_spent for row in orders), ZERO)
    bonuses_earned = sum((row.bonus_earned for row in orders), ZERO)
    salary_total = sum((row.total_pay for row in payroll), ZERO)
    salary_base = sum((row.base_pay for row in payroll), ZERO)
    salary_fuel = sum((row.fuel_pay for row in payroll), ZERO)
    salary_upsell = sum((row.upsell_pay for row in payroll), ZERO)
    other_expenses = sum((row.amount for row in expenses), ZERO)
    operating_profit = live_money - salary_total - other_expenses
    waterfall = {
        "gross_checks": gross_checks,
        "bonuses_spent": -bonuses_spent,
        "live_money": live_money,
        "salaries": -salary_total,
        "other_expenses": -other_expenses,
        "operating_profit": operating_profit,
    }
    return ManagementDashboard(
        gross_checks=gross_checks,
        live_money=live_money,
        bonuses_spent=bonuses_spent,
        bonuses_earned=bonuses_earned,
        bonus_loss_percent=ratio(bonuses_spent, gross_checks),
        salary_total=salary_total,
        salary_percent=ratio(salary_total, gross_checks),
        salary_base=salary_base,
        salary_fuel=salary_fuel,
        salary_upsell=salary_upsell,
        other_expenses=other_expenses,
        operating_profit=operating_profit,
        expense_groups=group_expenses(expenses),
        top_expenses=sorted(expenses, key=lambda row: row.amount, reverse=True)[:10],
        master_salaries=_build_master_salaries(orders, payroll, gross_checks),
        waterfall=waterfall,
        charts={},
    )
```

Resolve the circular import by keeping `ExpenseRow` above the `expense_groups` import or by importing `group_expenses` inside `build_management_dashboard()`.

- [ ] **Step 4: Run management and expense grouping tests**

Run:

```bash
python3.11 -m unittest tests.test_analytics_management tests.test_analytics_expense_groups
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add analytics_app/management.py tests/test_analytics_management.py
git commit -m "feat: calculate analytics management metrics"
```

---

### Task 4: Query Layer For Orders, Payroll, Expenses, And Cleaning

**Files:**
- Modify: `analytics_app/queries.py`
- Test: `tests/test_analytics_queries.py`

- [ ] **Step 1: Add fake connection support for multiple query results**

In `tests/test_analytics_queries.py`, replace `FakeConn` with:

```python
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
```

- [ ] **Step 2: Add failing test for management dashboard data**

Append:

```python
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
```

- [ ] **Step 3: Run query tests and verify failure**

Run:

```bash
python3.11 -m unittest tests.test_analytics_queries
```

Expected: failure because `build_main_cash_dashboard()` does not return `management`.

- [ ] **Step 4: Implement order/payroll/expense fetches**

In `analytics_app/queries.py` import management dataclasses:

```python
from .management import ExpenseRow, OrderMetricRow, PayrollMetricRow, build_management_dashboard
```

Add row converters:

```python
def _row_to_order_metric(row: Any) -> OrderMetricRow:
    return OrderMetricRow(
        id=int(_get(row, "id", 0)),
        created_at=_get(row, "created_at"),
        master_id=_get(row, "master_id"),
        master_name=str(_get(row, "master_name", "Без мастера") or "Без мастера"),
        amount_total=_decimal(_get(row, "amount_total", 0)),
        amount_cash=_decimal(_get(row, "amount_cash", 0)),
        bonus_spent=_decimal(_get(row, "bonus_spent", 0)),
        bonus_earned=_decimal(_get(row, "bonus_earned", 0)),
    )


def _row_to_payroll_metric(row: Any) -> PayrollMetricRow:
    return PayrollMetricRow(
        order_id=int(_get(row, "order_id", 0)),
        master_id=_get(row, "master_id"),
        master_name=str(_get(row, "master_name", "Без мастера") or "Без мастера"),
        base_pay=_decimal(_get(row, "base_pay", 0)),
        fuel_pay=_decimal(_get(row, "fuel_pay", 0)),
        upsell_pay=_decimal(_get(row, "upsell_pay", 0)),
        total_pay=_decimal(_get(row, "total_pay", 0)),
    )


def _row_to_expense_metric(row: Any) -> ExpenseRow:
    return ExpenseRow(
        id=int(_get(row, "id", 0)),
        happened_at=_get(row, "happened_at"),
        amount=_decimal(_get(row, "amount", 0)),
        method=str(_get(row, "method", "")),
        comment=str(_get(row, "comment", "")),
    )
```

Update `build_main_cash_dashboard()` to fetch:

```sql
SELECT o.id, o.created_at, o.master_id,
       TRIM(COALESCE(s.first_name, '') || ' ' || COALESCE(s.last_name, '')) AS master_name,
       COALESCE(o.amount_total, 0) AS amount_total,
       COALESCE(o.amount_cash, 0) AS amount_cash,
       COALESCE(o.bonus_spent, 0) AS bonus_spent,
       COALESCE(o.bonus_earned, 0) AS bonus_earned
FROM orders o
LEFT JOIN staff s ON s.id = o.master_id
WHERE o.created_at >= $1
  AND o.created_at <  $2
ORDER BY o.created_at DESC, o.id DESC
```

Fetch payroll:

```sql
SELECT pi.order_id, pi.master_id,
       TRIM(COALESCE(s.first_name, '') || ' ' || COALESCE(s.last_name, '')) AS master_name,
       COALESCE(pi.base_pay, 0) AS base_pay,
       COALESCE(pi.fuel_pay, 0) AS fuel_pay,
       COALESCE(pi.upsell_pay, 0) AS upsell_pay,
       COALESCE(pi.total_pay, 0) AS total_pay
FROM payroll_items pi
JOIN orders o ON o.id = pi.order_id
LEFT JOIN staff s ON s.id = pi.master_id
WHERE o.created_at >= $1
  AND o.created_at <  $2
ORDER BY o.created_at DESC, pi.order_id DESC
```

Fetch operating expenses from cashbook:

```sql
SELECT id, happened_at, kind, method, amount,
       COALESCE(comment, '') AS comment,
       order_id, master_id,
       COALESCE(is_deleted, false) AS is_deleted
FROM cashbook_entries
WHERE happened_at >= $1
  AND happened_at <  $2
  AND kind = 'expense'
  AND COALESCE(is_deleted,false)=false
  AND order_id IS NULL
ORDER BY happened_at DESC, id DESC
LIMIT 500
```

Return:

```python
    management = build_management_dashboard(
        orders=[_row_to_order_metric(row) for row in order_rows],
        payroll=[_row_to_payroll_metric(row) for row in payroll_rows],
        expenses=[_row_to_expense_metric(row) for row in rows if not _row_to_cashbook(row).is_deleted],
        group_by="day",
    )
```

Update the `build_main_cash_dashboard()` signature now so the caller can pass the selected bucket:

```python
async def build_main_cash_dashboard(conn, *, start_utc: datetime, end_utc: datetime, group_by: str = "day") -> dict[str, Any]:
```

Use that value in the management calculation:

```python
    management = build_management_dashboard(
        orders=[_row_to_order_metric(row) for row in order_rows],
        payroll=[_row_to_payroll_metric(row) for row in payroll_rows],
        expenses=[_row_to_expense_metric(row) for row in rows if not _row_to_cashbook(row).is_deleted],
        group_by=group_by,
    )
```

- [ ] **Step 5: Run query tests**

Run:

```bash
python3.11 -m unittest tests.test_analytics_queries
```

Expected: all query tests pass.

- [ ] **Step 6: Commit**

```bash
git add analytics_app/queries.py tests/test_analytics_queries.py
git commit -m "feat: fetch management analytics data"
```

---

### Task 5: Chart Payloads

**Files:**
- Modify: `analytics_app/management.py`
- Test: `tests/test_analytics_management.py`

- [ ] **Step 1: Add failing chart tests**

Append to `tests/test_analytics_management.py`:

```python
    def test_charts_include_waterfall_and_expense_groups(self):
        orders = [
            OrderMetricRow(id=1, created_at=datetime(2026, 6, 1, tzinfo=ZoneInfo("UTC")), master_id=10, master_name="Анна", amount_total=Decimal("5000"), amount_cash=Decimal("4000"), bonus_spent=Decimal("1000"), bonus_earned=Decimal("0")),
        ]
        payroll = [
            PayrollMetricRow(order_id=1, master_id=10, master_name="Анна", base_pay=Decimal("1000"), fuel_pay=Decimal("150"), upsell_pay=Decimal("0"), total_pay=Decimal("1150")),
        ]
        expenses = [
            ExpenseRow(id=1, happened_at=datetime(2026, 6, 1, tzinfo=ZoneInfo("UTC")), amount=Decimal("700"), method="прочее", comment="Химия"),
        ]

        dashboard = build_management_dashboard(orders=orders, payroll=payroll, expenses=expenses, group_by="day")

        self.assertEqual(dashboard.charts["waterfall"]["labels"][0], "Чеки")
        self.assertEqual(dashboard.charts["expense_groups"]["labels"], ["Материалы/химия"])
        self.assertIn("time_series", dashboard.charts)
```

- [ ] **Step 2: Run management tests and verify failure**

Run:

```bash
python3.11 -m unittest tests.test_analytics_management
```

Expected: failure because `charts` is empty.

- [ ] **Step 3: Implement chart payloads**

Add helper functions in `analytics_app/management.py`:

```python
def _chart_values(items: dict[str, Decimal]) -> dict[str, list[object]]:
    return {"labels": list(items.keys()), "values": [float(value) for value in items.values()]}


def _build_charts(dashboard: ManagementDashboard) -> dict[str, object]:
    return {
        "waterfall": {
            "labels": ["Чеки", "Бонусы", "Живые деньги", "Зарплаты", "Расходы", "Опер. прибыль"],
            "values": [float(dashboard.waterfall[key]) for key in ["gross_checks", "bonuses_spent", "live_money", "salaries", "other_expenses", "operating_profit"]],
        },
        "expense_groups": {
            "labels": [group.name for group in dashboard.expense_groups],
            "values": [float(group.amount) for group in dashboard.expense_groups],
        },
        "salary_by_master": {
            "labels": [row.master_name for row in dashboard.master_salaries],
            "values": [float(row.salary) for row in dashboard.master_salaries],
        },
        "time_series": {"labels": [], "datasets": []},
    }
```

In `build_management_dashboard()`, build `result = ManagementDashboard(...)`, then return:

```python
    return ManagementDashboard(
        ...
        charts=_build_charts(result),
    )
```

Use a local mutable `charts` variable or `dataclasses.replace()` to avoid referencing `result` before assignment.

- [ ] **Step 4: Run management tests**

Run:

```bash
python3.11 -m unittest tests.test_analytics_management
```

Expected: all management tests pass.

- [ ] **Step 5: Commit**

```bash
git add analytics_app/management.py tests/test_analytics_management.py
git commit -m "feat: build analytics chart payloads"
```

---

### Task 6: Management Dashboard Template

**Files:**
- Modify: `analytics_app/templates.py`
- Modify: `analytics_app/static/app.js`
- Modify: `analytics_app/static/app.css`
- Test: `tests/test_analytics_templates.py`

- [ ] **Step 1: Add failing template test for v2 sections**

Append to `tests/test_analytics_templates.py`:

```python
from analytics_app.management import ManagementDashboard


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
            charts={"waterfall": {"labels": ["Чеки"], "values": [8000]}, "expense_groups": {"labels": [], "values": []}, "salary_by_master": {"labels": [], "values": []}, "time_series": {"labels": [], "datasets": []}},
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
```

Update imports at the top:

```python
from analytics_app.templates import render_dashboard, render_login, render_management_dashboard
```

- [ ] **Step 2: Run template tests and verify failure**

Run:

```bash
python3.11 -m unittest tests.test_analytics_templates
```

Expected: import failure because `render_management_dashboard` does not exist.

- [ ] **Step 3: Implement `render_management_dashboard()`**

In `analytics_app/templates.py`, add:

```python
def percent(value: Decimal) -> str:
    return f"{(value * Decimal('100')):.0f}%"
```

Then add a new renderer that keeps the existing sidebar but makes the first viewport analytics-first:

```python
def render_management_dashboard(
    *,
    title: str,
    active_section: str,
    period_label: str,
    date_from: str,
    date_to: str,
    balance: Decimal,
    dashboard: Any,
    ledger: list[Any],
    extra_note: str,
) -> str:
    active_main = "active" if active_section == "main" else ""
    active_cleaning = "active" if active_section == "cleaning" else ""
    chart_data = dashboard.charts
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
      <form class="filters date-filters" method="get">
        <input type="date" name="date_from" value="{html.escape(date_from)}">
        <input type="date" name="date_to" value="{html.escape(date_to)}">
        <button type="submit">Показать</button>
        <button name="period" value="today">Сегодня</button>
        <button name="period" value="week">Неделя</button>
        <button name="period" value="month">Месяц</button>
        <button name="period" value="year">Год</button>
      </form>
    </header>
    <section class="dashboard-section">
      <h2>Управленческий обзор</h2>
      <div class="metrics management-metrics">
        <div><span>Чеки</span><strong>{money(dashboard.gross_checks)}</strong></div>
        <div><span>Живые деньги</span><strong>{money(dashboard.live_money)}</strong></div>
        <div><span>Зарплата</span><strong>{money(dashboard.salary_total)}</strong><small>Зарплата от чеков: {percent(dashboard.salary_percent)}</small></div>
        <div><span>Бонусная потеря</span><strong>{money(dashboard.bonuses_spent)}</strong><small>{percent(dashboard.bonus_loss_percent)} от чеков</small></div>
        <div><span>Прочие расходы</span><strong>{money(dashboard.other_expenses)}</strong></div>
        <div><span>Опер. прибыль</span><strong>{money(dashboard.operating_profit)}</strong></div>
        <div><span>Остаток кассы</span><strong>{money(balance)}</strong></div>
      </div>
    </section>
    <section class="charts grid-2">
      <div><h2>Водопад прибыли</h2><canvas id="waterfallChart"></canvas></div>
      <div><h2>Группы расходов</h2><canvas id="expenseGroupsChart"></canvas></div>
      <div><h2>Зарплата по мастерам</h2><canvas id="salaryByMasterChart"></canvas></div>
      <div><h2>Динамика</h2><canvas id="timeSeriesChart"></canvas></div>
    </section>
    <section class="dashboard-section">
      <h2>Зарплаты</h2>
      <p>База: {money(dashboard.salary_base)}. Бензин: {money(dashboard.salary_fuel)}. Допродажи: {money(dashboard.salary_upsell)}.</p>
    </section>
    <section class="dashboard-section">
      <h2>Бонусы</h2>
      <p>Списано: {money(dashboard.bonuses_spent)}. Начислено: {money(dashboard.bonuses_earned)}. Потеря: {percent(dashboard.bonus_loss_percent)} от чеков.</p>
    </section>
    <section class="dashboard-section">
      <h2>Крупные расходы</h2>
      {_top_expense_table(dashboard.top_expenses)}
    </section>
    <section class="dashboard-section">
      <h2>Детализация операций</h2>
      <input class="table-search" id="ledgerSearch" placeholder="Поиск по комментарию или методу">
      <table id="ledgerTable">
        <thead><tr><th>Дата</th><th>Тип</th><th>Метод</th><th>Комментарий</th><th>Сумма</th></tr></thead>
        <tbody>{_ledger_rows(ledger)}</tbody>
      </table>
    </section>
  </main>
  <script id="management-chart-data" type="application/json">{html.escape(json.dumps(chart_data, ensure_ascii=False))}</script>
  <script src="/static/app.js"></script>
</body>
</html>"""
```

Add `_top_expense_table()` similarly to `_ledger_rows()`.

- [ ] **Step 4: Update JS chart rendering**

In `analytics_app/static/app.js`, add support for `management-chart-data` before the old `chart-data` block:

```javascript
  const managementRaw = document.getElementById("management-chart-data");
  if (managementRaw && window.Chart) {
    const data = JSON.parse(managementRaw.textContent);
    const makeBar = (id, label, payload) => {
      const node = document.getElementById(id);
      if (!node) return;
      new Chart(node, {
        type: "bar",
        data: { labels: payload.labels || [], datasets: [{ label, data: payload.values || [], backgroundColor: "#2563eb" }] },
        options: { responsive: true, plugins: { legend: { display: false } } }
      });
    };
    makeBar("waterfallChart", "₽", data.waterfall || {});
    makeBar("expenseGroupsChart", "₽", data.expense_groups || {});
    makeBar("salaryByMasterChart", "₽", data.salary_by_master || {});
    makeBar("timeSeriesChart", "₽", data.time_series || {});
  }
```

- [ ] **Step 5: Update CSS for dense dashboard**

In `analytics_app/static/app.css`, add:

```css
.date-filters input { border: 1px solid #ccd4e0; border-radius: 6px; padding: 8px 10px; }
.dashboard-section { margin-bottom: 12px; }
.management-metrics { grid-template-columns: repeat(4, minmax(150px, 1fr)); }
.management-metrics small { display: block; margin-top: 6px; color: #667085; }
.grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 12px; }
@media (max-width: 880px) {
  .management-metrics, .grid-2 { grid-template-columns: 1fr; }
}
```

- [ ] **Step 6: Run template tests**

Run:

```bash
python3.11 -m unittest tests.test_analytics_templates
```

Expected: all template tests pass.

- [ ] **Step 7: Commit**

```bash
git add analytics_app/templates.py analytics_app/static/app.js analytics_app/static/app.css tests/test_analytics_templates.py
git commit -m "feat: render analytics management dashboard"
```

---

### Task 7: Server Wiring

**Files:**
- Modify: `analytics_app/server.py`
- Modify: `analytics_app/queries.py`
- Test: `tests/test_analytics_server.py`

- [ ] **Step 1: Update fake service to include management data**

In `tests/test_analytics_server.py`, import `ManagementDashboard`:

```python
from analytics_app.management import ManagementDashboard
```

Add helper:

```python
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
        charts={"waterfall": {"labels": ["Чеки"], "values": [1000]}, "expense_groups": {"labels": [], "values": []}, "salary_by_master": {"labels": [], "values": []}, "time_series": {"labels": [], "datasets": []}},
    )
```

Return `"management": fake_management_dashboard()` from fake `main()` and `cleaning()` data.

- [ ] **Step 2: Add server assertion for v2 UI**

Update `test_login_sets_session_and_root_renders()`:

```python
        self.assertIn("Управленческий обзор", text)
        self.assertIn("Бонусная потеря", text)
```

- [ ] **Step 3: Run server tests and verify failure**

Run:

```bash
python3.11 -m unittest tests.test_analytics_server
```

Expected: failure because server still calls old `render_dashboard()`.

- [ ] **Step 4: Wire server to v2 renderer and pass group_by**

In `analytics_app/server.py`, import:

```python
from .templates import render_dashboard, render_login, render_management_dashboard
```

In `QueryService.main()` and `QueryService.cleaning()`, pass `group_by`:

```python
    async def main(self, conn, *, start_utc, end_utc, group_by):
        return await build_main_cash_dashboard(conn, start_utc=start_utc, end_utc=end_utc, group_by=group_by)
```

Update `main_cash()`:

```python
        data = await query_service.main(conn, start_utc=rng.start_utc, end_utc=rng.end_utc, group_by=rng.group_by)
    rendered = render_management_dashboard(
        title="Основная касса",
        active_section="main",
        period_label=_period_label(rng),
        date_from=f"{rng.start_date:%Y-%m-%d}",
        date_to=f"{rng.end_date:%Y-%m-%d}",
        balance=data["balance"],
        dashboard=data["management"],
        ledger=data["ledger"],
        extra_note="",
    )
```

Make the same change for `cleaning()`.

Update `build_main_cash_dashboard()` and `build_cleaning_dashboard()` signatures to accept `group_by: str = "day"`.

- [ ] **Step 5: Run server tests**

Run:

```bash
python3.11 -m unittest tests.test_analytics_server
```

Expected: all server tests pass.

- [ ] **Step 6: Commit**

```bash
git add analytics_app/server.py analytics_app/queries.py tests/test_analytics_server.py
git commit -m "feat: wire management analytics routes"
```

---

### Task 8: Full Verification And Production Smoke

**Files:**
- Modify: `docs/analytics_deploy.md` only if a verification command or deploy step changes during implementation.

- [ ] **Step 1: Run the full local analytics test suite**

Run:

```bash
python3.11 -m unittest discover -s tests -p 'test_analytics*.py'
```

Expected: all analytics tests pass.

- [ ] **Step 2: Run full project unittest suite**

Run:

```bash
python3.11 -m unittest discover -s tests
```

Expected: all tests pass.

- [ ] **Step 3: Compile analytics app**

Run:

```bash
python3.11 -m py_compile analytics_app/*.py
```

Expected: exit 0.

- [ ] **Step 4: Start local fake-data smoke server**

Create `/tmp/analytics_v2_smoke.py` with this content and run it from the repo root:

```python
from __future__ import annotations

from decimal import Decimal

from aiohttp import web

from analytics_app.auth import make_password_hash
from analytics_app.config import AnalyticsConfig
from analytics_app.management import ManagementDashboard
from analytics_app.server import create_app


class FakePool:
    async def close(self):
        return None


class FakeAcquire:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, exc_type, exc, tb):
        return None


def dashboard():
    return ManagementDashboard(
        gross_checks=Decimal("8000"),
        live_money=Decimal("7000"),
        bonuses_spent=Decimal("1000"),
        bonuses_earned=Decimal("700"),
        bonus_loss_percent=Decimal("0.13"),
        salary_total=Decimal("2800"),
        salary_percent=Decimal("0.35"),
        salary_base=Decimal("2000"),
        salary_fuel=Decimal("300"),
        salary_upsell=Decimal("500"),
        other_expenses=Decimal("1000"),
        operating_profit=Decimal("3200"),
        charts={
            "waterfall": {"labels": ["Чеки", "Бонусы", "Живые деньги", "Зарплаты", "Расходы", "Опер. прибыль"], "values": [8000, -1000, 7000, -2800, -1000, 3200]},
            "expense_groups": {"labels": ["Материалы/химия"], "values": [1000]},
            "salary_by_master": {"labels": ["Анна"], "values": [2800]},
            "time_series": {"labels": [], "datasets": []},
        },
    )


class FakeQueryService:
    pool = FakePool()

    def acquire(self):
        return FakeAcquire()

    async def main(self, conn, *, start_utc, end_utc, group_by):
        return {"management": dashboard(), "balance": Decimal("5000"), "ledger": []}

    async def cleaning(self, conn, *, start_utc, end_utc, group_by):
        return {"management": dashboard(), "balance": Decimal("1500"), "gift_total": Decimal("0"), "ledger": []}


cfg = AnalyticsConfig(
    db_dsn="postgres://unused",
    session_secret="smoke-session-secret",
    users={"owner": make_password_hash("password", salt="fixedsalt", iterations=1000)},
)
web.run_app(create_app(cfg, query_service=FakeQueryService()), host="127.0.0.1", port=8091)
```

Run:

```bash
python3.11 /tmp/analytics_v2_smoke.py
```

Expected checks:

```bash
curl -I -fsS http://127.0.0.1:8091/login
curl -fsS -c /tmp/analytics-v2.cookie -d 'login=owner' -d 'password=password' http://127.0.0.1:8091/login
curl -fsS -b /tmp/analytics-v2.cookie 'http://127.0.0.1:8091/?date_from=2026-06-01&date_to=2026-06-30'
```

The authenticated HTML must contain:

```text
Управленческий обзор
Зарплата от чеков
Бонусная потеря
Крупные расходы
management-chart-data
```

- [ ] **Step 5: Commit any smoke/doc fixes**

If deployment docs need adjustment:

```bash
git add docs/analytics_deploy.md
git commit -m "docs: update analytics v2 deploy notes"
```

- [ ] **Step 6: Push main and deploy**

Run:

```bash
git push origin main
ssh admin@91.200.150.68
cd /opt/telegram-analytics
git fetch origin
git reset --hard origin/main
sudo systemctl restart telegram-analytics.service
sudo systemctl status telegram-analytics.service --no-pager -l
sudo nginx -t
```

Expected:

- Service is active.
- `nginx -t` reports syntax is ok and test is successful.

- [ ] **Step 7: Production smoke**

Run:

```bash
curl -I -fsS https://analytics.dastydev.ru/login
curl -fsS -c /tmp/analytics-v2-prod.cookie -d 'login=owner' -d "password=$ANALYTICS_OWNER_PASSWORD" https://analytics.dastydev.ru/login
curl -fsS -b /tmp/analytics-v2-prod.cookie 'https://analytics.dastydev.ru/?date_from=2026-01-01&date_to=2026-12-31'
curl -fsS -b /tmp/analytics-v2-prod.cookie 'https://analytics.dastydev.ru/cleaning?date_from=2026-01-01&date_to=2026-12-31'
```

Expected authenticated HTML contains:

```text
Управленческий обзор
Зарплата от чеков
Бонусная потеря
Крупные расходы
management-chart-data
```

- [ ] **Step 8: Update external project context**

Update:

- `/Users/evgenijpastusenko/Projects/agent1/project_ai_context/tgbot-v1/AGENT_STATE.md`
- `/Users/evgenijpastusenko/Projects/agent1/project_ai_context/tgbot-v1/SESSION_LOG.md`

Record commit SHA, deployed service status, production smoke result, and any remaining hardening item such as read-only DB credentials.

---

## Self-Review

- Spec coverage: arbitrary period controls are Task 1 and Task 6; salary metrics are Task 3, Task 4, Task 6; bonus loss percent is Task 3 and Task 6; expense grouping is Task 2 and Task 6; largest expenses are Task 3 and Task 6; charts are Task 5 and Task 6; cleaning route is Task 7.
- No database schema change is planned, matching the spec.
- Raw ledger remains below analytics sections in Task 6.
- Production deploy and smoke are covered in Task 8.
