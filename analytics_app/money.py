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
