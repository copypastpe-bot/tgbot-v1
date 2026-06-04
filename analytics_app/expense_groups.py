from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP

from expense_categories import classify_expense, normalize_expense_category

from .management import ExpenseRow


@dataclass(frozen=True)
class ExpenseGroup:
    name: str
    amount: Decimal
    share: Decimal
    count: int
    examples: list[str]


def _ratio(value: Decimal, total: Decimal) -> Decimal:
    if total == 0:
        return Decimal("0")
    return (value / total).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def group_expenses(rows: list[ExpenseRow]) -> list[ExpenseGroup]:
    total = sum((row.amount for row in rows), Decimal("0"))
    buckets: dict[str, list[ExpenseRow]] = {}
    for row in rows:
        label = normalize_expense_category(row.category) or classify_expense(method=row.method, comment=row.comment)
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
