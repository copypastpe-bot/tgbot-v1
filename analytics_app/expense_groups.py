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
