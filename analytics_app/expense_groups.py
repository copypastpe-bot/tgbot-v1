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
    (
        "Зарплата/подрядчики",
        (
            "зп", "з/п", "зарплат", "дима с вх", "саша вх", "вадим вх",
            "максим вх", "костя вх", "дима/женя", "женя пятница", "дима пятница",
            "дииа", "попов за заказ",
        ),
    ),
    (
        "Партнеры",
        (
            "ковры школа", "ковер школа", "гусев", "теплоход", "маша 20%",
            "аня клюнинг", "аня клининг", "мк окская", "арина кешбэк",
        ),
    ),
    (
        "Реклама/маркетинг",
        (
            "реклам", "авито", "директ", "таргет", "лид", "seo", "сео",
            "seokey", "сеокей", "яндекс карт", "якарты", "дискавери",
            "артгорький", "сайт прайс", "доработки сайта",
        ),
    ),
    ("Ремонт оборудования", ("турбина", "сварка", "ремонт оборуд")),
    ("Премии/подарки сотрудникам", ("полозов др", "др козлов")),
    (
        "Материалы/химия",
        (
            "хим", "средств", "расходник", "перчат", "пакет", "блю и смарт",
            "пробоайт", "профом", "кислот", "кислота", "окси", "бумага",
            "щетк", "щётк",
        ),
    ),
    ("Налоги", ("налог",)),
    ("Офис/аренда", ("аренд", "офис", "склад", "коммун")),
    (
        "Связь/IT/сервисы",
        (
            "связ", "телефон", "интернет", "софт", "crm", "wahelp", "вахелп",
            "ва хелп", "амо", "пбх", "pbx", "битрикс", "сервер", "хостинг",
            "openai", "домен", "vpn", "впн", "sms.ru",
        ),
    ),
    ("Банк/комиссии", ("банк", "комисс")),
    ("Транспорт/логистика", ("бенз", "топлив", "дорог", "такси", "гсм", "парков", "доставка", "эвакуатор", "штраф")),
    ("Питание/быт", ("питание", "вода", "чай", "печенье", "обед")),
    ("Возвраты/ущерб/коррекции", ("возврат", "коррект", "порча", "ламинат")),
    ("Прочее", ("кио",)),
)


def classify_expense(*, method: str, comment: str) -> str:
    haystack = f"{method or ''} {comment or ''}".lower()
    if (method or "").upper() == "DIV":
        return "Дивиденды/изъятия"
    normalized_comment = (comment or "").strip().lower()
    if "гсм" in haystack:
        return "Транспорт/логистика"
    if normalized_comment in {"дима", "женя", "дима/женя", "дииа"}:
        return "Зарплата/подрядчики"
    if normalized_comment.startswith("дима ") and any(ch.isdigit() for ch in normalized_comment):
        return "Зарплата/подрядчики"
    for label, needles in RULES:
        if any(needle in haystack for needle in needles):
            return label
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
