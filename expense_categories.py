from __future__ import annotations


UNCATEGORIZED_EXPENSE_CATEGORY = "Без категории"

EXPENSE_CATEGORIES: tuple[str, ...] = (
    "Зарплата/подрядчики",
    "Партнеры",
    "Реклама/маркетинг",
    "Материалы/химия",
    "Ремонт оборудования",
    "Премии/подарки сотрудникам",
    "Налоги",
    "Офис/аренда",
    "Связь/IT/сервисы",
    "Банк/комиссии",
    "Транспорт/логистика",
    "Питание/быт",
    "Возвраты/ущерб/коррекции",
    "Прочее",
)

SPECIAL_EXPENSE_CATEGORIES: tuple[str, ...] = (
    "Дивиденды/изъятия",
)

EXPENSE_CATEGORY_SET = set(EXPENSE_CATEGORIES)
SPECIAL_EXPENSE_CATEGORY_SET = set(SPECIAL_EXPENSE_CATEGORIES)

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


def classify_expense(*, method: str = "", comment: str = "") -> str:
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
    return UNCATEGORIZED_EXPENSE_CATEGORY


def suggest_expense_category(*, method: str = "прочее", comment: str = "") -> str:
    category = classify_expense(method=method, comment=comment)
    if category in EXPENSE_CATEGORY_SET:
        return category
    return "Прочее"


def normalize_expense_category(category: str | None) -> str | None:
    cleaned = (category or "").strip()
    if cleaned in EXPENSE_CATEGORY_SET:
        return cleaned
    return None
