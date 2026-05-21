"""Чистые хелперы вокруг проведения заказа на уборку.

Никаких обращений к БД, никаких aiogram-зависимостей — только данные
и валидация. Используется FSM-обработчиками и тестами.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from .constants import CLEANING_GIFT_CERT_LABEL, ZERO


@dataclass(frozen=True)
class PaymentPart:
    method: str
    amount: Decimal


def parse_amount(text: str) -> Decimal | None:
    """Принимает '1 234,50', '1234.5', '0'. Возвращает Decimal или None.

    Отрицательные числа не принимаем — суммы всегда неотрицательные.
    """
    if not text:
        return None
    cleaned = text.replace(" ", "").replace(" ", "").replace(",", ".")
    try:
        value = Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None
    if value < 0:
        return None
    return value


def payments_balance_diff(
    parts: list[PaymentPart], total: Decimal, bonuses_used: Decimal
) -> Decimal:
    """Разница (сумма оплат) − (total − bonuses_used).

    0 значит «сошлось». Знак показывает направление расхождения.
    """
    paid = sum((p.amount for p in parts), ZERO)
    expected = total - bonuses_used
    return paid - expected


def cashbook_rows_from_payments(
    parts: list[PaymentPart],
) -> list[tuple[str, Decimal]]:
    """Какие строки должны попасть в cleaning_cashbook как income.

    Сертификаты намеренно вырезаны (контракт химчистки: в кассу 0₽).
    Нулевые суммы тоже отбрасываем — мусорные записи не нужны.
    """
    rows: list[tuple[str, Decimal]] = []
    for part in parts:
        if part.method == CLEANING_GIFT_CERT_LABEL:
            continue
        if part.amount <= 0:
            continue
        rows.append((part.method, part.amount))
    return rows
