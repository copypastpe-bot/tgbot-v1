"""Чистые хелперы вокруг проведения заказа на уборку.

Никаких обращений к БД, никаких aiogram-зависимостей — только данные
и валидация. Используется FSM-обработчиками и тестами.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN
import os
from typing import Mapping

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


def _env_decimal(env: Mapping[str, str], key: str, default: str) -> Decimal:
    raw = env.get(key, default)
    parsed = parse_amount(str(raw))
    return parsed if parsed is not None else Decimal(default)


def qround_ruble(value: Decimal) -> Decimal:
    return value.quantize(Decimal("1."), rounding=ROUND_DOWN)


def get_min_order_amount(env: Mapping[str, str] = os.environ) -> Decimal:
    return _env_decimal(env, "CLEANING_MIN_ORDER_AMOUNT", "5500")


def get_min_cash_amount(env: Mapping[str, str] = os.environ) -> Decimal:
    return _env_decimal(env, "MIN_CASH", "2500")


def get_bonus_rate(env: Mapping[str, str] = os.environ) -> Decimal:
    return _env_decimal(env, "BONUS_RATE_PERCENT", "5") / Decimal(100)


def get_max_bonus_spend_rate(env: Mapping[str, str] = os.environ) -> Decimal:
    return _env_decimal(env, "MAX_BONUS_SPEND_RATE_PERCENT", "50") / Decimal(100)


def calculate_bonus_max(
    total: Decimal, balance: int | Decimal, env: Mapping[str, str] = os.environ
) -> Decimal:
    balance_amount = Decimal(balance or 0)
    max_by_rate = qround_ruble(total * get_max_bonus_spend_rate(env))
    max_by_min_cash = qround_ruble(total - get_min_cash_amount(env))
    return max(ZERO, min(max_by_rate, balance_amount, max_by_min_cash))


def is_wire_payment_method(method: str | None) -> bool:
    return (method or "").strip().casefold() == "расчётный"


def calculate_bonus_earned(
    payment_method: str | None,
    cash_payment: Decimal,
    env: Mapping[str, str] = os.environ,
) -> int:
    if is_wire_payment_method(payment_method):
        return 0
    return int(qround_ruble(cash_payment * get_bonus_rate(env)))


def payments_balance_diff(
    parts: list[PaymentPart], total: Decimal, bonuses_used: Decimal
) -> Decimal:
    """Разница (сумма оплат) − (total − bonuses_used).

    0 значит «сошлось». Знак показывает направление расхождения.
    """
    paid = sum((p.amount for p in parts), ZERO)
    expected = total - bonuses_used
    return paid - expected


def validate_payment_parts(
    parts: list[PaymentPart], total: Decimal, bonuses_used: Decimal
) -> tuple[bool, str]:
    if not parts:
        return False, "Добавьте хотя бы одну часть оплаты."
    wire_parts = [p for p in parts if is_wire_payment_method(p.method)]
    if wire_parts and len(parts) > 1:
        return False, "Расчётный нельзя смешивать с другими способами оплаты."
    diff = payments_balance_diff(parts, total, bonuses_used)
    if diff != 0:
        return False, f"Оплата не сходится на {diff}."
    return True, ""


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
