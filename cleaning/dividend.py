"""Правила выплаты прибыли из кассы клининга.

Клинер держит наличные и выдаёт их получателям поровну — сейчас это трое
владельцев дела. Сумму вводит человек, поэтому две проверки обязательны:
хватает ли кассы и делится ли сумма нацело. Копейка, потерянная при делении,
развела бы кассу с тем, что написано в чате, а деньги живые.

Состав получателей — настройка, а не константа: люди меняются чаще, чем код.
"""

from __future__ import annotations

import os
import re
from decimal import Decimal

DEFAULT_RECIPIENTS: tuple[str, ...] = ("Оля", "Дима", "Женя")
DEFAULT_DIVIDEND_COMMENT = "Выплата прибыли"

PAYOUT_OK = "ok"
PAYOUT_BAD_AMOUNT = "bad_amount"
PAYOUT_NOT_ENOUGH = "not_enough"
PAYOUT_NOT_DIVISIBLE = "not_divisible"

KOPECKS = Decimal("0.01")


def parse_recipients(raw: str | None) -> list[str]:
    names = [part.strip() for part in re.split(r"[,;]", raw or "") if part.strip()]
    return names or list(DEFAULT_RECIPIENTS)


def configured_recipients() -> list[str]:
    return parse_recipients(os.getenv("CLEANING_DIVIDEND_RECIPIENTS"))


def dividend_comment() -> str:
    return (os.getenv("CLEANING_DIVIDEND_COMMENT") or DEFAULT_DIVIDEND_COMMENT).strip()


def _to_kopecks(value: Decimal) -> int:
    return int((value.quantize(KOPECKS) * 100).to_integral_value())


def split_equally(total: Decimal, count: int) -> list[Decimal] | None:
    """Делит сумму поровну. None, если нацело не делится."""
    if count <= 0:
        return None
    kopecks = _to_kopecks(total)
    if kopecks % count != 0:
        return None
    share = Decimal(kopecks // count) / 100
    return [share.quantize(KOPECKS) for _ in range(count)]


def largest_divisible_not_above(total: Decimal, count: int) -> Decimal:
    """Ближайшая сумма не больше введённой, которая делится нацело.

    Нужна для подсказки: «введите 9 999» понятнее, чем «сумма не делится».
    """
    if count <= 0:
        return Decimal("0")
    kopecks = _to_kopecks(total)
    return (Decimal(kopecks - kopecks % count) / 100).quantize(KOPECKS)


def check_payout(
    *, amount: Decimal, balance: Decimal, recipients: list[str]
) -> tuple[str, list[Decimal]]:
    """Решает, можно ли провести выплату, и как она делится.

    Нехватку денег проверяем раньше делимости: сказать «не делится» про сумму,
    которой всё равно нет в кассе, значит сбить человека с толку.
    """
    if not recipients or amount <= 0:
        return PAYOUT_BAD_AMOUNT, []
    if amount > balance:
        return PAYOUT_NOT_ENOUGH, []
    shares = split_equally(amount, len(recipients))
    if shares is None:
        return PAYOUT_NOT_DIVISIBLE, []
    return PAYOUT_OK, shares
