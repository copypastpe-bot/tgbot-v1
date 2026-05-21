"""Чтение клининг-кассы: баланс и P&L. Записи делаются из FSM проведения."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import asyncpg

from .constants import (
    CASHBOOK_KIND_DEPOSIT,
    CASHBOOK_KIND_DIVIDEND,
    CASHBOOK_KIND_EXPENSE,
    CASHBOOK_KIND_INCOME,
    CASHBOOK_KIND_WITHDRAWAL,
    CLEANING_DIVIDEND_METHOD,
    ZERO,
)


async def record_income(
    conn: asyncpg.Connection,
    *,
    method: str,
    amount: Decimal,
    order_id: int,
    comment: str | None = None,
) -> None:
    await conn.execute(
        """
        INSERT INTO cleaning_cashbook (kind, method, amount, comment, order_id)
        VALUES ($1, $2, $3, $4, $5)
        """,
        CASHBOOK_KIND_INCOME,
        method,
        amount,
        comment,
        order_id,
    )


async def record_expense(
    conn: asyncpg.Connection,
    *,
    category: str,
    amount: Decimal,
    order_id: int | None = None,
    comment: str | None = None,
) -> None:
    await conn.execute(
        """
        INSERT INTO cleaning_cashbook (kind, method, amount, comment, order_id)
        VALUES ($1, $2, $3, $4, $5)
        """,
        CASHBOOK_KIND_EXPENSE,
        category,
        amount,
        comment,
        order_id,
    )


async def record_dividend(
    conn: asyncpg.Connection, *, amount: Decimal, comment: str
) -> None:
    await conn.execute(
        """
        INSERT INTO cleaning_cashbook (kind, method, amount, comment)
        VALUES ($1, $2, $3, $4)
        """,
        CASHBOOK_KIND_DIVIDEND,
        CLEANING_DIVIDEND_METHOD,
        amount,
        comment,
    )


async def get_cleaning_balance(conn: asyncpg.Connection) -> Decimal:
    """Остаток кассы: приход + deposit − расход − dividend − withdrawal.

    Сертификаты не попадают сюда: они изначально не пишутся в cleaning_cashbook
    (см. правило записи в дизайн-доке §1).
    """
    value = await conn.fetchval(
        """
        SELECT COALESCE(SUM(
          CASE
            WHEN kind IN ($1, $2) THEN amount
            WHEN kind IN ($3, $4, $5) THEN -amount
            ELSE 0
          END
        ), 0)::numeric(12,2)
        FROM cleaning_cashbook
        WHERE deleted_at IS NULL
        """,
        CASHBOOK_KIND_INCOME,
        CASHBOOK_KIND_DEPOSIT,
        CASHBOOK_KIND_EXPENSE,
        CASHBOOK_KIND_DIVIDEND,
        CASHBOOK_KIND_WITHDRAWAL,
    )
    return Decimal(value) if value is not None else ZERO


async def get_cleaning_pnl(
    conn: asyncpg.Connection, start: datetime, end: datetime
) -> dict[str, Decimal]:
    """P&L за период: выручка, расход, прибыль.

    DIV/withdrawal/deposit исключены — это движения капитала, не операционные.
    """
    row = await conn.fetchrow(
        """
        SELECT
          COALESCE(SUM(CASE WHEN kind = $3 THEN amount ELSE 0 END), 0)::numeric(12,2) AS income,
          COALESCE(SUM(CASE WHEN kind = $4 THEN amount ELSE 0 END), 0)::numeric(12,2) AS expense
        FROM cleaning_cashbook
        WHERE happened_at >= $1
          AND happened_at <  $2
          AND deleted_at IS NULL
          AND kind IN ($3, $4)
        """,
        start,
        end,
        CASHBOOK_KIND_INCOME,
        CASHBOOK_KIND_EXPENSE,
    )
    income = Decimal(row["income"]) if row else ZERO
    expense = Decimal(row["expense"]) if row else ZERO
    return {"income": income, "expense": expense, "profit": income - expense}
