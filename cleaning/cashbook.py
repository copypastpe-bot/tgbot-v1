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


async def get_cleaning_cash_report(
    conn: asyncpg.Connection, start: datetime, end: datetime
) -> dict:
    """Структурированный отчёт по кассе клининга за период.

    Сертификаты в cashbook не пишутся — берём их отдельно из
    cleaning_order_payments по тем же датам (по happened_at заказа).
    """
    rows = await conn.fetch(
        """
        SELECT kind, method, COALESCE(SUM(amount), 0)::numeric(12,2) AS total
        FROM cleaning_cashbook
        WHERE happened_at >= $1
          AND happened_at <  $2
          AND deleted_at IS NULL
        GROUP BY kind, method
        ORDER BY kind, method
        """,
        start,
        end,
    )
    by_kind: dict[str, dict[str, Decimal]] = {
        CASHBOOK_KIND_INCOME: {},
        CASHBOOK_KIND_EXPENSE: {},
        CASHBOOK_KIND_DIVIDEND: {},
        CASHBOOK_KIND_WITHDRAWAL: {},
        CASHBOOK_KIND_DEPOSIT: {},
    }
    for r in rows:
        by_kind.setdefault(r["kind"], {})[r["method"]] = Decimal(r["total"])

    gift_total = await conn.fetchval(
        """
        SELECT COALESCE(SUM(p.amount), 0)::numeric(12,2)
        FROM cleaning_order_payments p
        JOIN cleaning_orders o ON o.id = p.order_id
        WHERE p.method = 'Подарочный сертификат'
          AND o.happened_at >= $1
          AND o.happened_at <  $2
          AND o.deleted_at IS NULL
        """,
        start,
        end,
    )
    income_sum = sum(by_kind[CASHBOOK_KIND_INCOME].values(), ZERO)
    expense_sum = sum(by_kind[CASHBOOK_KIND_EXPENSE].values(), ZERO)
    dividend_sum = sum(by_kind[CASHBOOK_KIND_DIVIDEND].values(), ZERO)
    withdrawal_sum = sum(by_kind[CASHBOOK_KIND_WITHDRAWAL].values(), ZERO)
    deposit_sum = sum(by_kind[CASHBOOK_KIND_DEPOSIT].values(), ZERO)
    return {
        "income_by_method": by_kind[CASHBOOK_KIND_INCOME],
        "expense_by_category": by_kind[CASHBOOK_KIND_EXPENSE],
        "income_total": income_sum,
        "expense_total": expense_sum,
        "dividend_total": dividend_sum,
        "withdrawal_total": withdrawal_sum,
        "deposit_total": deposit_sum,
        "gift_total": Decimal(gift_total) if gift_total is not None else ZERO,
        "profit": income_sum - expense_sum,
    }


async def get_cleaning_orders_list(
    conn: asyncpg.Connection, start: datetime, end: datetime
) -> list[dict]:
    rows = await conn.fetch(
        """
        SELECT
            o.id, o.happened_at, o.address, o.total_amount,
            o.bonuses_used, o.bonuses_earned,
            c.full_name AS client_name, c.phone AS client_phone,
            (SELECT string_agg(p.method || ' ' || p.amount::text, ', ')
             FROM cleaning_order_payments p
             WHERE p.order_id = o.id) AS pay_summary
        FROM cleaning_orders o
        JOIN clients c ON c.id = o.client_id
        WHERE o.happened_at >= $1
          AND o.happened_at <  $2
          AND o.deleted_at IS NULL
        ORDER BY o.happened_at
        """,
        start,
        end,
    )
    return [
        {
            "id": r["id"],
            "happened_at": r["happened_at"],
            "address": r["address"],
            "total_amount": Decimal(r["total_amount"]),
            "bonuses_used": int(r["bonuses_used"] or 0),
            "bonuses_earned": int(r["bonuses_earned"] or 0),
            "client_name": r["client_name"],
            "client_phone": r["client_phone"],
            "pay_summary": r["pay_summary"],
        }
        for r in rows
    ]


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
