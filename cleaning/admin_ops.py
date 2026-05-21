"""Административные кассовые операции и отмена заказа уборки.

Никаких aiogram-зависимостей — только данные и запись в БД.
Используется из handlers.py.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import asyncpg

from .constants import (
    CASHBOOK_KIND_DEPOSIT,
    CASHBOOK_KIND_EXPENSE,
    CASHBOOK_KIND_WITHDRAWAL,
    CLEANING_DIVIDEND_METHOD,
)


async def add_cash_income(
    conn: asyncpg.Connection, *, method: str, amount: Decimal, comment: str | None
) -> int:
    """Ручной приход (deposit) — НЕ income от заказа, отдельная категория."""
    return await conn.fetchval(
        """
        INSERT INTO cleaning_cashbook (kind, method, amount, comment)
        VALUES ($1, $2, $3, $4)
        RETURNING id
        """,
        CASHBOOK_KIND_DEPOSIT,
        method,
        amount,
        comment,
    )


async def add_cash_expense(
    conn: asyncpg.Connection, *, category: str, amount: Decimal, comment: str | None
) -> int:
    return await conn.fetchval(
        """
        INSERT INTO cleaning_cashbook (kind, method, amount, comment)
        VALUES ($1, $2, $3, $4)
        RETURNING id
        """,
        CASHBOOK_KIND_EXPENSE,
        category,
        amount,
        comment,
    )


async def add_cash_withdrawal(
    conn: asyncpg.Connection, *, amount: Decimal, comment: str | None
) -> int:
    return await conn.fetchval(
        """
        INSERT INTO cleaning_cashbook (kind, method, amount, comment)
        VALUES ($1, $2, $3, $4)
        RETURNING id
        """,
        CASHBOOK_KIND_WITHDRAWAL,
        CLEANING_DIVIDEND_METHOD,  # тот же «Касса клининга» как single source
        amount,
        comment,
    )


async def cancel_order(
    conn: asyncpg.Connection, *, order_id: int
) -> dict | None:
    """Откатить заказ уборки целиком.

    Soft-delete заказа, всех строк cleaning_cashbook по order_id, плюс
    обратный пересчёт бонусов клиента в общей таблице. Возвращает dict
    с фактами для построения алерта, или None если заказа нет / уже
    отменён.
    """
    order = await conn.fetchrow(
        """
        SELECT id, client_id, foreman_id, total_amount,
               bonuses_used, bonuses_earned, address, deleted_at
        FROM cleaning_orders
        WHERE id = $1
        """,
        order_id,
    )
    if order is None or order["deleted_at"] is not None:
        return None

    now_utc = datetime.now(timezone.utc)

    # 1. Помечаем заказ удалённым
    await conn.execute(
        "UPDATE cleaning_orders SET deleted_at = $1 WHERE id = $2",
        now_utc,
        order_id,
    )

    # 2. Soft-delete всех cashbook-строк по этому заказу
    cb_affected = await conn.fetchval(
        """
        WITH upd AS (
            UPDATE cleaning_cashbook
            SET deleted_at = $1
            WHERE order_id = $2 AND deleted_at IS NULL
            RETURNING 1
        )
        SELECT count(*) FROM upd
        """,
        now_utc,
        order_id,
    )

    # 3. Откат бонусов (зеркальные транзакции, чтобы история была полной)
    used = int(order["bonuses_used"] or 0)
    earned = int(order["bonuses_earned"] or 0)
    if used > 0:
        await conn.execute(
            """
            INSERT INTO bonus_transactions
                (client_id, delta, reason, created_at, happened_at, meta)
            VALUES ($1, $2, 'refund', $3, $3,
                    jsonb_build_object('source','cleaning_cancel','order_id',$4::int))
            """,
            order["client_id"],
            used,                    # вернули клиенту то, что списали
            now_utc,
            order_id,
        )
    if earned > 0:
        await conn.execute(
            """
            INSERT INTO bonus_transactions
                (client_id, delta, reason, created_at, happened_at, meta)
            VALUES ($1, $2, 'reversal', $3, $3,
                    jsonb_build_object('source','cleaning_cancel','order_id',$4::int))
            """,
            order["client_id"],
            -earned,                 # сняли то, что начислили
            now_utc,
            order_id,
        )
    if used or earned:
        await conn.execute(
            "UPDATE clients SET bonus_balance = bonus_balance + $1 - $2 WHERE id = $3",
            used,
            earned,
            order["client_id"],
        )

    return {
        "order_id": order_id,
        "client_id": order["client_id"],
        "address": order["address"],
        "total_amount": Decimal(order["total_amount"]),
        "bonuses_used": used,
        "bonuses_earned": earned,
        "cashbook_rows_deleted": int(cb_affected or 0),
    }
