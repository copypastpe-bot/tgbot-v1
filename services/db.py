from __future__ import annotations
from decimal import Decimal
import asyncpg


async def get_master_wallet(conn: asyncpg.Connection, master_id: int) -> tuple[Decimal, Decimal]:
    """
    Возвращает (cash_on_hand, withdrawn_total) по тем же правилам, что и в отчёте «Мастер/Заказы/Оплаты».
    cash_on_hand = «Наличных у мастера»
    withdrawn_total = «Изъято у мастера»
    """
    cash_on_orders = await conn.fetchval(
        """
        SELECT COALESCE(SUM(amount),0)
        FROM cashbook_entries
        WHERE kind='income' AND method='Наличные'
          AND master_id=$1 AND order_id IS NOT NULL
        """,
        master_id,
    )
    withdrawn = await conn.fetchval(
        """
        SELECT COALESCE(SUM(amount),0)
        FROM cashbook_entries
        WHERE kind='withdrawal' AND method='cash'
          AND master_id=$1 AND order_id IS NULL
        """,
        master_id,
    )
    from decimal import Decimal as D
    return D(cash_on_orders or 0), D(withdrawn or 0)


async def _record_income(conn: asyncpg.Connection, method: str, amount: Decimal, comment: str):
    tx = await conn.fetchrow(
        """
        INSERT INTO cashbook_entries(kind, method, amount, comment, order_id, master_id, happened_at)
        VALUES ('income', $1, $2, $3, NULL, NULL, now())
        RETURNING id, happened_at
        """,
        method, amount, comment or "Приход",
    )
    return tx


async def _record_expense(conn: asyncpg.Connection, amount: Decimal, comment: str, method: str = "прочее"):
    tx = await conn.fetchrow(
        """
        INSERT INTO cashbook_entries(kind, method, amount, comment, order_id, master_id, happened_at)
        VALUES ('expense', $1, $2, $3, NULL, NULL, now())
        RETURNING id, happened_at
        """,
        method, amount, comment or "Расход",
    )
    return tx
