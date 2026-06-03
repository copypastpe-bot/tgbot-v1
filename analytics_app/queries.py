from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from .money import CashbookRow, summarize_cashbook_rows, summarize_cleaning_rows


def _decimal(value: Any) -> Decimal:
    return Decimal(value or 0)


def _get(row: Any, key: str, default: Any = None) -> Any:
    try:
        value = row[key]
    except Exception:
        return default
    return default if value is None else value


def _row_to_cashbook(row: Any) -> CashbookRow:
    return CashbookRow(
        kind=_get(row, "kind", ""),
        method=_get(row, "method", ""),
        amount=_decimal(_get(row, "amount", 0)),
        comment=_get(row, "comment", ""),
        is_deleted=bool(_get(row, "is_deleted", False)),
    )


async def build_main_cash_dashboard(conn, *, start_utc: datetime, end_utc: datetime) -> dict[str, Any]:
    rows = await conn.fetch(
        """
        SELECT id, happened_at, kind, method, amount,
               COALESCE(comment, '') AS comment,
               order_id, master_id,
               COALESCE(is_deleted, false) AS is_deleted
        FROM cashbook_entries
        WHERE happened_at >= $1
          AND happened_at <  $2
        ORDER BY happened_at DESC, id DESC
        LIMIT 500
        """,
        start_utc,
        end_utc,
    )
    balance = await conn.fetchval(
        """
        SELECT
          COALESCE(SUM(CASE WHEN kind='income' THEN amount ELSE 0 END),0)
          -
          COALESCE(SUM(CASE WHEN kind='expense'
                             AND NOT (comment ILIKE '[WDR]%'
                                      OR comment ILIKE 'изъят%'
                                      OR method = 'DIV')
                            THEN amount ELSE 0 END),0)
        FROM cashbook_entries
        WHERE COALESCE(is_deleted,false)=false
        """
    )
    summary = summarize_cashbook_rows([_row_to_cashbook(row) for row in rows])
    return {
        "summary": summary,
        "balance": _decimal(balance),
        "ledger": rows,
    }


async def build_cleaning_dashboard(conn, *, start_utc: datetime, end_utc: datetime) -> dict[str, Any]:
    rows = await conn.fetch(
        """
        SELECT id, happened_at, kind, method, amount,
               COALESCE(comment, '') AS comment,
               order_id,
               (deleted_at IS NOT NULL) AS is_deleted
        FROM cleaning_cashbook
        WHERE happened_at >= $1
          AND happened_at <  $2
        ORDER BY happened_at DESC, id DESC
        LIMIT 500
        """,
        start_utc,
        end_utc,
    )
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
        start_utc,
        end_utc,
    )
    balance = await conn.fetchval(
        """
        SELECT COALESCE(SUM(
          CASE
            WHEN kind IN ('income', 'deposit') THEN amount
            WHEN kind IN ('expense', 'dividend', 'withdrawal') THEN -amount
            ELSE 0
          END
        ), 0)::numeric(12,2)
        FROM cleaning_cashbook
        WHERE deleted_at IS NULL
        """
    )
    summary = summarize_cleaning_rows([_row_to_cashbook(row) for row in rows])
    return {
        "summary": summary,
        "balance": _decimal(balance),
        "gift_total": _decimal(gift_total),
        "ledger": rows,
    }
