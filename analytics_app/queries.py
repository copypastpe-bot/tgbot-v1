from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from .management import ExpenseRow, OrderMetricRow, PayrollMetricRow, build_management_dashboard
from .money import (
    CashbookRow,
    is_dividend,
    is_withdrawal,
    summarize_cashbook_rows,
    summarize_cleaning_rows,
)


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


def _row_to_order_metric(row: Any) -> OrderMetricRow:
    return OrderMetricRow(
        id=int(_get(row, "id", 0)),
        created_at=_get(row, "created_at"),
        master_id=_get(row, "master_id"),
        master_name=str(_get(row, "master_name", "Без мастера") or "Без мастера"),
        amount_total=_decimal(_get(row, "amount_total", 0)),
        amount_cash=_decimal(_get(row, "amount_cash", 0)),
        bonus_spent=_decimal(_get(row, "bonus_spent", 0)),
        bonus_earned=_decimal(_get(row, "bonus_earned", 0)),
    )


def _row_to_payroll_metric(row: Any) -> PayrollMetricRow:
    return PayrollMetricRow(
        order_id=int(_get(row, "order_id", 0)),
        master_id=_get(row, "master_id"),
        master_name=str(_get(row, "master_name", "Без мастера") or "Без мастера"),
        base_pay=_decimal(_get(row, "base_pay", 0)),
        fuel_pay=_decimal(_get(row, "fuel_pay", 0)),
        upsell_pay=_decimal(_get(row, "upsell_pay", 0)),
        total_pay=_decimal(_get(row, "total_pay", 0)),
    )


def _row_to_expense_metric(row: Any) -> ExpenseRow:
    return ExpenseRow(
        id=int(_get(row, "id", 0)),
        happened_at=_get(row, "happened_at"),
        amount=_decimal(_get(row, "amount", 0)),
        method=str(_get(row, "method", "")),
        comment=str(_get(row, "comment", "")),
        category=str(_get(row, "category", "")),
    )


def _row_scope(row: Any) -> str:
    return str(_get(row, "row_scope", "ledger") or "ledger")


def _is_operating_expense(row: Any) -> bool:
    cashbook_row = _row_to_cashbook(row)
    if cashbook_row.is_deleted:
        return False
    if cashbook_row.kind != "expense":
        return False
    return not (is_withdrawal(cashbook_row) or is_dividend(cashbook_row))


async def build_main_cash_dashboard(
    conn, *, start_utc: datetime, end_utc: datetime, group_by: str = "day"
) -> dict[str, Any]:
    order_rows = await conn.fetch(
        """
        SELECT o.id, o.created_at, o.master_id,
               TRIM(COALESCE(s.first_name, '') || ' ' || COALESCE(s.last_name, '')) AS master_name,
               COALESCE(o.amount_total, 0) AS amount_total,
               CASE
                 WHEN COALESCE(op.payment_count, 0) > 0
                   THEN COALESCE(op.non_wire_money, 0) + COALESCE(wire_income.live_money, 0)
                 ELSE COALESCE(o.amount_cash, 0)
               END AS amount_cash,
               COALESCE(o.bonus_spent, 0) AS bonus_spent,
               COALESCE(o.bonus_earned, 0) AS bonus_earned
        FROM orders o
        LEFT JOIN staff s ON s.id = o.master_id
        LEFT JOIN (
            SELECT order_id,
                   COUNT(*) AS payment_count,
                   COALESCE(SUM(CASE WHEN method <> 'р/с' THEN amount ELSE 0 END), 0) AS non_wire_money
            FROM order_payments
            GROUP BY order_id
        ) op ON op.order_id = o.id
        LEFT JOIN (
            SELECT order_id, COALESCE(SUM(amount), 0) AS live_money
            FROM cashbook_entries
            WHERE kind = 'income'
              AND method = 'р/с'
              AND order_id IS NOT NULL
              AND COALESCE(is_deleted, false) = false
              AND COALESCE(awaiting_order, false) = false
            GROUP BY order_id
        ) wire_income ON wire_income.order_id = o.id
        WHERE o.created_at >= $1
          AND o.created_at <  $2
        ORDER BY o.created_at DESC, o.id DESC
        """,
        start_utc,
        end_utc,
    )
    payroll_rows = await conn.fetch(
        """
        SELECT pi.order_id, pi.master_id,
               TRIM(COALESCE(s.first_name, '') || ' ' || COALESCE(s.last_name, '')) AS master_name,
               COALESCE(pi.base_pay, 0) AS base_pay,
               COALESCE(pi.fuel_pay, 0) AS fuel_pay,
               COALESCE(pi.upsell_pay, 0) AS upsell_pay,
               COALESCE(pi.total_pay, 0) AS total_pay
        FROM payroll_items pi
        JOIN orders o ON o.id = pi.order_id
        LEFT JOIN staff s ON s.id = pi.master_id
        WHERE o.created_at >= $1
          AND o.created_at <  $2
        ORDER BY o.created_at DESC, pi.order_id DESC
        """,
        start_utc,
        end_utc,
    )
    cash_metrics = await conn.fetchrow(
        """
        SELECT
          COALESCE(SUM(
            CASE
              WHEN kind = 'income'
                AND NOT COALESCE(comment, '') ILIKE 'Стартовый остаток%'
              THEN amount
              ELSE 0
            END
          ), 0)::numeric(12,2) AS cash_income,
          COALESCE(SUM(
            CASE
              WHEN kind = 'expense'
               AND NOT (
                 COALESCE(comment, '') ILIKE '[WDR]%'
                 OR COALESCE(comment, '') ILIKE 'изъят%'
                 OR COALESCE(method, '') = 'DIV'
                 OR COALESCE(comment, '') ILIKE '[DIV]%'
               )
              THEN amount
              ELSE 0
            END
          ), 0)::numeric(12,2) AS cash_expense,
          COALESCE(SUM(
            CASE
              WHEN kind = 'expense'
               AND (
                 COALESCE(method, '') = 'DIV'
                 OR COALESCE(comment, '') ILIKE '[DIV]%'
               )
              THEN amount
              ELSE 0
            END
          ), 0)::numeric(12,2) AS div_paid
        FROM cashbook_entries
        WHERE happened_at >= $1
          AND happened_at <  $2
          AND COALESCE(is_deleted, false) = false
          AND kind <> 'opening_balance'
        """,
        start_utc,
        end_utc,
    )
    rows = await conn.fetch(
        """
        SELECT row_scope, id, happened_at, kind, method, amount,
               comment, category, order_id, master_id, is_deleted
        FROM (
            SELECT 0 AS row_sort, ledger_rows.*
            FROM (
                SELECT 'ledger'::text AS row_scope, id, happened_at, kind, method, amount,
                       COALESCE(comment, '') AS comment,
                       COALESCE(to_jsonb(ce)->>'category', '') AS category,
                       order_id, master_id,
                       COALESCE(is_deleted, false) AS is_deleted
                FROM cashbook_entries ce
                WHERE happened_at >= $1
                  AND happened_at <  $2
                ORDER BY happened_at DESC, id DESC
                LIMIT 500
            ) ledger_rows

            UNION ALL

            SELECT 1 AS row_sort,
                   'operating_expense'::text AS row_scope, id, happened_at, kind, method, amount,
                   COALESCE(comment, '') AS comment,
                   COALESCE(to_jsonb(ce)->>'category', '') AS category,
                   order_id, master_id,
                   COALESCE(is_deleted, false) AS is_deleted
            FROM cashbook_entries ce
            WHERE happened_at >= $1
              AND happened_at <  $2
              AND kind = 'expense'
              AND COALESCE(is_deleted, false) = false
              AND NOT (
                  COALESCE(comment, '') ILIKE '[WDR]%'
                  OR COALESCE(comment, '') ILIKE 'изъят%'
                  OR COALESCE(method, '') = 'DIV'
                  OR COALESCE(comment, '') ILIKE '[DIV]%'
              )
        ) scoped_rows
        ORDER BY row_sort ASC, happened_at DESC, id DESC
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
    ledger_rows = [row for row in rows if _row_scope(row) == "ledger"]
    expense_rows = [
        row
        for row in rows
        if _row_scope(row) == "operating_expense" and _is_operating_expense(row)
    ]
    summary = summarize_cashbook_rows([_row_to_cashbook(row) for row in ledger_rows])
    management = build_management_dashboard(
        orders=[_row_to_order_metric(row) for row in order_rows],
        payroll=[_row_to_payroll_metric(row) for row in payroll_rows],
        expenses=[_row_to_expense_metric(row) for row in expense_rows],
        group_by=group_by,
        cash_income=_decimal(_get(cash_metrics, "cash_income", 0)),
        cash_expense=_decimal(_get(cash_metrics, "cash_expense", 0)),
        div_paid=_decimal(_get(cash_metrics, "div_paid", 0)),
    )
    return {
        "summary": summary,
        "balance": _decimal(balance),
        "ledger": ledger_rows,
        "management": management,
    }


async def build_cleaning_dashboard(
    conn, *, start_utc: datetime, end_utc: datetime, group_by: str = "day"
) -> dict[str, Any]:
    order_rows = await conn.fetch(
        """
        SELECT o.id,
               o.happened_at AS created_at,
               o.foreman_id AS master_id,
               TRIM(COALESCE(f.fn, '') || ' ' || COALESCE(f.ln, '')) AS master_name,
               COALESCE(o.total_amount, 0) AS amount_total,
               COALESCE(op.live_money, o.total_amount - o.bonuses_used, 0) AS amount_cash,
               COALESCE(o.bonuses_used, 0) AS bonus_spent,
               COALESCE(o.bonuses_earned, 0) AS bonus_earned
        FROM cleaning_orders o
        LEFT JOIN cleaning_foremen f ON f.id = o.foreman_id
        LEFT JOIN (
            SELECT order_id, COALESCE(SUM(amount), 0) AS live_money
            FROM cleaning_order_payments
            WHERE method <> 'Подарочный сертификат'
            GROUP BY order_id
        ) op ON op.order_id = o.id
        WHERE o.happened_at >= $1
          AND o.happened_at <  $2
          AND o.deleted_at IS NULL
        ORDER BY o.happened_at DESC, o.id DESC
        """,
        start_utc,
        end_utc,
    )
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
    expense_rows = [
        row
        for row in rows
        if _row_to_cashbook(row).kind == "expense" and not _row_to_cashbook(row).is_deleted
    ]
    management = build_management_dashboard(
        orders=[_row_to_order_metric(row) for row in order_rows],
        payroll=[],
        expenses=[_row_to_expense_metric(row) for row in expense_rows],
        group_by=group_by,
    )
    return {
        "summary": summary,
        "balance": _decimal(balance),
        "gift_total": _decimal(gift_total),
        "ledger": rows,
        "management": management,
    }
