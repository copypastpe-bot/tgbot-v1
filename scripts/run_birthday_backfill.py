"""Manually run birthday bonus accrual/expire for a date range."""

from __future__ import annotations

import argparse
import asyncio
import os
from datetime import date, datetime, timedelta, time, timezone

import asyncpg
from dotenv import load_dotenv

MOSCOW_TZ = timezone(timedelta(hours=3))


async def accrue_for_date(conn: asyncpg.Connection, target_date: date) -> int:
    today_local = target_date
    rows = await conn.fetch(
        """
        SELECT id, bonus_balance
        FROM clients
        WHERE birthday IS NOT NULL
          AND EXTRACT(MONTH FROM birthday) = $1
          AND EXTRACT(DAY FROM birthday) = $2
        """,
        today_local.month,
        today_local.day,
    )
    if not rows:
        return 0
    processed = 0
    happened_at = datetime.combine(today_local, time(hour=12), tzinfo=MOSCOW_TZ).astimezone(timezone.utc)
    expires_at = (datetime.combine(today_local, time(), tzinfo=MOSCOW_TZ) + timedelta(days=365)).astimezone(timezone.utc)
    for row in rows:
        client_id = row["id"]
        existing = await conn.fetchval(
            """
            SELECT 1
            FROM bonus_transactions
            WHERE client_id=$1
              AND reason='birthday'
              AND date(happened_at AT TIME ZONE 'Europe/Moscow') = $2
            LIMIT 1
            """,
            client_id,
            today_local,
        )
        if existing:
            continue
        amount = 300
        await conn.execute(
            """
            INSERT INTO bonus_transactions (client_id, delta, reason, created_at, happened_at, expires_at, meta)
            VALUES ($1, $2, 'birthday', $3, $4, $5, jsonb_build_object('bonus_type','birthday','backfill',true))
            """,
            client_id,
            amount,
            happened_at,
            happened_at,
            expires_at,
        )
        await conn.execute(
            "UPDATE clients SET bonus_balance = COALESCE(bonus_balance,0) + $1, last_updated = $2 WHERE id=$3",
            amount,
            happened_at,
            client_id,
        )
        processed += 1
    return processed


async def expire_for_date(conn: asyncpg.Connection, target_date: date) -> int:
    now_utc = datetime.combine(target_date, time(hour=23, minute=59, second=59, microsecond=0), tzinfo=MOSCOW_TZ).astimezone(timezone.utc)
    rows = await conn.fetch(
        """
        SELECT t.id, t.client_id, t.delta
        FROM bonus_transactions t
        WHERE t.delta > 0
          AND t.expires_at IS NOT NULL
          AND t.expires_at <= $1
          AND NOT EXISTS (
                SELECT 1 FROM bonus_transactions e
                WHERE e.meta ->> 'expires_source' = t.id::text
          )
        """,
        now_utc,
    )
    if not rows:
        return 0
    expired = 0
    for row in rows:
        client_id = row["client_id"]
        delta = int(row["delta"])
        if delta <= 0:
            continue
        balance = await conn.fetchval("SELECT COALESCE(bonus_balance,0) FROM clients WHERE id=$1", client_id) or 0
        expire_amount = min(balance, delta)
        if expire_amount <= 0:
            continue
        await conn.execute(
            """
            INSERT INTO bonus_transactions (client_id, delta, reason, created_at, happened_at, meta)
            VALUES ($1, $2, 'expire', $3, $3, jsonb_build_object('expires_source', $4::text, 'backfill', true))
            """,
            client_id,
            -expire_amount,
            now_utc,
            str(row["id"]),
        )
        await conn.execute(
            "UPDATE clients SET bonus_balance = bonus_balance - $1, last_updated = $2 WHERE id=$3",
            expire_amount,
            now_utc,
            client_id,
        )
        expired += 1
    return expired


async def run_backfill(dsn: str, start: date, end: date) -> None:
    conn = await asyncpg.connect(dsn)
    try:
        cur = start
        while cur <= end:
            accr = await accrue_for_date(conn, cur)
            expd = await expire_for_date(conn, cur)
            print(f"{cur}: accrued {accr}, expired {expd}")
            cur += timedelta(days=1)
    finally:
        await conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run birthday bonus backfill for date range")
    parser.add_argument("start", help="Start date YYYY-MM-DD")
    parser.add_argument("end", help="End date YYYY-MM-DD")
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    dsn = os.getenv("DB_DSN")
    if not dsn:
        raise SystemExit("DB_DSN is not set")
    args = parse_args()
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    asyncio.run(run_backfill(dsn, start, end))


if __name__ == "__main__":
    main()
