"""Доступ бригадиров к клининг-сценариям."""

from __future__ import annotations

import asyncpg


async def is_cleaning_foreman(conn: asyncpg.Connection, tg_user_id: int) -> bool:
    row = await conn.fetchval(
        "SELECT 1 FROM cleaning_foremen WHERE tg_user_id = $1 AND is_active = true",
        tg_user_id,
    )
    return bool(row)


async def get_foreman(conn: asyncpg.Connection, tg_user_id: int) -> asyncpg.Record | None:
    return await conn.fetchrow(
        "SELECT id, tg_user_id, fn, ln, phone FROM cleaning_foremen "
        "WHERE tg_user_id = $1 AND is_active = true",
        tg_user_id,
    )


async def list_active_foremen(conn: asyncpg.Connection) -> list[asyncpg.Record]:
    return await conn.fetch(
        "SELECT id, tg_user_id, fn, ln, phone FROM cleaning_foremen "
        "WHERE is_active = true ORDER BY id"
    )
