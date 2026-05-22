"""Доступ бригадиров к клининг-сценариям."""

from __future__ import annotations

import asyncpg


async def is_cleaning_foreman(conn: asyncpg.Connection, tg_user_id: int) -> bool:
    row = await conn.fetchval(
        "SELECT 1 FROM cleaning_foremen WHERE tg_user_id = $1 AND is_active = true",
        tg_user_id,
    )
    return bool(row)


async def get_user_role(conn: asyncpg.Connection, tg_user_id: int) -> str | None:
    return await conn.fetchval(
        "SELECT role FROM staff WHERE tg_user_id = $1 AND is_active LIMIT 1",
        tg_user_id,
    )


async def has_permission(
    conn: asyncpg.Connection, tg_user_id: int, permission_name: str
) -> bool:
    role = await get_user_role(conn, tg_user_id)
    if role is None:
        return False
    if role == "superadmin":
        return True
    row = await conn.fetchval(
        """
        SELECT 1
        FROM role_permissions rp
        JOIN permissions p ON p.id = rp.permission_id
        WHERE rp.role = $1 AND p.name = $2
        LIMIT 1
        """,
        role,
        permission_name,
    )
    return bool(row)


async def is_cleaning_admin(conn: asyncpg.Connection, tg_user_id: int) -> bool:
    role = await get_user_role(conn, tg_user_id)
    return role in {"admin", "superadmin"}


async def can_create_cleaning_order(conn: asyncpg.Connection, tg_user_id: int) -> bool:
    return await has_permission(conn, tg_user_id, "cleaning_create_orders") and await is_cleaning_foreman(conn, tg_user_id)


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
