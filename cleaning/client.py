"""Работа с общей таблицей clients из клининг-сценария.

Намеренно не переиспользуем функцию _find_client_by_phone из bot.py —
держим контракт явным: cleaning/* не лезет внутрь монолита,
только в clients-таблицу.
"""

from __future__ import annotations

import re

import asyncpg


def _digits(value: str | None) -> str:
    return re.sub(r"[^0-9]", "", value or "")


def normalize_phone(phone_raw: str) -> str:
    """Приводит «8 904 ...», «+7 904 ...», «9047...» к +7XXXXXXXXXX.

    Возвращает исходную строку, если приведение невозможно.
    """
    digits = _digits(phone_raw)
    if len(digits) == 11 and digits[0] in {"7", "8"}:
        return "+7" + digits[1:]
    if len(digits) == 10 and digits[0] == "9":
        return "+7" + digits
    return phone_raw.strip()


async def find_client_by_phone(
    conn: asyncpg.Connection, phone_raw: str
) -> asyncpg.Record | None:
    norm_digits = _digits(normalize_phone(phone_raw))
    raw_digits = _digits(phone_raw)
    candidates: list[str] = []
    if norm_digits:
        candidates.append(norm_digits)
    if raw_digits and raw_digits != norm_digits:
        candidates.append(raw_digits)
    if not candidates:
        return None
    return await conn.fetchrow(
        """
        SELECT id, full_name, phone, address, birthday, status, bonus_balance
        FROM clients
        WHERE regexp_replace(COALESCE(phone,''), '[^0-9]+', '', 'g') = ANY($1::text[])
        LIMIT 1
        """,
        candidates,
    )


async def upsert_client(
    conn: asyncpg.Connection, *, full_name: str, phone_norm: str
) -> asyncpg.Record:
    """Создать клиента или вернуть существующего по phone.

    Mirrors the laundry confirm_order upsert: status='client', address не
    трогаем, bonus_balance не обнуляем (ON CONFLICT keeps existing).
    """
    return await conn.fetchrow(
        """
        INSERT INTO clients (full_name, phone, bonus_balance, status)
        VALUES ($1, $2, 0, 'client')
        ON CONFLICT (phone) DO UPDATE SET
            full_name = COALESCE(EXCLUDED.full_name, clients.full_name),
            status    = 'client'
        RETURNING id, full_name, phone, address, bonus_balance
        """,
        full_name,
        phone_norm,
    )
