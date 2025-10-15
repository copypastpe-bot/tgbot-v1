from __future__ import annotations

import logging
import re

import asyncpg
from aiogram import Router, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton

from utils.ui import show_admin_menu, admin_root_kb

router = Router(name="clients")
logger = logging.getLogger(__name__)


class AdminClientsFSM(StatesGroup):
    find_wait_phone = State()
    edit_wait_phone = State()
    edit_pick_field = State()
    edit_wait_value = State()


def admin_clients_kb() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="Найти клиента"), KeyboardButton(text="Редактировать клиента")],
        [KeyboardButton(text="Назад"), KeyboardButton(text="Отмена")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=True)


def client_edit_fields_kb() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="Имя"), KeyboardButton(text="Телефон")],
        [KeyboardButton(text="ДР"), KeyboardButton(text="Бонусы установить")],
        [KeyboardButton(text="Бонусы добавить/убавить")],
        [KeyboardButton(text="Назад"), KeyboardButton(text="Отмена")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=True)


def _fmt_client_row(rec) -> str:
    bday = rec["birthday"].strftime("%Y-%m-%d") if rec["birthday"] else "—"
    return "\n".join([
        f"id: {rec['id']}",
        f"Имя: {rec['full_name'] or '—'}",
        f"Телефон: {rec['phone'] or '—'}",
        f"ДР: {bday}",
        f"Бонусы: {rec['bonus_balance']}",
        f"Статус: {rec['status']}",
    ])


async def _find_client_by_phone(conn: asyncpg.Connection, phone_input: str):
    from bot import normalize_phone_for_db  # type: ignore

    s = phone_input or ""
    norm = normalize_phone_for_db(s)
    norm_digits = re.sub(r"[^0-9]", "", norm or "")
    raw_digits = re.sub(r"[^0-9]", "", s)

    candidates: list[str] = []
    if norm_digits:
        candidates.append(norm_digits)
    if raw_digits and raw_digits != norm_digits:
        candidates.append(raw_digits)
    if not candidates:
        return None

    rec = await conn.fetchrow(
        """
        SELECT id, full_name, phone, birthday, bonus_balance, status
        FROM clients
        WHERE regexp_replace(COALESCE(phone,''), '[^0-9]+', '', 'g') = ANY($1::text[])
        """,
        candidates,
    )
    return rec


@router.message(StateFilter("AdminMenuFSM:root"), F.text == "Клиенты")
async def admin_clients_root(msg: Message, state: FSMContext):
    from bot import has_permission  # type: ignore

    if not await has_permission(msg.from_user.id, "edit_client"):
        return await msg.answer("Только для администраторов.")
    await state.set_state("AdminMenuFSM:clients")
    await msg.answer("Клиенты: выбери действие.", reply_markup=admin_clients_kb())


@router.message(StateFilter("AdminMenuFSM:clients"), F.text == "Найти клиента")
async def client_find_start(msg: Message, state: FSMContext):
    await state.set_state(AdminClientsFSM.find_wait_phone)
    await msg.answer("Введите номер телефона клиента (8/ +7/ 9...):")


@router.message(StateFilter("AdminMenuFSM:clients"), F.text == "Редактировать клиента")
async def client_edit_start(msg: Message, state: FSMContext):
    await state.set_state(AdminClientsFSM.edit_wait_phone)
    await msg.answer("Введите номер телефона клиента для редактирования:")


@router.message(StateFilter("AdminMenuFSM:clients"), F.text == "Назад")
async def admin_clients_back(msg: Message, state: FSMContext):
    await show_admin_menu(msg, state)


@router.message(StateFilter("AdminMenuFSM:clients"), F.text == "Отмена")
async def admin_clients_cancel(msg: Message, state: FSMContext):
    await show_admin_menu(msg, state)


@router.message(
    StateFilter(
        AdminClientsFSM.find_wait_phone,
        AdminClientsFSM.edit_wait_phone,
        AdminClientsFSM.edit_pick_field,
        AdminClientsFSM.edit_wait_value,
    ),
    F.text == "Назад",
)
async def admin_clients_states_back(msg: Message, state: FSMContext):
    await state.clear()
    await state.set_state("AdminMenuFSM:clients")
    await msg.answer("Клиенты: выбери действие.", reply_markup=admin_clients_kb())


@router.message(
    StateFilter(
        AdminClientsFSM.find_wait_phone,
        AdminClientsFSM.edit_wait_phone,
        AdminClientsFSM.edit_pick_field,
        AdminClientsFSM.edit_wait_value,
    ),
    F.text == "Отмена",
)
async def admin_clients_states_cancel(msg: Message, state: FSMContext):
    await state.clear()
    await show_admin_menu(msg, state)


@router.message(AdminClientsFSM.find_wait_phone)
async def client_find_got_phone(msg: Message, state: FSMContext):
    from bot import pool as _pool  # type: ignore

    async with _pool.acquire() as conn:
        rec = await _find_client_by_phone(conn, msg.text)
    if not rec:
        await state.clear()
        await show_admin_menu(msg, state, "Клиент не найден.")
        return
    await state.clear()
    await show_admin_menu(msg, state, _fmt_client_row(rec))


@router.message(AdminClientsFSM.edit_wait_phone)
async def client_edit_got_phone(msg: Message, state: FSMContext):
    from bot import pool as _pool  # type: ignore

    async with _pool.acquire() as conn:
        rec = await _find_client_by_phone(conn, msg.text)
    if not rec:
        await state.clear()
        await show_admin_menu(msg, state, "Клиент не найден.")
        return
    await state.update_data(client_id=rec["id"])
    await state.set_state(AdminClientsFSM.edit_pick_field)
    await msg.answer("Что изменить?", reply_markup=client_edit_fields_kb())


@router.message(
    AdminClientsFSM.edit_pick_field,
    F.text.in_({"Имя", "Телефон", "ДР", "Бонусы установить", "Бонусы добавить/убавить"}),
)
async def client_edit_pick_field(msg: Message, state: FSMContext):
    await state.update_data(edit_field=msg.text)
    prompt = {
        "Имя": "Введите новое имя:",
        "Телефон": "Введите новый телефон (+7 / 8 / 9...):",
        "ДР": "Введите дату (DD.MM.YYYY или YYYY-MM-DD):",
        "Бонусы установить": "Введите новое количество бонусов (целое число):",
        "Бонусы добавить/убавить": "Введите дельту бонусов (целое число, можно со знаком -/+):",
    }[msg.text]
    await state.set_state(AdminClientsFSM.edit_wait_value)
    await msg.answer(prompt)


@router.message(AdminClientsFSM.edit_wait_value)
async def client_edit_apply(msg: Message, state: FSMContext):
    from bot import pool as _pool, normalize_phone_for_db, parse_birthday_str  # type: ignore

    data = await state.get_data()
    client_id = data.get("client_id")
    field = data.get("edit_field")
    if not client_id or not field:
        await state.clear()
        return await msg.answer("Сессия сброшена, попробуйте заново.", reply_markup=admin_root_kb())

    async with _pool.acquire() as conn:
        if field == "Имя":
            await conn.execute(
                "UPDATE clients SET full_name=$1, last_updated=NOW() WHERE id=$2",
                (msg.text or "").strip(),
                client_id,
            )
        elif field == "Телефон":
            new_phone = normalize_phone_for_db(msg.text)
            if not new_phone or not new_phone.startswith("+7"):
                return await msg.answer("Неверный телефон. Пример: +7XXXXXXXXXX. Введите ещё раз.")
            await conn.execute(
                "UPDATE clients SET phone=$1, last_updated=NOW() WHERE id=$2",
                new_phone,
                client_id,
            )
        elif field == "ДР":
            b = parse_birthday_str(msg.text)
            if not b:
                return await msg.answer("Неверная дата. Форматы: DD.MM.YYYY / YYYY-MM-DD. Введите ещё раз.")
            await conn.execute(
                "UPDATE clients SET birthday=$1, last_updated=NOW() WHERE id=$2",
                b,
                client_id,
            )
        elif field == "Бонусы установить":
            try:
                val = int((msg.text or "0").strip())
            except Exception:
                return await msg.answer("Нужно целое число. Введите ещё раз.")
            await conn.execute(
                "UPDATE clients SET bonus_balance=$1, last_updated=NOW() WHERE id=$2",
                val,
                client_id,
            )
        elif field == "Бонусы добавить/убавить":
            try:
                delta = int((msg.text or "0").strip())
            except Exception:
                return await msg.answer("Нужно целое число (можно со знаком). Введите ещё раз.")
            rec = await conn.fetchrow(
                "SELECT bonus_balance FROM clients WHERE id=$1",
                client_id,
            )
            current_bonus = int(rec["bonus_balance"] or 0) if rec else 0
            new_bonus = current_bonus + delta
            if new_bonus < 0:
                new_bonus = 0
            await conn.execute(
                "UPDATE clients SET bonus_balance=$1, last_updated=NOW() WHERE id=$2",
                new_bonus,
                client_id,
            )

    await state.clear()
    await show_admin_menu(msg, state, "Готово. Клиент обновлён.")


@router.message(Command("client_info"))
async def client_info(msg: Message):
    from bot import has_permission, pool as _pool  # type: ignore

    if not await has_permission(msg.from_user.id, "edit_client"):
        return await msg.answer("Только для администраторов.")
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        return await msg.answer("Формат: /client_info <телефон>")
    phone_q = parts[1].strip()
    async with _pool.acquire() as conn:
        rec = await _find_client_by_phone(conn, phone_q)
    if not rec:
        return await msg.answer("Клиент не найден по этому номеру.")
    return await msg.answer(_fmt_client_row(rec))


@router.message(Command("client_set_name"))
async def client_set_name(msg: Message):
    from bot import has_permission, pool as _pool  # type: ignore

    if not await has_permission(msg.from_user.id, "edit_client"):
        return await msg.answer("Только для администраторов.")
    parts = msg.text.split(maxsplit=2)
    if len(parts) < 3:
        return await msg.answer("Формат: /client_set_name <телефон> <новое_имя>")
    phone_q = parts[1].strip()
    new_name = parts[2].strip()
    async with _pool.acquire() as conn:
        rec = await _find_client_by_phone(conn, phone_q)
        if not rec:
            return await msg.answer("Клиент не найден по этому номеру.")
        await conn.execute("UPDATE clients SET full_name=$1, last_updated=NOW() WHERE id=$2", new_name, rec["id"])
        rec2 = await conn.fetchrow(
            "SELECT id, full_name, phone, birthday, bonus_balance, status FROM clients WHERE id=$1",
            rec["id"],
        )
    return await msg.answer("Имя обновлено:\n" + _fmt_client_row(rec2))


@router.message(Command("client_set_birthday"))
async def client_set_birthday(msg: Message):
    from bot import has_permission, pool as _pool, normalize_phone_for_db, parse_birthday_str  # type: ignore

    if not await has_permission(msg.from_user.id, "edit_client"):
        return await msg.answer("Только для администраторов.")
    try:
        parts = msg.text.split(maxsplit=2)
        if len(parts) < 3:
            return await msg.answer("Формат: /client_set_birthday <телефон> <ДР: DD.MM.YYYY или YYYY-MM-DD>")
        phone_q = parts[1].strip()
        bday_raw = parts[2].strip()

        bday_date = parse_birthday_str(bday_raw)
        if not bday_date:
            return await msg.answer("Не распознал дату. Форматы: DD.MM.YYYY (допускаются 1-2 цифры) или YYYY-MM-DD.")

        async with _pool.acquire() as conn:
            rec = await _find_client_by_phone(conn, phone_q)
            if not rec:
                norm = normalize_phone_for_db(phone_q)
                digits = re.sub(r"[^0-9]", "", norm or phone_q)
                return await msg.answer(
                    f"Клиент не найден по номеру.\nИскали: {phone_q}\nНормализовано: {norm}\nЦифры: {digits}"
                )

            await conn.execute(
                "UPDATE clients SET birthday=$1, last_updated=NOW() WHERE id=$2",
                bday_date, rec["id"],
            )
            rec2 = await conn.fetchrow(
                "SELECT id, full_name, phone, birthday, bonus_balance, status FROM clients WHERE id=$1",
                rec["id"],
            )

        return await msg.answer("ДР обновлён:\n" + _fmt_client_row(rec2))

    except Exception as exc:  # noqa: BLE001
        logger.exception("client_set_birthday failed")
        return await msg.answer(f"Ошибка при обновлении ДР: {exc}")


@router.message(Command("client_set_bonus"))
async def client_set_bonus(msg: Message):
    from bot import has_permission, pool as _pool  # type: ignore

    if not await has_permission(msg.from_user.id, "edit_client"):
        return await msg.answer("Только для администраторов.")
    parts = msg.text.split(maxsplit=2)
    if len(parts) < 3:
        return await msg.answer("Формат: /client_set_bonus <телефон> <сумма_баллов>")
    phone_q = parts[1].strip()
    try:
        amount = int(parts[2].strip())
    except Exception:
        return await msg.answer("Сумма должна быть целым числом.")
    async with _pool.acquire() as conn:
        rec = await _find_client_by_phone(conn, phone_q)
        if not rec:
            return await msg.answer("Клиент не найден по этому номеру.")
        await conn.execute(
            "UPDATE clients SET bonus_balance=$1, last_updated=NOW() WHERE id=$2",
            amount,
            rec["id"],
        )
        rec2 = await conn.fetchrow(
            "SELECT id, full_name, phone, birthday, bonus_balance, status FROM clients WHERE id=$1",
            rec["id"],
        )
    return await msg.answer("Бонусы установлены:\n" + _fmt_client_row(rec2))


@router.message(Command("client_add_bonus"))
async def client_add_bonus(msg: Message):
    from bot import has_permission, pool as _pool  # type: ignore

    if not await has_permission(msg.from_user.id, "edit_client"):
        return await msg.answer("Только для администраторов.")
    parts = msg.text.split(maxsplit=2)
    if len(parts) < 3:
        return await msg.answer("Формат: /client_add_bonus <телефон> <дельта>")
    phone_q = parts[1].strip()
    try:
        delta = int(parts[2].strip())
    except Exception:
        return await msg.answer("Дельта должна быть целым числом (можно со знаком -/+).")
    async with _pool.acquire() as conn:
        rec = await _find_client_by_phone(conn, phone_q)
        if not rec:
            return await msg.answer("Клиент не найден по этому номеру.")
        new_bonus = int(rec["bonus_balance"] or 0) + delta
        if new_bonus < 0:
            new_bonus = 0
        await conn.execute(
            "UPDATE clients SET bonus_balance=$1, last_updated=NOW() WHERE id=$2",
            new_bonus,
            rec["id"],
        )
        rec2 = await conn.fetchrow(
            "SELECT id, full_name, phone, birthday, bonus_balance, status FROM clients WHERE id=$1",
            rec["id"],
        )
    return await msg.answer("Бонусы обновлены:\n" + _fmt_client_row(rec2))


@router.message(Command("client_set_phone"))
async def client_set_phone(msg: Message):
    from bot import has_permission, pool as _pool, normalize_phone_for_db  # type: ignore

    if not await has_permission(msg.from_user.id, "edit_client"):
        return await msg.answer("Только для администраторов.")
    parts = msg.text.split(maxsplit=2)
    if len(parts) < 3:
        return await msg.answer("Формат: /client_set_phone <старый_телефон> <новый_телефон>")
    phone_q = parts[1].strip()
    new_phone_raw = parts[2].strip()
    new_phone_norm = normalize_phone_for_db(new_phone_raw)
    if (
        not new_phone_norm
        or not new_phone_norm.startswith("+7")
        or len(re.sub(r"[^0-9]", "", new_phone_norm)) != 11
    ):
        return await msg.answer("Не распознал новый телефон. Пример: +7XXXXXXXXXX")
    async with _pool.acquire() as conn:
        rec = await _find_client_by_phone(conn, phone_q)
        if not rec:
            return await msg.answer("Клиент не найден по этому номеру.")
        try:
            await conn.execute(
                "UPDATE clients SET phone=$1, last_updated=NOW() WHERE id=$2",
                new_phone_norm,
                rec["id"],
            )
        except asyncpg.exceptions.UniqueViolationError:
            other = await conn.fetchrow(
                "SELECT id, full_name FROM clients WHERE phone_digits = regexp_replace($1,'[^0-9]','','g') AND id <> $2",
                new_phone_norm,
                rec["id"],
            )
            if other:
                return await msg.answer(
                    f"Номер уже используется клиентом id={other['id']} ({other['full_name'] or '—'})."
                )
            return await msg.answer("Номер уже используется другим клиентом.")
        rec2 = await conn.fetchrow(
            "SELECT id, full_name, phone, birthday, bonus_balance, status FROM clients WHERE id=$1",
            rec["id"],
        )
    return await msg.answer("Телефон обновлён:\n" + _fmt_client_row(rec2))
