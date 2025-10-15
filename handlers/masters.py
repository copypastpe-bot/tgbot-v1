from __future__ import annotations

import logging

from aiogram import Router, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton

import sys

from utils.ui import show_admin_menu


def _get_main_attr(name: str):
    module = sys.modules.get("__main__")
    if module is None:
        raise AttributeError(f"__main__ module not found while accessing {name}")
    if not hasattr(module, name):
        raise AttributeError(f"{name} is not available on __main__")
    return getattr(module, name)

router = Router(name="masters")
logger = logging.getLogger(__name__)

has_permission = _get_main_attr("has_permission")
normalize_phone_for_db = _get_main_attr("normalize_phone_for_db")
_pool = _get_main_attr("pool")


class AdminMastersFSM(StatesGroup):
    root = State()
    remove_wait_phone = State()


class AddMasterFSM(StatesGroup):
    waiting_first_name = State()
    waiting_last_name = State()
    waiting_phone = State()


def admin_masters_kb() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="Добавить мастера"), KeyboardButton(text="Список мастеров")],
        [KeyboardButton(text="Деактивировать мастера")],
        [KeyboardButton(text="Назад"), KeyboardButton(text="Отмена")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=True)


@router.message(AdminMastersFSM.root, F.text == "Мастера")
async def admin_masters_root(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "add_master"):
        return await msg.answer("Только для администраторов.")
    await msg.answer("Мастера: выбери действие.", reply_markup=admin_masters_kb())


@router.message(AdminMastersFSM.root, F.text == "Назад")
async def admin_masters_back(msg: Message, state: FSMContext):
    await show_admin_menu(msg, state)


@router.message(AdminMastersFSM.root, F.text == "Отмена")
async def admin_masters_cancel(msg: Message, state: FSMContext):
    await show_admin_menu(msg, state)


@router.message(AdminMastersFSM.root, F.text == "Список мастеров")
async def admin_masters_list(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "add_master"):
        return await msg.answer("Только для администраторов.")
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT s.id, s.tg_user_id, s.role, s.is_active, COALESCE(s.first_name,'') AS fn, COALESCE(s.last_name,'') AS ln "
            "FROM staff s WHERE role IN ('master','admin') ORDER BY role DESC, id"
        )
    if not rows:
        await msg.answer("Список пуст.")
    else:
        lines = [
            f"#{r['id']} {r['role']} {r['fn']} {r['ln']} | tg={r['tg_user_id']} {'✅' if r['is_active'] else '⛔️'}"
            for r in rows
        ]
        await msg.answer("Мастера/админы:\n" + "\n".join(lines))
    await show_admin_menu(msg, state)


@router.message(Command("list_masters"))
async def list_masters(msg: Message):
    if not await has_permission(msg.from_user.id, "add_master"):
        return await msg.answer("Только для администраторов.")
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT s.id, s.tg_user_id, s.role, s.is_active, COALESCE(s.first_name,'') AS fn, COALESCE(s.last_name,'') AS ln "
            "FROM staff s WHERE role IN ('master','admin') ORDER BY role DESC, id"
        )
    if not rows:
        return await msg.answer("Список пуст.")
    lines = [
        f"#{r['id']} {r['role']} {r['fn']} {r['ln']} | tg={r['tg_user_id']} {'✅' if r['is_active'] else '⛔️'}"
        for r in rows
    ]
    await msg.answer("Мастера/админы:\n" + "\n".join(lines))


@router.message(Command("add_master"))
async def add_master(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "add_master"):
        return await msg.answer("Только для администраторов.")
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        return await msg.answer("Формат: /add_master <tg_user_id>")
    try:
        target_id = int(parts[1].lstrip("@"))
    except Exception:
        return await msg.answer("Нужно указать числовой tg_user_id.")
    await state.clear()
    await state.update_data(tg_id=target_id)
    await state.set_state(AddMasterFSM.waiting_first_name)
    await msg.answer("Имя мастера:")


@router.message(AddMasterFSM.waiting_first_name)
async def add_master_first(msg: Message, state: FSMContext):
    first = (msg.text or "").strip()
    if len(first) < 2:
        return await msg.answer("Имя слишком короткое. Введите корректное имя (>= 2 символов).")
    await state.update_data(first_name=first)
    await state.set_state(AddMasterFSM.waiting_last_name)
    await msg.answer("Фамилия мастера:")


@router.message(AddMasterFSM.waiting_last_name)
async def add_master_last(msg: Message, state: FSMContext):
    last = (msg.text or "").strip()
    if len(last) < 2:
        return await msg.answer("Фамилия слишком короткая. Введите корректную фамилию (>= 2 символов).")
    await state.update_data(last_name=last)
    await state.set_state(AddMasterFSM.waiting_phone)
    await msg.answer("Телефон мастера (формат: +7XXXXXXXXXX или 8/9...):")


@router.message(AddMasterFSM.waiting_phone)
async def add_master_phone(msg: Message, state: FSMContext):
    phone_norm = normalize_phone_for_db(msg.text)
    if not phone_norm or not phone_norm.startswith("+7"):
        return await msg.answer("Не распознал телефон. Пример: +7XXXXXXXXXX. Введите ещё раз.")
    data = await state.get_data()
    if len((data.get("first_name") or "").strip()) < 2 or len((data.get("last_name") or "").strip()) < 2:
        return await msg.answer("Имя/фамилия заданы некорректно. Перезапустите /add_master.")
    tg_id = int(data["tg_id"])
    async with _pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO staff(tg_user_id, role, is_active, first_name, last_name, phone) "
            "VALUES ($1,'master',true,$2,$3,$4) "
            "ON CONFLICT (tg_user_id) DO UPDATE SET role='master', is_active=true, first_name=$2, last_name=$3, phone=$4",
            tg_id, data.get("first_name"), data.get("last_name"), phone_norm
        )
    await msg.answer(f"✅ Мастер добавлен: {data.get('first_name','')} {data.get('last_name','')}, tg={tg_id}")
    await state.clear()


@router.message(AdminMastersFSM.root, F.text == "Деактивировать мастера")
async def admin_masters_remove_start(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "add_master"):
        return await msg.answer("Только для администраторов.")
    await state.set_state(AdminMastersFSM.remove_wait_phone)
    await msg.answer("Введите телефон мастера (8/+7/9...):")


@router.message(AdminMastersFSM.remove_wait_phone, F.text == "Назад")
async def admin_masters_remove_back(msg: Message, state: FSMContext):
    await state.set_state(AdminMastersFSM.root)
    await msg.answer("Мастера: выбери действие.", reply_markup=admin_masters_kb())


@router.message(AdminMastersFSM.remove_wait_phone, F.text == "Отмена")
async def admin_masters_remove_cancel(msg: Message, state: FSMContext):
    await state.clear()
    await show_admin_menu(msg, state)


@router.message(AdminMastersFSM.remove_wait_phone, F.text)
async def admin_masters_remove_phone(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "add_master"):
        await state.clear()
        await show_admin_menu(msg, state, "Только для администраторов.")
        return
    phone = normalize_phone_for_db(msg.text)
    if not phone or not phone.startswith("+7"):
        return await msg.answer("Неверный телефон. Пример: +7XXXXXXXXXX. Введите ещё раз.")
    async with _pool.acquire() as conn:
        rec = await conn.fetchrow(
            "SELECT id FROM staff WHERE phone=$1 AND role='master' LIMIT 1",
            phone,
        )
        if not rec:
            await state.clear()
            await show_admin_menu(msg, state, "Мастер не найден по этому телефону.")
            return
        await conn.execute("UPDATE staff SET is_active=false WHERE id=$1", rec["id"])
    await state.clear()
    await show_admin_menu(msg, state, "Мастер деактивирован.")


@router.message(Command("remove_master"))
async def remove_master(msg: Message):
    if not await has_permission(msg.from_user.id, "add_master"):
        return await msg.answer("Только для администраторов.")
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        return await msg.answer("Формат: /remove_master <tg_user_id>")
    try:
        target_id = int(parts[1].lstrip("@"))
    except Exception:
        return await msg.answer("Нужно указать числовой tg_user_id.")
    async with _pool.acquire() as conn:
        await conn.execute("UPDATE staff SET is_active=false WHERE tg_user_id=$1 AND role='master'", target_id)
    await msg.answer(f"Пользователь {target_id} деактивирован как мастер.")
