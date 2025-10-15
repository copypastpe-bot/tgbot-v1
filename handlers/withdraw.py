from __future__ import annotations

import logging
from decimal import Decimal

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    ContentType,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)

from services.db import get_master_wallet
from utils.ui import show_admin_menu, admin_root_kb

router = Router(name="withdraw")
logger = logging.getLogger(__name__)
logging.getLogger("diag").warning("ROUTER LOADED: %s", __name__)


class WithdrawFSM(StatesGroup):
    waiting_amount = State()
    waiting_master = State()
    waiting_comment = State()


def withdraw_nav_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Назад"), KeyboardButton(text="Отмена")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def format_money(amount: Decimal) -> str:
    q = (amount or Decimal(0)).quantize(Decimal("0.1"))
    int_part, frac_part = f"{q:.1f}".split('.')
    int_formatted = f"{int(int_part):,}".replace(',', ' ')
    return f"{int_formatted},{frac_part}"


async def build_masters_kb(conn) -> InlineKeyboardMarkup:
    masters = await conn.fetch(
        "SELECT id, COALESCE(first_name,'') AS fn, COALESCE(last_name,'') AS ln "
        "FROM staff WHERE role='master' AND is_active=true ORDER BY fn, ln, id"
    )

    buttons: list[InlineKeyboardButton] = []
    for r in masters:
        cash_on_hand, _withdrawn = await get_master_wallet(conn, r['id'])
        display_name = f"{r['fn']} {r['ln']}".strip() or f"Мастер {r['id']}"
        amount_str = format_money(cash_on_hand)
        label = f"{display_name}({amount_str})"
        buttons.append(
            InlineKeyboardButton(
                text=label,
                callback_data=f"withdraw_master:{r['id']}",
            )
        )

    markup = InlineKeyboardMarkup(row_width=2)

    for i in range(0, len(buttons), 2):
        markup.row(*buttons[i:i + 2])

    markup.row(
        InlineKeyboardButton(text="Назад", callback_data="withdraw_nav:back"),
        InlineKeyboardButton(text="Отмена", callback_data="withdraw_nav:cancel"),
    )

    return markup


def parse_amount_ru(text: str) -> tuple[Decimal | None, dict]:
    raw = (text or "").strip()
    dbg: dict[str, object] = {"raw": raw}

    normalized = raw.replace("\u00A0", " ")
    normalized = normalized.replace(" ", "")
    dbg["no_spaces"] = normalized

    normalized = normalized.replace(",", ".")
    dbg["comma_to_dot"] = normalized

    if normalized.count(".") > 1:
        dbg["error"] = "too_many_decimal_points"
        return None, dbg

    if not any(ch.isdigit() for ch in normalized):
        dbg["error"] = "no_digits"
        return None, dbg

    try:
        value = Decimal(normalized)
    except Exception as exc:  # noqa: BLE001
        dbg["error"] = f"decimal_error:{exc}"
        return None, dbg

    value = value.quantize(Decimal("0.1"))
    dbg["value"] = str(value)

    if value <= 0:
        dbg["error"] = "non_positive"
        return None, dbg

    return value, dbg


@router.message(F.text == "Изъятие")
async def admin_withdraw_entry(msg: Message, state: FSMContext):
    from bot import has_permission, AdminMenuFSM  # type: ignore

    current_state = await state.get_state()
    if current_state != AdminMenuFSM.root.state:
        return

    if not await has_permission(msg.from_user.id, "record_cashflows"):
        return await msg.answer("Только для администраторов.")

    await state.set_state(WithdrawFSM.waiting_amount)
    return await msg.answer(
        "Введите сумму изъятия (рубли, целое или с копейками):",
        reply_markup=withdraw_nav_kb(),
    )


@router.message(WithdrawFSM.waiting_amount, F.text.lower() == "назад")
async def withdraw_amount_back(msg: Message, state: FSMContext):
    logging.info(f"[withdraw] step=amount_back user={msg.from_user.id} text={msg.text}")
    await state.clear()
    await show_admin_menu(msg, state, "Операция отменена.")


@router.message(WithdrawFSM.waiting_amount, F.text.lower() == "отмена")
async def withdraw_amount_cancel(msg: Message, state: FSMContext):
    logging.info(f"[withdraw] step=amount_cancel user={msg.from_user.id} text={msg.text}")
    await state.clear()
    await show_admin_menu(msg, state, "Операция отменена.")


@router.message(WithdrawFSM.waiting_amount, F.text)
async def withdraw_amount_got(msg: Message, state: FSMContext):
    from bot import pool as _pool  # type: ignore

    logger.debug(
        f"[withdraw amount] state={await state.get_state()} user={msg.from_user.id} text={msg.text!r}"
    )
    amount, dbg = parse_amount_ru(msg.text or "")
    logger.debug(f"[withdraw amount] parse_dbg={dbg}")
    if amount is None:
        return await msg.answer(
            "Не понял сумму. Пример: 2 500 или 2500,5",
            reply_markup=withdraw_nav_kb(),
        )

    await state.update_data(withdraw_amount=str(amount))
    async with _pool.acquire() as conn:
        kb = await build_masters_kb(conn)
    await state.set_state(WithdrawFSM.waiting_master)
    return await msg.answer(
        "Выберите мастера:\nДоступно к изъятию = только наличные, которые мастер получил по заказам. "
        "Изъятия уменьшают этот остаток.",
        reply_markup=kb,
    )


@router.message(WithdrawFSM.waiting_master, F.text.lower() == "назад")
async def withdraw_master_back(msg: Message, state: FSMContext):
    logging.info(f"[withdraw] step=master_back user={msg.from_user.id} text={msg.text}")
    await state.update_data(withdraw_master_id=None, withdraw_master_name=None)
    await state.set_state(WithdrawFSM.waiting_amount)
    await msg.answer("Введите сумму изъятия:", reply_markup=withdraw_nav_kb())


@router.message(WithdrawFSM.waiting_master, F.text.lower() == "отмена")
async def withdraw_master_cancel(msg: Message, state: FSMContext):
    logging.info(f"[withdraw] step=master_cancel user={msg.from_user.id} text={msg.text}")
    await state.clear()
    await show_admin_menu(msg, state, "Операция отменена.")


@router.callback_query(WithdrawFSM.waiting_master)
async def withdraw_master_callback(query: CallbackQuery, state: FSMContext):
    from bot import pool as _pool  # type: ignore

    logging.info(
        f"[withdraw] step=master_callback user={query.from_user.id} data={query.data}"
    )
    data = (query.data or "").strip()

    if data == "withdraw_nav:back":
        await query.answer()
        await state.update_data(withdraw_master_id=None, withdraw_master_name=None)
        await state.set_state(WithdrawFSM.waiting_amount)
        await query.message.answer(
            "Введите сумму изъятия:", reply_markup=withdraw_nav_kb()
        )
        return

    if data == "withdraw_nav:cancel":
        await query.answer()
        await state.clear()
        await show_admin_menu(query.message, state, "Операция отменена.")
        return

    if not data.startswith("withdraw_master:"):
        await query.answer("Неизвестное действие", show_alert=True)
        return

    try:
        master_id = int(data.split(":", 1)[1])
    except Exception:
        await query.answer("Некорректные данные", show_alert=True)
        return

    async with _pool.acquire() as conn:
        master_row = await conn.fetchrow(
            "SELECT COALESCE(first_name,'') AS fn, COALESCE(last_name,'') AS ln FROM staff WHERE id=$1",
            master_id,
        )
        if not master_row:
            await query.answer("Мастер не найден", show_alert=True)
            return
        cash_on_hand, withdrawn = await get_master_wallet(conn, master_id)

    stored = await state.get_data()
    amount = Decimal(stored.get("withdraw_amount", "0") or "0").quantize(Decimal("0.1"))

    if amount > cash_on_hand:
        await query.answer("Недостаточно наличных", show_alert=True)
        await state.set_state(WithdrawFSM.waiting_amount)
        await state.update_data(withdraw_master_id=None, withdraw_master_name=None)
        await query.message.answer(
            f"У мастера на руках {format_money(cash_on_hand)}₽ (изъято ранее {format_money(withdrawn)}₽). "
            f"Вы запросили {format_money(amount)}₽ — это больше доступного.\n"
            f"Введите сумму не больше {format_money(cash_on_hand)}₽:",
            reply_markup=withdraw_nav_kb(),
        )
        return

    await query.answer()
    display_name = f"{(master_row['fn'] or '').strip()} {(master_row['ln'] or '').strip()}".strip() or f"ID {master_id}"
    await state.update_data(
        withdraw_master_id=master_id,
        withdraw_master_name=display_name,
    )
    await state.set_state(WithdrawFSM.waiting_comment)
    await query.message.answer(
        "Комментарий (или «Без комментария»):",
        reply_markup=withdraw_nav_kb(),
    )


@router.callback_query(F.data.startswith("withdraw_"))
async def withdraw_unexpected_callback(query: CallbackQuery, state: FSMContext):
    current = await state.get_state()
    if current != WithdrawFSM.waiting_master.state:
        logging.info(
            f"[withdraw] step=unexpected_callback user={query.from_user.id} data={query.data} state={current}"
        )
        await query.answer("Пожалуйста, завершите текущий шаг.", show_alert=True)
    else:
        await query.answer()


@router.message(WithdrawFSM.waiting_master)
async def withdraw_master_got(msg: Message, state: FSMContext):
    from bot import pool as _pool  # type: ignore

    logging.info(f"[withdraw] step=master_text_input user={msg.from_user.id} text={msg.text}")
    async with _pool.acquire() as conn:
        kb = await build_masters_kb(conn)
    return await msg.answer("Пожалуйста, выберите мастера кнопкой ниже.", reply_markup=kb)


@router.message(WithdrawFSM.waiting_comment, F.text.lower() == "назад")
async def withdraw_comment_back(msg: Message, state: FSMContext):
    from bot import pool as _pool  # type: ignore

    logging.info(f"[withdraw] step=comment_back user={msg.from_user.id} text={msg.text}")
    async with _pool.acquire() as conn:
        kb = await build_masters_kb(conn)
    await state.update_data(withdraw_master_id=None, withdraw_master_name=None)
    await state.set_state(WithdrawFSM.waiting_master)
    await msg.answer(
        "Выберите мастера:\nДоступно к изъятию = только наличные, которые мастер получил по заказам. "
        "Изъятия уменьшают этот остаток.",
        reply_markup=kb,
    )


@router.message(WithdrawFSM.waiting_comment, F.text.lower() == "отмена")
async def withdraw_comment_cancel(msg: Message, state: FSMContext):
    logging.info(f"[withdraw] step=comment_cancel user={msg.from_user.id} text={msg.text}")
    await state.clear()
    await show_admin_menu(msg, state, "Операция отменена.")


@router.message(WithdrawFSM.waiting_comment, F.text)
async def withdraw_finish(msg: Message, state: FSMContext):
    from bot import pool as _pool, AdminMenuFSM  # type: ignore

    logging.info(f"[withdraw] step=finish user={msg.from_user.id} text={msg.text}")
    data = await state.get_data()
    try:
        amount = Decimal(data.get("withdraw_amount", "0") or "0").quantize(Decimal("0.1"))
        master_id = int(data.get("withdraw_master_id"))
    except Exception:
        await state.set_state(AdminMenuFSM.root)
        return await msg.answer("Сессия изъятия потеряна. Попробуйте снова.", reply_markup=admin_root_kb())
    comment_raw = (msg.text or "").strip()
    if not comment_raw or comment_raw.lower() == "без комментария":
        comment = "—"
    else:
        comment = comment_raw

    master_name = data.get("withdraw_master_name")

    async with _pool.acquire() as conn:
        if not master_name:
            row = await conn.fetchrow(
                "SELECT COALESCE(first_name,'') AS fn, COALESCE(last_name,'') AS ln FROM staff WHERE id=$1",
                master_id,
            )
            master_name = f"{(row['fn'] or '').strip()} {(row['ln'] or '').strip()}".strip() or f"ID {master_id}"
        tx = await conn.fetchrow(
            """
            INSERT INTO cashbook_entries(kind, method, amount, comment, order_id, master_id, created_by, happened_at)
            VALUES ('withdrawal', 'cash', $1, $2, NULL, $3, $4, now())
            RETURNING id
            """,
            amount,
            comment,
            master_id,
            msg.from_user.id,
        )

    await state.clear()
    await state.set_state(AdminMenuFSM.root)
    formatted_amount = format_money(amount)
    return await msg.answer(
        "Изъятие оформлено:\n"
        f"• Мастер: {master_name}\n"
        f"• Сумма: {formatted_amount}₽\n"
        f"• Комментарий: {comment}\n"
        f"• Транзакция №{tx['id']}",
        reply_markup=admin_root_kb(),
    )
