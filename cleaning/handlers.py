"""aiogram Router with cleaning-order and cleaning-dividend handlers.

Регистрируется в bot.py через dp.include_router(cleaning_router).
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import asyncpg
from aiogram import F, Router
from aiogram.filters import Command, CommandObject, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.types import KeyboardButton, Message, ReplyKeyboardMarkup, ReplyKeyboardRemove
from notifications import NotificationRules, enqueue_notification

from .access import can_create_cleaning_order, has_permission
from .admin_ops import (
    add_cash_expense,
    add_cash_income,
    add_cash_withdrawal,
    cancel_order,
)
from .cashbook import (
    get_cleaning_balance,
    get_cleaning_cash_report,
    get_cleaning_orders_list,
    record_dividend,
    record_expense,
    record_income,
)
from .client import find_client_by_phone, normalize_phone, upsert_client
from .constants import (
    CLEANING_ALL_PAYMENT_LABELS,
    CLEANING_EXPENSE_CATEGORIES,
    CLEANING_GIFT_CERT_LABEL,
    CLEANING_PAYMENT_METHODS,
    ZERO,
)
from .format import (
    format_cancel_order_alert,
    format_cash_op_alert,
    format_cash_report,
    format_dividend_alert,
    format_order_provided_alert,
    format_orders_list,
)

MOSCOW_TZ = ZoneInfo("Europe/Moscow")


def _bonus_expire_label(value: datetime | None) -> str:
    if value is None:
        return "—"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(MOSCOW_TZ).strftime("%d.%m.%Y")


def _period_bounds(kind: str) -> tuple[datetime, datetime, str]:
    """day|month|year → (start_utc, end_utc, human_label).

    Все вычисления через MSK, потом конвертация в UTC для SQL.
    """
    now_msk = datetime.now(MOSCOW_TZ)
    if kind == "day":
        start_msk = now_msk.replace(hour=0, minute=0, second=0, microsecond=0)
        end_msk = start_msk + timedelta(days=1)
        label = "день " + start_msk.strftime("%d.%m.%Y")
    elif kind == "month":
        start_msk = now_msk.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if start_msk.month == 12:
            end_msk = start_msk.replace(year=start_msk.year + 1, month=1)
        else:
            end_msk = start_msk.replace(month=start_msk.month + 1)
        label = "месяц " + start_msk.strftime("%m.%Y")
    elif kind == "year":
        start_msk = now_msk.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        end_msk = start_msk.replace(year=start_msk.year + 1)
        label = "год " + start_msk.strftime("%Y")
    else:
        raise ValueError(f"unknown period: {kind}")
    return start_msk.astimezone(timezone.utc), end_msk.astimezone(timezone.utc), label
from .fsm import (
    CleaningCancelOrderFSM,
    CleaningCashAddFSM,
    CleaningCashExpenseFSM,
    CleaningCashWithdrawalFSM,
    CleaningClientLookupFSM,
    CleaningDividendFSM,
    CleaningForemanExpenseFSM,
    CleaningOrderFSM,
)
from .notify import send_cleaning_money_flow
from .orders import (
    PaymentPart,
    calculate_bonus_earned,
    calculate_bonus_max,
    cashbook_rows_from_payments,
    get_min_order_amount,
    is_wire_payment_method,
    parse_amount,
    validate_payment_parts,
)

logger = logging.getLogger(__name__)

router = Router(name="cleaning")

cancel_kb = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="Отмена")]], resize_keyboard=True
)


def _pay_method_kb(*, allow_wire: bool = True) -> ReplyKeyboardMarkup:
    methods = CLEANING_PAYMENT_METHODS if allow_wire else [
        m for m in CLEANING_PAYMENT_METHODS if not is_wire_payment_method(m)
    ]
    rows = [[KeyboardButton(text=m) for m in methods]]
    rows.append([KeyboardButton(text=CLEANING_GIFT_CERT_LABEL)])
    rows.append([KeyboardButton(text="Отмена")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def _address_choice_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Использовать этот адрес")],
            [KeyboardButton(text="Другой адрес")],
            [KeyboardButton(text="Отмена")],
        ],
        resize_keyboard=True,
    )


def _expense_category_kb() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text=c) for c in CLEANING_EXPENSE_CATEGORIES[:2]],
        [KeyboardButton(text=c) for c in CLEANING_EXPENSE_CATEGORIES[2:]],
        [KeyboardButton(text="Готово"), KeyboardButton(text="Отмена")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def _yes_no_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Да"), KeyboardButton(text="Нет")],
            [KeyboardButton(text="Отмена")],
        ],
        resize_keyboard=True,
    )


def _confirm_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Провести")],
            [KeyboardButton(text="Отменить")],
        ],
        resize_keyboard=True,
    )


def cleaning_main_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🧹 Провести уборку")],
            [KeyboardButton(text="🔍 Клиент"), KeyboardButton(text="💰 Баланс")],
            [KeyboardButton(text="➖ Добавить расход")],
        ],
        resize_keyboard=True,
    )


async def _is_foreman(pool: asyncpg.Pool, tg_user_id: int) -> bool:
    async with pool.acquire() as conn:
        return await can_create_cleaning_order(conn, tg_user_id)


async def _has_permission(pool: asyncpg.Pool, tg_user_id: int, permission: str) -> bool:
    async with pool.acquire() as conn:
        return await has_permission(conn, tg_user_id, permission)


def _is_valid_phone(s: str) -> bool:
    digits = [c for c in s if c.isdigit()]
    return 10 <= len(digits) <= 11


def _money_str(value: Decimal) -> str:
    quant = value.quantize(Decimal("0.01"))
    if quant == quant.to_integral_value():
        return f"{int(quant):,}".replace(",", " ")
    return f"{quant:,.2f}".replace(",", " ")


async def _enqueue_cleaning_completed_notifications(
    conn: asyncpg.Connection,
    rules: NotificationRules | None,
    *,
    order_id: int,
    client_id: int,
    total: Decimal,
    bonuses_used: int,
    bonuses_earned: int,
    bonus_balance: int,
    amount_due: Decimal,
    bonus_expires_at: datetime | None,
    is_wire_payment: bool,
) -> None:
    if rules is None:
        return
    if is_wire_payment:
        await enqueue_notification(
            conn,
            rules,
            event_key="cleaning_order_completed_wire",
            client_id=client_id,
            payload={},
        )
    else:
        await enqueue_notification(
            conn,
            rules,
            event_key="cleaning_order_completed_summary",
            client_id=client_id,
            payload={
                "total_sum": _money_str(total),
                "used_bonus": bonuses_used,
                "earned_bonus": bonuses_earned,
                "bonus_balance": bonus_balance,
                "amount_due": _money_str(amount_due),
                "bonus_expire_date": _bonus_expire_label(bonus_expires_at),
            },
        )
    await enqueue_notification(
        conn,
        rules,
        event_key="cleaning_order_rating_reminder",
        client_id=client_id,
        payload={"order_id": order_id},
    )


# ---------- /cleaning_order ----------


@router.message(Command("cleaning_order"))
@router.message(F.text == "🧹 Провести уборку")
async def start_cleaning_order(msg: Message, state: FSMContext, **data) -> None:
    pool: asyncpg.Pool = data["pool"]
    if not await _is_foreman(pool, msg.from_user.id):
        await msg.answer("Команда доступна только клининг-бригадирам.")
        return
    await state.clear()
    await state.update_data(client_op_id=uuid.uuid4().hex)
    await state.set_state(CleaningOrderFSM.phone)
    await msg.answer(
        "Введите номер клиента (9XXXXXXXXX, 8XXXXXXXXXX или +7XXXXXXXXXX):",
        reply_markup=cancel_kb,
    )


@router.message(
    StateFilter(
        CleaningOrderFSM,
        CleaningDividendFSM,
        CleaningCashAddFSM,
        CleaningCashExpenseFSM,
        CleaningCashWithdrawalFSM,
        CleaningCancelOrderFSM,
        CleaningClientLookupFSM,
        CleaningForemanExpenseFSM,
    ),
    F.text.in_({"Отмена", "Отменить"}),
)
async def cancel(msg: Message, state: FSMContext) -> None:
    await state.clear()
    await msg.answer("Отменено.", reply_markup=cleaning_main_kb())


@router.message(CleaningOrderFSM.phone, F.text)
async def got_phone(msg: Message, state: FSMContext, **data) -> None:
    pool: asyncpg.Pool = data["pool"]
    raw = msg.text.strip()
    if not _is_valid_phone(raw):
        await msg.answer(
            "Формат номера: 9XXXXXXXXX, 8XXXXXXXXXX или +7XXXXXXXXXX",
            reply_markup=cancel_kb,
        )
        return
    phone_norm = normalize_phone(raw)
    async with pool.acquire() as conn:
        client = await find_client_by_phone(conn, raw)
    payload = {"phone_norm": phone_norm}
    if client:
        payload["client_id"] = client["id"]
        payload["client_name"] = client["full_name"] or ""
        payload["client_phone"] = client["phone"] or phone_norm
        payload["client_address"] = client["address"] or ""
        payload["bonus_balance"] = int(client["bonus_balance"] or 0)
        await state.update_data(**payload)
        if payload["client_address"]:
            await state.set_state(CleaningOrderFSM.address_choice)
            await msg.answer(
                f"Клиент: {client['full_name'] or 'Без имени'}\n"
                f"Бонусов: {payload['bonus_balance']}\n"
                f"Адрес из базы: {payload['client_address']}",
                reply_markup=_address_choice_kb(),
            )
            return
        await state.set_state(CleaningOrderFSM.address)
        await msg.answer(
            f"Клиент: {client['full_name'] or 'Без имени'}\n"
            f"Бонусов: {payload['bonus_balance']}\n"
            "Введите адрес уборки:",
            reply_markup=cancel_kb,
        )
    else:
        payload["client_id"] = None
        payload["client_phone"] = phone_norm
        payload["client_address"] = ""
        payload["bonus_balance"] = 0
        await state.update_data(**payload)
        await state.set_state(CleaningOrderFSM.name)
        await msg.answer("Клиент не найден. Введите имя клиента:", reply_markup=cancel_kb)


@router.message(CleaningOrderFSM.name, F.text)
async def got_name(msg: Message, state: FSMContext) -> None:
    name = msg.text.strip()
    if not name:
        await msg.answer("Имя не может быть пустым.", reply_markup=cancel_kb)
        return
    await state.update_data(client_name=name)
    await state.set_state(CleaningOrderFSM.address)
    await msg.answer("Введите адрес уборки:", reply_markup=cancel_kb)


@router.message(CleaningOrderFSM.address_choice, F.text)
async def got_address_choice(msg: Message, state: FSMContext) -> None:
    choice = msg.text.strip()
    data = await state.get_data()
    if choice == "Использовать этот адрес":
        address = (data.get("client_address") or "").strip()
        if not address:
            await state.set_state(CleaningOrderFSM.address)
            await msg.answer("Введите адрес уборки:", reply_markup=cancel_kb)
            return
        await state.update_data(address=address, address_from_client=True)
        await state.set_state(CleaningOrderFSM.amount)
        await msg.answer("Введите сумму чека (руб):", reply_markup=cancel_kb)
        return
    if choice == "Другой адрес":
        await state.set_state(CleaningOrderFSM.address)
        await msg.answer("Введите адрес уборки:", reply_markup=cancel_kb)
        return
    await msg.answer("Выберите вариант кнопкой.", reply_markup=_address_choice_kb())


@router.message(CleaningOrderFSM.address, F.text)
async def got_address(msg: Message, state: FSMContext) -> None:
    address = msg.text.strip()
    if not address:
        await msg.answer("Адрес не может быть пустым.", reply_markup=cancel_kb)
        return
    await state.update_data(address=address, address_from_client=False)
    await state.set_state(CleaningOrderFSM.amount)
    await msg.answer("Введите сумму чека (руб):", reply_markup=cancel_kb)


@router.message(CleaningOrderFSM.amount, F.text)
async def got_amount(msg: Message, state: FSMContext) -> None:
    amount = parse_amount(msg.text)
    if amount is None or amount <= 0:
        await msg.answer("Нужно число > 0. Повторите.", reply_markup=cancel_kb)
        return
    min_amount = get_min_order_amount()
    if amount < min_amount:
        await msg.answer(
            f"Минимальный чек уборки: {_money_str(min_amount)}₽. Повторите.",
            reply_markup=cancel_kb,
        )
        return
    await state.update_data(total_amount=str(amount))
    await state.set_state(CleaningOrderFSM.pay_method)
    await msg.answer("Выберите способ оплаты:", reply_markup=_pay_method_kb(allow_wire=True))


@router.message(CleaningOrderFSM.bonus_spend, F.text)
async def got_bonus_spend(msg: Message, state: FSMContext) -> None:
    data = await state.get_data()
    text = msg.text.strip()
    if text == "Не списывать":
        spend_value = Decimal("0")
    elif text.lower().startswith("списать"):
        spend_value = Decimal(data.get("bonus_max") or 0)
    else:
        parsed = parse_amount(text)
        if parsed is None:
            await msg.answer("Нужно число ≥ 0.", reply_markup=cancel_kb)
            return
        spend_value = parsed
    max_spend = Decimal(data.get("bonus_max") or 0)
    if spend_value > max_spend:
        await msg.answer(f"Максимум к списанию: {_money_str(max_spend)}.", reply_markup=cancel_kb)
        return
    method = data.get("pending_pay_method")
    await state.update_data(bonus_spend=int(spend_value), bonus_step_done=True)
    await state.set_state(CleaningOrderFSM.pay_amount)
    await msg.answer(f"Сумма по «{method}»:", reply_markup=cancel_kb)


@router.message(CleaningOrderFSM.pay_method, F.text)
async def got_pay_method(msg: Message, state: FSMContext) -> None:
    method = msg.text.strip()
    data = await state.get_data()
    payments = list(data.get("payments") or [])
    allow_wire = not payments and not data.get("bonus_step_done")
    if method not in CLEANING_ALL_PAYMENT_LABELS:
        await msg.answer("Выберите метод кнопкой.", reply_markup=_pay_method_kb(allow_wire=allow_wire))
        return
    if is_wire_payment_method(method):
        if not allow_wire:
            await msg.answer(
                "Расчётный нельзя смешивать с другими способами оплаты.",
                reply_markup=_pay_method_kb(allow_wire=False),
            )
            return
        total = Decimal(data["total_amount"])
        await state.update_data(
            payments=[{"method": method, "amount": str(total)}],
            bonus_spend=0,
            bonus_earned=0,
            is_wire_payment=True,
            bonus_step_done=True,
        )
        await state.set_state(CleaningOrderFSM.expense_category)
        await msg.answer(
            "Оплата по расчётному счёту принята. Бонусы не списываем и не начисляем.\n"
            "Теперь расходы по заказу. Выберите категорию или «Готово»:",
            reply_markup=_expense_category_kb(),
        )
        return
    await state.update_data(pending_pay_method=method)
    if not data.get("bonus_step_done"):
        total = Decimal(data["total_amount"])
        balance = int(data.get("bonus_balance") or 0)
        bonus_max = calculate_bonus_max(total, balance)
        if balance > 0 and bonus_max > 0:
            await state.update_data(bonus_max=str(bonus_max), payments=[])
            await state.set_state(CleaningOrderFSM.bonus_spend)
            kb = ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text=f"Списать {int(bonus_max)} бонусов")],
                    [KeyboardButton(text="Не списывать")],
                    [KeyboardButton(text="Отмена")],
                ],
                resize_keyboard=True,
            )
            await msg.answer(
                f"Бонусов у клиента: {balance}. Максимум к списанию: {int(bonus_max)}.\n"
                "Выберите списание:",
                reply_markup=kb,
            )
            return
        await state.update_data(bonus_spend=0, bonus_step_done=True, payments=[])
    await state.set_state(CleaningOrderFSM.pay_amount)
    await msg.answer(f"Сумма по «{method}»:", reply_markup=cancel_kb)


@router.message(CleaningOrderFSM.pay_amount, F.text)
async def got_pay_amount(msg: Message, state: FSMContext) -> None:
    amount = parse_amount(msg.text)
    if amount is None or amount <= 0:
        await msg.answer("Нужно число > 0.", reply_markup=cancel_kb)
        return
    data = await state.get_data()
    payments = list(data.get("payments") or [])
    method = data.get("pending_pay_method")
    payments.append({"method": method, "amount": str(amount)})
    await state.update_data(payments=payments, pending_pay_method=None)
    parts = [PaymentPart(p["method"], Decimal(p["amount"])) for p in payments]
    total = Decimal(data["total_amount"])
    used = Decimal(int(data.get("bonus_spend") or 0))
    ok, error = validate_payment_parts(parts, total, used)
    diff = sum((p.amount for p in parts), ZERO) - (total - used)
    if ok:
        await state.set_state(CleaningOrderFSM.expense_category)
        await msg.answer(
            "Оплата сошлась. Теперь расходы по заказу.\nВыберите категорию или «Готово»:",
            reply_markup=_expense_category_kb(),
        )
        return
    if diff > 0:
        await msg.answer(
            f"{error} Удалите лишнюю часть или начните заново /cleaning_order.",
            reply_markup=cancel_kb,
        )
        return
    remaining = -diff
    await state.set_state(CleaningOrderFSM.pay_method)
    await msg.answer(
        f"Принято. Осталось: {_money_str(remaining)}₽.\nВыберите следующий метод:",
        reply_markup=_pay_method_kb(allow_wire=False),
    )


@router.message(CleaningOrderFSM.expense_category, F.text)
async def got_expense_category(msg: Message, state: FSMContext) -> None:
    text = msg.text.strip()
    if text == "Готово":
        await _show_confirm(msg, state)
        return
    if text not in CLEANING_EXPENSE_CATEGORIES:
        await msg.answer("Выберите категорию кнопкой или «Готово».", reply_markup=_expense_category_kb())
        return
    await state.update_data(pending_expense=text)
    await state.set_state(CleaningOrderFSM.expense_amount)
    await msg.answer(f"Сумма расхода по «{text}»:", reply_markup=cancel_kb)


@router.message(CleaningOrderFSM.expense_amount, F.text)
async def got_expense_amount(msg: Message, state: FSMContext) -> None:
    amount = parse_amount(msg.text)
    if amount is None or amount <= 0:
        await msg.answer("Нужно число > 0.", reply_markup=cancel_kb)
        return
    data = await state.get_data()
    expenses = list(data.get("expenses") or [])
    category = data.get("pending_expense")
    expenses.append({"category": category, "amount": str(amount)})
    await state.update_data(expenses=expenses, pending_expense=None)
    await state.set_state(CleaningOrderFSM.expense_category)
    await msg.answer(
        f"Добавлено: {category} {_money_str(amount)}₽.\nЕщё расход? Или «Готово».",
        reply_markup=_expense_category_kb(),
    )


async def _show_confirm(msg: Message, state: FSMContext) -> None:
    data = await state.get_data()
    total = Decimal(data["total_amount"])
    used = Decimal(int(data.get("bonus_spend") or 0))
    payments = data.get("payments") or []
    expenses = data.get("expenses") or []
    primary_method = payments[0]["method"] if payments else ""
    earned = calculate_bonus_earned(primary_method, total - used)
    lines = [
        "Подтвердите проведение уборки:",
        f"Клиент: {data.get('client_name') or '—'} ({data.get('phone_norm')})",
        f"Адрес: {data.get('address')}",
        f"Сумма чека: {_money_str(total)}₽",
        f"Бонусы: списано {int(used)}, начислено {earned}",
        "Оплата:",
    ]
    for p in payments:
        lines.append(f"  • {p['method']}: {_money_str(Decimal(p['amount']))}₽")
    if expenses:
        lines.append("Расходы:")
        for e in expenses:
            lines.append(f"  • {e['category']}: {_money_str(Decimal(e['amount']))}₽")
    else:
        lines.append("Расходов нет.")
    await state.set_state(CleaningOrderFSM.confirm)
    await msg.answer("\n".join(lines), reply_markup=_confirm_kb())


@router.message(CleaningOrderFSM.confirm, F.text == "Провести")
async def do_provesti(msg: Message, state: FSMContext, **kw) -> None:
    pool: asyncpg.Pool = kw["pool"]
    bot = kw["bot"]
    notification_rules: NotificationRules | None = kw.get("notification_rules")
    data = await state.get_data()
    foreman_tg = msg.from_user.id

    async with pool.acquire() as conn:
        foreman = await conn.fetchrow(
            "SELECT id, fn, ln FROM cleaning_foremen WHERE tg_user_id=$1 AND is_active",
            foreman_tg,
        )
        if not foreman:
            await msg.answer("Доступ отозван.")
            await state.clear()
            return

        async with conn.transaction():
            # клиент (upsert)
            if data.get("client_id"):
                client_row = await conn.fetchrow(
                    "SELECT id, full_name, phone, address, bonus_balance FROM clients WHERE id=$1",
                    data["client_id"],
                )
                if client_row is None:
                    await msg.answer("Клиент исчез из БД, отмените и повторите.")
                    return
            else:
                client_row = await upsert_client(
                    conn,
                    full_name=data.get("client_name") or "Клиент",
                    phone_norm=data["phone_norm"],
                )

            client_id = client_row["id"]
            if not (client_row["address"] or "").strip() and data.get("address"):
                await conn.execute(
                    "UPDATE clients SET address=$1 WHERE id=$2 AND (address IS NULL OR address = '')",
                    data["address"],
                    client_id,
                )
            total = Decimal(data["total_amount"])
            bonus_spend = int(data.get("bonus_spend") or 0)
            payments = data.get("payments") or []
            primary_method = payments[0]["method"] if payments else ""
            bonus_earned = calculate_bonus_earned(
                primary_method,
                total - Decimal(bonus_spend),
            )

            client_op_id = data.get("client_op_id")
            order_row = await conn.fetchrow(
                """
                INSERT INTO cleaning_orders
                    (client_id, foreman_id, address, total_amount,
                     bonuses_used, bonuses_earned, client_op_id)
                VALUES ($1,$2,$3,$4,$5,$6,$7)
                ON CONFLICT (client_op_id) DO NOTHING
                RETURNING id
                """,
                client_id,
                foreman["id"],
                data["address"],
                total,
                bonus_spend,
                bonus_earned,
                client_op_id,
            )
            if order_row is None:
                # уже было проведено по этому client_op_id — выходим тихо
                await msg.answer("Заказ уже проведён ранее.", reply_markup=ReplyKeyboardRemove())
                await state.clear()
                return
            order_id = order_row["id"]

            for p in payments:
                method = p["method"]
                amount = Decimal(p["amount"])
                await conn.execute(
                    """
                    INSERT INTO cleaning_order_payments (order_id, method, amount)
                    VALUES ($1,$2,$3)
                    """,
                    order_id,
                    method,
                    amount,
                )
            parts = [PaymentPart(p["method"], Decimal(p["amount"])) for p in payments]
            for method, amount in cashbook_rows_from_payments(parts):
                await record_income(
                    conn,
                    method=method,
                    amount=amount,
                    order_id=order_id,
                    comment=f"Заказ #{order_id}",
                )

            expenses = data.get("expenses") or []
            for e in expenses:
                await record_expense(
                    conn,
                    category=e["category"],
                    amount=Decimal(e["amount"]),
                    order_id=order_id,
                    comment=e["category"],
                )

            # Бонусы клиента — обновляем общий баланс и пишем в bonus_transactions
            now_utc = datetime.now(timezone.utc)
            bonus_expires_at = (datetime.now(MOSCOW_TZ) + timedelta(days=365)).astimezone(timezone.utc)
            if bonus_spend > 0:
                await conn.execute(
                    """
                    INSERT INTO bonus_transactions
                        (client_id, delta, reason, created_at, happened_at, meta)
                    VALUES ($1, $2, 'spend', $3, $3,
                            jsonb_build_object('source','cleaning','order_id',$4::int))
                    """,
                    client_id,
                    -bonus_spend,
                    now_utc,
                    order_id,
                )
            if bonus_earned > 0:
                await conn.execute(
                    """
                    INSERT INTO bonus_transactions
                        (client_id, delta, reason, created_at, happened_at, expires_at, meta)
                    VALUES ($1, $2, 'accrual', $3, $3, $5,
                            jsonb_build_object('source','cleaning','order_id',$4::int))
                    """,
                    client_id,
                    bonus_earned,
                    now_utc,
                    order_id,
                    bonus_expires_at,
                )
            new_balance = int(client_row["bonus_balance"] or 0) - bonus_spend + bonus_earned
            await conn.execute(
                "UPDATE clients SET bonus_balance=$1 WHERE id=$2",
                new_balance,
                client_id,
            )
            await _enqueue_cleaning_completed_notifications(
                conn,
                notification_rules,
                order_id=order_id,
                client_id=client_id,
                total=total,
                bonuses_used=bonus_spend,
                bonuses_earned=bonus_earned,
                bonus_balance=new_balance,
                amount_due=total - Decimal(bonus_spend),
                bonus_expires_at=bonus_expires_at if bonus_earned > 0 else None,
                is_wire_payment=bool(data.get("is_wire_payment")),
            )

            balance_after = await get_cleaning_balance(conn)
            income_sum = sum(
                (a for _, a in cashbook_rows_from_payments(parts)), ZERO
            )
            expense_sum = sum(
                (Decimal(e["amount"]) for e in expenses), ZERO
            )
            profit = income_sum - expense_sum

    # после COMMIT — оповещение в чат
    foreman_name = (foreman["fn"] or "") + (" " + foreman["ln"] if foreman["ln"] else "")
    text = format_order_provided_alert(
        order_id=order_id,
        foreman_name=foreman_name.strip() or "Бригадир",
        client_phone=data["phone_norm"],
        client_name=client_row["full_name"] or data.get("client_name") or "Клиент",
        address=data["address"],
        total_amount=total,
        payments=[(p["method"], Decimal(p["amount"])) for p in payments],
        expenses=[(e["category"], Decimal(e["amount"])) for e in expenses],
        bonuses_used=Decimal(bonus_spend),
        bonuses_earned=Decimal(bonus_earned),
        profit=profit,
        balance_after=balance_after,
    )
    await send_cleaning_money_flow(bot, text)
    await msg.answer(
        f"Заказ #{order_id} проведён. Касса клининга: {_money_str(balance_after)}₽.",
        reply_markup=cleaning_main_kb(),
    )
    await state.clear()


# ---------- /cleaning_balance ----------


@router.message(Command("cleaning_balance"))
@router.message(F.text.in_({"💰 Баланс", "💰 Баланс кассы клининга"}))
async def cleaning_balance_cmd(msg: Message, **kw) -> None:
    pool: asyncpg.Pool = kw["pool"]
    if not await _has_permission(pool, msg.from_user.id, "cleaning_view_balance"):
        await msg.answer("Команда доступна только клинерам и администраторам.")
        return
    async with pool.acquire() as conn:
        balance = await get_cleaning_balance(conn)
    await msg.answer(f"Касса клининга: {_money_str(balance)}₽")


# ---------- cleaner client lookup ----------


@router.message(F.text == "🔍 Клиент")
async def cleaning_client_lookup_start(msg: Message, state: FSMContext, **kw) -> None:
    pool: asyncpg.Pool = kw["pool"]
    if not await _has_permission(pool, msg.from_user.id, "cleaning_view_clients"):
        await msg.answer("Команда доступна только клинерам и администраторам.")
        return
    await state.clear()
    await state.set_state(CleaningClientLookupFSM.phone)
    await msg.answer("Введите номер телефона клиента:", reply_markup=cancel_kb)


@router.message(CleaningClientLookupFSM.phone, F.text)
async def cleaning_client_lookup_phone(msg: Message, state: FSMContext, **kw) -> None:
    pool: asyncpg.Pool = kw["pool"]
    raw = msg.text.strip()
    if not _is_valid_phone(raw):
        await msg.answer(
            "Формат номера: 9XXXXXXXXX, 8XXXXXXXXXX или +7XXXXXXXXXX",
            reply_markup=cancel_kb,
        )
        return
    async with pool.acquire() as conn:
        rec = await find_client_by_phone(conn, raw)
    await state.clear()
    if rec is None:
        await msg.answer("Не найдено.", reply_markup=cleaning_main_kb())
        return
    birthday = rec["birthday"].isoformat() if rec["birthday"] else "—"
    status = rec["status"] or "—"
    text = (
        f"👤 {rec['full_name'] or 'Без имени'}\n"
        f"📞 {rec['phone']}\n"
        f"💳 {rec['bonus_balance']}\n"
        f"🎂 {birthday}\n"
        f"🏷️ {status}"
    )
    await msg.answer(text, reply_markup=cleaning_main_kb())


# ---------- cleaner manual expense ----------


@router.message(Command("cleaning_expense"))
@router.message(F.text == "➖ Добавить расход")
async def foreman_expense_start(msg: Message, state: FSMContext, **kw) -> None:
    pool: asyncpg.Pool = kw["pool"]
    if not await _has_permission(pool, msg.from_user.id, "cleaning_record_expense"):
        await msg.answer("Команда доступна только клинерам и администраторам.")
        return
    await state.clear()
    await state.set_state(CleaningForemanExpenseFSM.amount)
    await msg.answer("Введите сумму расхода:", reply_markup=cancel_kb)


@router.message(CleaningForemanExpenseFSM.amount, F.text)
async def foreman_expense_amount(msg: Message, state: FSMContext) -> None:
    amount = parse_amount(msg.text)
    if amount is None or amount <= 0:
        await msg.answer("Нужно число > 0.", reply_markup=cancel_kb)
        return
    await state.update_data(amount=str(amount))
    await state.set_state(CleaningForemanExpenseFSM.category)
    await msg.answer("Выберите категорию расхода:", reply_markup=_expense_category_kb())


@router.message(CleaningForemanExpenseFSM.category, F.text)
async def foreman_expense_category(msg: Message, state: FSMContext) -> None:
    category = msg.text.strip()
    if category == "Готово":
        await msg.answer("Категория обязательна.", reply_markup=_expense_category_kb())
        return
    if category not in CLEANING_EXPENSE_CATEGORIES:
        await msg.answer("Выберите категорию кнопкой.", reply_markup=_expense_category_kb())
        return
    await state.update_data(category=category)
    await state.set_state(CleaningForemanExpenseFSM.comment)
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Без комментария")], [KeyboardButton(text="Отмена")]],
        resize_keyboard=True,
    )
    await msg.answer("Комментарий? (введите текст или нажмите «Без комментария»)", reply_markup=kb)


@router.message(CleaningForemanExpenseFSM.comment, F.text)
async def foreman_expense_comment(msg: Message, state: FSMContext) -> None:
    comment = msg.text.strip()
    if comment.casefold() == "без комментария" or not comment:
        comment = "Расход"
    await state.update_data(comment=comment)
    data = await state.get_data()
    amount = Decimal(data["amount"])
    await state.set_state(CleaningForemanExpenseFSM.confirm)
    await msg.answer(
        "Подтвердите расход:\n"
        f"Категория: {data['category']}\n"
        f"Сумма: {_money_str(amount)}₽\n"
        f"Комментарий: {comment}",
        reply_markup=_confirm_kb(),
    )


@router.message(CleaningForemanExpenseFSM.confirm, F.text == "Провести")
async def foreman_expense_confirm(msg: Message, state: FSMContext, **kw) -> None:
    pool: asyncpg.Pool = kw["pool"]
    bot = kw["bot"]
    data = await state.get_data()
    amount = Decimal(data["amount"])
    category = data["category"]
    comment = data.get("comment") or "Расход"
    async with pool.acquire() as conn:
        async with conn.transaction():
            await add_cash_expense(conn, category=category, amount=amount, comment=comment)
            balance_after = await get_cleaning_balance(conn)
    await send_cleaning_money_flow(
        bot,
        format_cash_op_alert(
            op_label="Расход",
            bucket=category,
            amount=amount,
            comment=comment,
            balance_after=balance_after,
        ),
    )
    await state.clear()
    await msg.answer(
        f"Расход списан. Касса: {_money_str(balance_after)}₽",
        reply_markup=cleaning_main_kb(),
    )


# ---------- /cleaning_dividend ----------


@router.message(Command("cleaning_dividend"))
async def start_cleaning_dividend(msg: Message, state: FSMContext, **kw) -> None:
    pool: asyncpg.Pool = kw["pool"]
    if not await _has_permission(pool, msg.from_user.id, "cleaning_manage_cash"):
        await msg.answer("Команда доступна только администраторам.")
        return
    await state.clear()
    await state.set_state(CleaningDividendFSM.amount)
    await msg.answer("DIV-выплата клининга.\nВведите сумму (руб):", reply_markup=cancel_kb)


@router.message(CleaningDividendFSM.amount, F.text)
async def div_amount(msg: Message, state: FSMContext) -> None:
    amount = parse_amount(msg.text)
    if amount is None or amount <= 0:
        await msg.answer("Нужно число > 0.", reply_markup=cancel_kb)
        return
    await state.update_data(amount=str(amount))
    await state.set_state(CleaningDividendFSM.comment)
    await msg.answer("Кому выплачено (комментарий, обязательно):", reply_markup=cancel_kb)


@router.message(CleaningDividendFSM.comment, F.text)
async def div_comment(msg: Message, state: FSMContext) -> None:
    comment = msg.text.strip()
    if not comment:
        await msg.answer("Комментарий обязателен.", reply_markup=cancel_kb)
        return
    await state.update_data(comment=comment)
    data = await state.get_data()
    await state.set_state(CleaningDividendFSM.confirm)
    await msg.answer(
        f"Подтвердить выплату {_money_str(Decimal(data['amount']))}₽ — {comment}?",
        reply_markup=_confirm_kb(),
    )


@router.message(CleaningDividendFSM.confirm, F.text == "Провести")
async def div_provesti(msg: Message, state: FSMContext, **kw) -> None:
    pool: asyncpg.Pool = kw["pool"]
    bot = kw["bot"]
    data = await state.get_data()
    amount = Decimal(data["amount"])
    comment = data["comment"]
    async with pool.acquire() as conn:
        async with conn.transaction():
            await record_dividend(conn, amount=amount, comment=comment)
            balance_after = await get_cleaning_balance(conn)
    text = format_dividend_alert(amount=amount, recipient=comment, balance_after=balance_after)
    await send_cleaning_money_flow(bot, text)
    await msg.answer(
        f"DIV проведён. Касса клининга: {_money_str(balance_after)}₽",
        reply_markup=ReplyKeyboardRemove(),
    )
    await state.clear()


# ---------- /cleaning_cash_add (manual income / deposit) ----------


@router.message(Command("cleaning_cash_add"))
async def start_cash_add(msg: Message, state: FSMContext, **kw) -> None:
    pool: asyncpg.Pool = kw["pool"]
    if not await _has_permission(pool, msg.from_user.id, "cleaning_manage_cash"):
        await msg.answer("Команда доступна только администраторам.")
        return
    await state.clear()
    await state.set_state(CleaningCashAddFSM.method)
    await msg.answer(
        "Ручной приход в кассу клининга.\nВыберите метод:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text=m) for m in CLEANING_PAYMENT_METHODS],
                [KeyboardButton(text="Отмена")],
            ],
            resize_keyboard=True,
        ),
    )


@router.message(CleaningCashAddFSM.method, F.text)
async def cash_add_method(msg: Message, state: FSMContext) -> None:
    if msg.text not in CLEANING_PAYMENT_METHODS:
        await msg.answer("Выберите метод кнопкой.", reply_markup=cancel_kb)
        return
    await state.update_data(method=msg.text)
    await state.set_state(CleaningCashAddFSM.amount)
    await msg.answer("Сумма (руб):", reply_markup=cancel_kb)


@router.message(CleaningCashAddFSM.amount, F.text)
async def cash_add_amount(msg: Message, state: FSMContext) -> None:
    amount = parse_amount(msg.text)
    if amount is None or amount <= 0:
        await msg.answer("Нужно число > 0.", reply_markup=cancel_kb)
        return
    await state.update_data(amount=str(amount))
    await state.set_state(CleaningCashAddFSM.comment)
    await msg.answer("Комментарий (или «-»):", reply_markup=cancel_kb)


@router.message(CleaningCashAddFSM.comment, F.text)
async def cash_add_comment(msg: Message, state: FSMContext) -> None:
    comment = msg.text.strip()
    if comment == "-":
        comment = ""
    await state.update_data(comment=comment)
    data = await state.get_data()
    await state.set_state(CleaningCashAddFSM.confirm)
    await msg.answer(
        f"Подтвердите приход: {data['method']} {_money_str(Decimal(data['amount']))}₽"
        + (f"\nКомментарий: {comment}" if comment else ""),
        reply_markup=_confirm_kb(),
    )


@router.message(CleaningCashAddFSM.confirm, F.text == "Провести")
async def cash_add_provesti(msg: Message, state: FSMContext, **kw) -> None:
    pool: asyncpg.Pool = kw["pool"]
    bot = kw["bot"]
    data = await state.get_data()
    amount = Decimal(data["amount"])
    method = data["method"]
    comment = data.get("comment") or None
    async with pool.acquire() as conn:
        async with conn.transaction():
            await add_cash_income(conn, method=method, amount=amount, comment=comment)
            balance_after = await get_cleaning_balance(conn)
    await send_cleaning_money_flow(
        bot,
        format_cash_op_alert(
            op_label="Приход",
            bucket=method,
            amount=amount,
            comment=comment,
            balance_after=balance_after,
        ),
    )
    await msg.answer(
        f"Приход зачислен. Касса: {_money_str(balance_after)}₽",
        reply_markup=ReplyKeyboardRemove(),
    )
    await state.clear()


# ---------- /cleaning_cash_expense (manual expense) ----------


@router.message(Command("cleaning_cash_expense"))
async def start_cash_expense(msg: Message, state: FSMContext, **kw) -> None:
    pool: asyncpg.Pool = kw["pool"]
    if not await _has_permission(pool, msg.from_user.id, "cleaning_manage_cash"):
        await msg.answer("Команда доступна только администраторам.")
        return
    await state.clear()
    await state.set_state(CleaningCashExpenseFSM.category)
    await msg.answer(
        "Ручной расход. Выберите категорию:",
        reply_markup=_expense_category_kb(),
    )


@router.message(CleaningCashExpenseFSM.category, F.text)
async def cash_exp_category(msg: Message, state: FSMContext) -> None:
    if msg.text == "Готово":
        await msg.answer("Категория обязательна.", reply_markup=_expense_category_kb())
        return
    if msg.text not in CLEANING_EXPENSE_CATEGORIES:
        await msg.answer("Выберите категорию кнопкой.", reply_markup=_expense_category_kb())
        return
    await state.update_data(category=msg.text)
    await state.set_state(CleaningCashExpenseFSM.amount)
    await msg.answer("Сумма (руб):", reply_markup=cancel_kb)


@router.message(CleaningCashExpenseFSM.amount, F.text)
async def cash_exp_amount(msg: Message, state: FSMContext) -> None:
    amount = parse_amount(msg.text)
    if amount is None or amount <= 0:
        await msg.answer("Нужно число > 0.", reply_markup=cancel_kb)
        return
    await state.update_data(amount=str(amount))
    await state.set_state(CleaningCashExpenseFSM.comment)
    await msg.answer("Комментарий (или «-»):", reply_markup=cancel_kb)


@router.message(CleaningCashExpenseFSM.comment, F.text)
async def cash_exp_comment(msg: Message, state: FSMContext) -> None:
    comment = msg.text.strip()
    if comment == "-":
        comment = ""
    await state.update_data(comment=comment)
    data = await state.get_data()
    await state.set_state(CleaningCashExpenseFSM.confirm)
    await msg.answer(
        f"Подтвердите расход: {data['category']} {_money_str(Decimal(data['amount']))}₽"
        + (f"\nКомментарий: {comment}" if comment else ""),
        reply_markup=_confirm_kb(),
    )


@router.message(CleaningCashExpenseFSM.confirm, F.text == "Провести")
async def cash_exp_provesti(msg: Message, state: FSMContext, **kw) -> None:
    pool: asyncpg.Pool = kw["pool"]
    bot = kw["bot"]
    data = await state.get_data()
    amount = Decimal(data["amount"])
    category = data["category"]
    comment = data.get("comment") or None
    async with pool.acquire() as conn:
        async with conn.transaction():
            await add_cash_expense(conn, category=category, amount=amount, comment=comment)
            balance_after = await get_cleaning_balance(conn)
    await send_cleaning_money_flow(
        bot,
        format_cash_op_alert(
            op_label="Расход",
            bucket=category,
            amount=amount,
            comment=comment,
            balance_after=balance_after,
        ),
    )
    await msg.answer(
        f"Расход списан. Касса: {_money_str(balance_after)}₽",
        reply_markup=ReplyKeyboardRemove(),
    )
    await state.clear()


# ---------- /cleaning_cash_withdrawal ----------


@router.message(Command("cleaning_cash_withdrawal"))
async def start_cash_withdrawal(msg: Message, state: FSMContext, **kw) -> None:
    pool: asyncpg.Pool = kw["pool"]
    if not await _has_permission(pool, msg.from_user.id, "cleaning_manage_cash"):
        await msg.answer("Команда доступна только администраторам.")
        return
    await state.clear()
    await state.set_state(CleaningCashWithdrawalFSM.amount)
    await msg.answer(
        "Изъятие из кассы клининга (не считается расходом в P&L).\nСумма (руб):",
        reply_markup=cancel_kb,
    )


@router.message(CleaningCashWithdrawalFSM.amount, F.text)
async def cash_wd_amount(msg: Message, state: FSMContext) -> None:
    amount = parse_amount(msg.text)
    if amount is None or amount <= 0:
        await msg.answer("Нужно число > 0.", reply_markup=cancel_kb)
        return
    await state.update_data(amount=str(amount))
    await state.set_state(CleaningCashWithdrawalFSM.comment)
    await msg.answer("Комментарий (или «-»):", reply_markup=cancel_kb)


@router.message(CleaningCashWithdrawalFSM.comment, F.text)
async def cash_wd_comment(msg: Message, state: FSMContext) -> None:
    comment = msg.text.strip()
    if comment == "-":
        comment = ""
    await state.update_data(comment=comment)
    data = await state.get_data()
    await state.set_state(CleaningCashWithdrawalFSM.confirm)
    await msg.answer(
        f"Подтвердите изъятие: {_money_str(Decimal(data['amount']))}₽"
        + (f"\nКомментарий: {comment}" if comment else ""),
        reply_markup=_confirm_kb(),
    )


@router.message(CleaningCashWithdrawalFSM.confirm, F.text == "Провести")
async def cash_wd_provesti(msg: Message, state: FSMContext, **kw) -> None:
    pool: asyncpg.Pool = kw["pool"]
    bot = kw["bot"]
    data = await state.get_data()
    amount = Decimal(data["amount"])
    comment = data.get("comment") or None
    async with pool.acquire() as conn:
        async with conn.transaction():
            await add_cash_withdrawal(conn, amount=amount, comment=comment)
            balance_after = await get_cleaning_balance(conn)
    await send_cleaning_money_flow(
        bot,
        format_cash_op_alert(
            op_label="Изъятие",
            bucket="Касса клининга",
            amount=amount,
            comment=comment,
            balance_after=balance_after,
        ),
    )
    await msg.answer(
        f"Изъятие проведено. Касса: {_money_str(balance_after)}₽",
        reply_markup=ReplyKeyboardRemove(),
    )
    await state.clear()


# ---------- /cleaning_cancel_order N ----------


@router.message(Command("cleaning_cancel_order"))
async def start_cancel_order(
    msg: Message, state: FSMContext, command: CommandObject = None, **kw
) -> None:
    pool: asyncpg.Pool = kw["pool"]
    if not await _has_permission(pool, msg.from_user.id, "cleaning_cancel_orders"):
        await msg.answer("Команда доступна только администраторам.")
        return
    arg = (command.args if command else "") or ""
    arg = arg.strip()
    if not arg.isdigit():
        await msg.answer("Использование: /cleaning_cancel_order N (N — id заказа уборки).")
        return
    order_id = int(arg)
    await state.clear()
    await state.update_data(cancel_order_id=order_id)
    await state.set_state(CleaningCancelOrderFSM.confirm)
    await msg.answer(
        f"Отменить заказ уборки #{order_id}?\n"
        "Будут soft-deleted все кассовые строки заказа, бонусы клиента откатятся.",
        reply_markup=_confirm_kb(),
    )


@router.message(CleaningCancelOrderFSM.confirm, F.text == "Провести")
async def cancel_order_confirmed(msg: Message, state: FSMContext, **kw) -> None:
    pool: asyncpg.Pool = kw["pool"]
    bot = kw["bot"]
    data = await state.get_data()
    order_id = int(data["cancel_order_id"])
    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await cancel_order(conn, order_id=order_id)
            balance_after = await get_cleaning_balance(conn) if result else None
    if result is None:
        await msg.answer(
            f"Заказ #{order_id} не найден или уже отменён.",
            reply_markup=ReplyKeyboardRemove(),
        )
    else:
        await send_cleaning_money_flow(
            bot,
            format_cancel_order_alert(
                order_id=result["order_id"],
                address=result["address"],
                total_amount=result["total_amount"],
                bonuses_used=result["bonuses_used"],
                bonuses_earned=result["bonuses_earned"],
                cashbook_rows_deleted=result["cashbook_rows_deleted"],
                balance_after=balance_after,
            ),
        )
        await msg.answer(
            f"Заказ #{order_id} отменён. Касса: {_money_str(balance_after)}₽",
            reply_markup=ReplyKeyboardRemove(),
        )
    await state.clear()


# ---------- /cleaning_cash day|month|year ----------


@router.message(Command("cleaning_cash"))
async def cleaning_cash_report(
    msg: Message, command: CommandObject = None, **kw
) -> None:
    pool: asyncpg.Pool = kw["pool"]
    if not await _has_permission(pool, msg.from_user.id, "cleaning_view_reports"):
        await msg.answer("Команда доступна только администраторам.")
        return
    arg = ((command.args if command else "") or "").strip().lower() or "day"
    if arg not in {"day", "month", "year"}:
        await msg.answer("Использование: /cleaning_cash day|month|year")
        return
    start_utc, end_utc, label = _period_bounds(arg)
    async with pool.acquire() as conn:
        report = await get_cleaning_cash_report(conn, start_utc, end_utc)
        balance_after = await get_cleaning_balance(conn)
    await msg.answer(
        format_cash_report(label=label, report=report, balance_after=balance_after)
    )


# ---------- /cleaning_orders day|month ----------


@router.message(Command("cleaning_orders"))
async def cleaning_orders_list(
    msg: Message, command: CommandObject = None, **kw
) -> None:
    pool: asyncpg.Pool = kw["pool"]
    if not await _has_permission(pool, msg.from_user.id, "cleaning_view_reports"):
        await msg.answer("Команда доступна только администраторам.")
        return
    arg = ((command.args if command else "") or "").strip().lower() or "day"
    if arg not in {"day", "month"}:
        await msg.answer("Использование: /cleaning_orders day|month")
        return
    start_utc, end_utc, label = _period_bounds(arg)
    async with pool.acquire() as conn:
        orders = await get_cleaning_orders_list(conn, start_utc, end_utc)
    await msg.answer(format_orders_list(label=label, orders=orders))
