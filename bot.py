import asyncio, os, re, logging
import csv, io
from decimal import Decimal, ROUND_DOWN
from datetime import date, datetime, timezone
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message,
    CallbackQuery,
    BotCommand,
    BotCommandScopeDefault,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.filters import CommandStart, Command, CommandObject, StateFilter
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext

# ===== FSM State Groups =====
class AdminMenuFSM(StatesGroup):
    root    = State()
    masters = State()
    clients = State()


class AdminClientsFSM(StatesGroup):
    find_wait_phone = State()
    edit_wait_phone = State()
    edit_pick_field = State()
    edit_wait_value = State()


class AdminMastersFSM(StatesGroup):
    remove_wait_phone = State()


class IncomeFSM(StatesGroup):
    waiting_method = State()
    waiting_amount = State()
    waiting_comment = State()


class ExpenseFSM(StatesGroup):
    waiting_amount = State()
    waiting_comment = State()


class WithdrawFSM(StatesGroup):
    waiting_amount  = State()
    waiting_master  = State()
    waiting_comment = State()


class AddMasterFSM(StatesGroup):
    waiting_first_name = State()
    waiting_last_name  = State()
    waiting_phone      = State()


class ReportsFSM(StatesGroup):
    waiting_root        = State()
    waiting_pick_master = State()
    waiting_pick_period = State()
from dotenv import load_dotenv

import asyncpg

# Проверка формата телефона: допускаем +7XXXXXXXXXX, 8XXXXXXXXXX или 9XXXXXXXXX
# Разрешаем пробелы, дефисы и скобки в пользовательском вводе

def is_valid_phone_format(s: str) -> bool:
    d = re.sub(r"[^0-9]", "", s or "")  # оставляем только цифры
    # 11 цифр и начинается с 7 или 8 — ок; 10 цифр и начинается с 9 — ок
    return (len(d) == 11 and d[0] in ("7", "8")) or (len(d) == 10 and d[0] == "9")

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_DSN = os.getenv("DB_DSN")
if not BOT_TOKEN: raise RuntimeError("BOT_TOKEN is not set")
if not DB_DSN:    raise RuntimeError("DB_DSN is not set")
ADMIN_TG_IDS: set[int] = set()
_admin_ids_env = os.getenv("ADMIN_TG_IDS", "") or os.getenv("ADMIN_IDS", "")
for part in re.split(r"[ ,;]+", _admin_ids_env.strip()):
    if part.isdigit():
        ADMIN_TG_IDS.add(int(part))

# env rules
MIN_CASH = Decimal(os.getenv("MIN_CASH", "2500"))
BONUS_RATE = Decimal(os.getenv("BONUS_RATE_PERCENT", "5")) / Decimal(100)
MAX_BONUS_RATE = Decimal(os.getenv("MAX_BONUS_SPEND_RATE_PERCENT", "50")) / Decimal(100)
FUEL_PAY = Decimal(os.getenv("FUEL_PAY", "150"))
MASTER_PER_3000 = Decimal(os.getenv("MASTER_RATE_PER_3000", "1000"))
UPSELL_PER_3000 = Decimal(os.getenv("UPSELL_RATE_PER_3000", "500"))

logging.basicConfig(level=logging.INFO)
bot = Bot(BOT_TOKEN)
dp = Dispatcher()
pool: asyncpg.Pool | None = None

# ===== RBAC helpers (DB-driven) =====
async def get_user_role(conn: asyncpg.Connection, user_id: int) -> str | None:
    rec = await conn.fetchrow(
        "SELECT role FROM staff WHERE tg_user_id=$1 AND is_active LIMIT 1",
        user_id,
    )
    return rec["role"] if rec else None

async def has_permission(user_id: int, permission_name: str) -> bool:
    """Check permission by role via DB tables: permissions, role_permissions.
    Superadmin implicitly has all permissions.
    """
    global pool
    async with pool.acquire() as conn:
        role = await get_user_role(conn, user_id)
        if role is None:
            return False
        if role == "superadmin":
            return True
        rec = await conn.fetchrow(
            """
            SELECT 1
            FROM role_permissions rp
            JOIN permissions p ON p.id = rp.permission_id
            WHERE rp.role = $1 AND p.name = $2
            LIMIT 1
            """,
            role, permission_name,
        )
        return rec is not None

# ===== helpers =====
def only_digits(s: str) -> str:
    return re.sub(r"[^0-9]", "", s or "")

def normalize_phone_for_db(s: str) -> str:
    """Extract first valid RU phone subsequence from mixed text and normalize to +7XXXXXXXXXX.
    Rules:
    - If the first collected digit is '7' or '8' → take exactly 11 digits.
    - If it's '9' → take exactly 10 digits.
    - Stop as soon as enough digits are collected; ignore everything after.
    - Return +7XXXXXXXXXX for 8XXXXXXXXXX/7XXXXXXXXXX/9XXXXXXXXX.
    """
    if not s:
        return s
    first = None
    buf = []
    for ch in s:
        if ch.isdigit():
            if first is None:
                # start only on 7/8/9 as per our formats
                if ch in ('7', '8', '9'):
                    first = ch
                    buf.append(ch)
            else:
                buf.append(ch)
            if first in ('7', '8') and len(buf) == 11:
                break
            if first == '9' and len(buf) == 10:
                break
    if not buf:
        return s
    d = ''.join(buf)
    if len(d) == 10 and d.startswith('9'):
        return '+7' + d
    if len(d) == 11 and d.startswith('8'):
        return '+7' + d[1:]
    if len(d) == 11 and d.startswith('7'):
        return '+' + d
    # fallback to previous behavior if nothing matched cleanly
    digits_all = re.sub(r"[^0-9]", "", s)
    if len(digits_all) == 10 and digits_all.startswith('9'):
        return '+7' + digits_all
    if len(digits_all) == 11 and digits_all.startswith('8'):
        return '+7' + digits_all[1:]
    if len(digits_all) == 11 and digits_all.startswith('7'):
        return '+' + digits_all
    if digits_all and not s.startswith('+'):
        return '+' + digits_all
    return s

# Имя выглядит «плохим», если похоже на пропущенный звонок/метку или содержит телефон
BAD_NAME_PATTERNS = [
    r"^пропущенный\b",      # Пропущенный ...
    r"\bгугл\s*карты\b",  # (.. Гугл Карты)
    r"\bgoogle\s*maps\b", # на случай англ. подписи
    r"\d{10,11}",           # длинная числовая последовательность (похожая на телефон)
]

def is_bad_name(name: str | None) -> bool:
    if not name:
        return False
    low = name.strip().lower()
    for pat in BAD_NAME_PATTERNS:
        if re.search(pat, low):
            return True
    # если имя целиком похоже на номер телефона — тоже считаем плохим
    digits = only_digits(low)
    if digits and (len(digits) in (10, 11)):
        return True
    return False

def qround_ruble(x: Decimal) -> Decimal:
    # округление вниз до рубля
    return x.quantize(Decimal("1."), rounding=ROUND_DOWN)

# Birthday parser: accepts DD.MM.YYYY or YYYY-MM-DD, returns ISO or None
def parse_birthday_str(s: str | None) -> date | None:
    """
    Accepts 'DD.MM.YYYY', 'D.M.YYYY' (1–2 digits) or 'YYYY-MM-DD'.
    Returns Python date or None.
    """
    if not s:
        return None
    s = s.strip()
    if not s:
        return None
    # try D.M.YYYY or DD.MM.YYYY (with optional spaces)
    m = re.fullmatch(r"\s*(\d{1,2})\s*\.\s*(\d{1,2})\s*\.\s*(\d{4})\s*", s)
    if m:
        dd, mm, yyyy = m.groups()
        try:
            return date(int(yyyy), int(mm), int(dd))
        except Exception:
            return None
    # try YYYY-MM-DD
    m = re.fullmatch(r"\s*(\d{4})-(\d{2})-(\d{2})\s*", s)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception:
            return None
    return None

# ===== Client edit helpers =====
async def _find_client_by_phone(conn: asyncpg.Connection, phone_input: str):
    """Lookup client by any phone format. Accepts 8XXXXXXXXXX, +7XXXXXXXXXX, 9XXXXXXXXX, mixed text.
    Uses normalize_phone_for_db first, then falls back to raw digits. Matches by phone_digits.
    """
    s = phone_input or ""
    # normalized to +7XXXXXXXXXX if possible
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

# ==== Payment constants (canonical labels) ====
PAYMENT_METHODS = ["Карта Женя", "Карта Дима", "Наличные", "р/с"]
GIFT_CERT_LABEL = "Подарочный сертификат"

def payment_method_kb() -> ReplyKeyboardMarkup:
    btns = [KeyboardButton(text=m) for m in PAYMENT_METHODS + [GIFT_CERT_LABEL]]
    # разместим в 2-3 ряда
    rows = [
        [btns[0], btns[1]],
        [btns[2], btns[3]],
        [btns[4]],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def admin_payment_method_kb() -> ReplyKeyboardMarkup:
    btns = [KeyboardButton(text=m) for m in PAYMENT_METHODS]
    rows = [
        [btns[0], btns[1]],
        [btns[2], btns[3]],
        [KeyboardButton(text="Отмена")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=True)


def reports_root_kb() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="Мастер/Заказы/Оплаты")],
        [KeyboardButton(text="Мастер/Зарплата")],
        [KeyboardButton(text="Прибыль"), KeyboardButton(text="Касса")],
        [KeyboardButton(text="Типы оплат")],
        [KeyboardButton(text="Назад"), KeyboardButton(text="Отмена")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=True)


def reports_period_kb() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="День"), KeyboardButton(text="Месяц"), KeyboardButton(text="Год")],
        [KeyboardButton(text="Назад"), KeyboardButton(text="Выйти"), KeyboardButton(text="Отмена")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=True)


def admin_root_kb() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="Отчёты")],
        [KeyboardButton(text="Приход"), KeyboardButton(text="Расход"), KeyboardButton(text="Изъятие")],
        [KeyboardButton(text="Мастера"), KeyboardButton(text="Клиенты")],
        [KeyboardButton(text="Tx последние"), KeyboardButton(text="Кто я")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def admin_masters_kb() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="Добавить мастера"), KeyboardButton(text="Список мастеров")],
        [KeyboardButton(text="Деактивировать мастера")],
        [KeyboardButton(text="Назад"), KeyboardButton(text="Отмена")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=True)


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


def tx_last_kb() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="/tx_last 10"), KeyboardButton(text="/tx_last 30"), KeyboardButton(text="/tx_last 50")],
        [KeyboardButton(text="Назад"), KeyboardButton(text="Выйти")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=True)


async def build_masters_kb(conn) -> InlineKeyboardMarkup:
    masters = await conn.fetch(
        "SELECT id, COALESCE(first_name,'') AS fn, COALESCE(last_name,'') AS ln "
        "FROM staff WHERE role='master' AND is_active=true ORDER BY fn, ln, id"
    )

    buttons: list[InlineKeyboardButton] = []
    for r in masters:
        cash_on_hand, _withdrawn = await get_master_wallet(conn, r['id'])
        display_name = f"{r['fn']} {r['ln']}".strip() or f"Мастер {r['id']}"
        amount_str = str((cash_on_hand or Decimal(0)).quantize(Decimal("0.1"))).replace('.', ',')
        label = f"{display_name}({amount_str})"
        buttons.append(
            InlineKeyboardButton(
                text=label,
                callback_data=f"withdraw_master:{r['id']}",
            )
        )

    markup = InlineKeyboardMarkup(row_width=2)
    # store attribute for completeness; InlineKeyboardMarkup ignores resize, but we keep for consistency with requirement
    setattr(markup, "resize_keyboard", True)

    for i in range(0, len(buttons), 2):
        markup.row(*buttons[i:i + 2])

    markup.row(
        InlineKeyboardButton(text="Назад", callback_data="withdraw_nav:back"),
        InlineKeyboardButton(text="Отмена", callback_data="withdraw_nav:cancel"),
    )

    return markup


def withdraw_nav_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Назад"), KeyboardButton(text="Отмена")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


@dp.message(F.text == "Отчёты")
async def reports_root(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "view_orders_reports"):
        return await msg.answer("Только для администраторов.")
    await state.set_state(ReportsFSM.waiting_root)
    await msg.answer("Отчёты: выбери раздел.", reply_markup=reports_root_kb())


@dp.message(StateFilter(None), F.text == "Касса")
@dp.message(ReportsFSM.waiting_root, F.text == "Касса")
@dp.message(AdminMenuFSM.root, F.text == "Касса")
async def reports_shortcut_cash(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "view_cash_reports"):
        return await msg.answer("Только для администраторов.")
    cur_state = await state.get_state()
    logging.info("reports_shortcut_cash: state=%s text=%s", cur_state, msg.text)
    await state.clear()
    await state.update_data(report_kind="Касса")
    await state.set_state(ReportsFSM.waiting_pick_period)
    await msg.answer("Касса: выбери период.", reply_markup=reports_period_kb())


@dp.message(StateFilter(None), F.text == "Прибыль")
@dp.message(ReportsFSM.waiting_root, F.text == "Прибыль")
@dp.message(AdminMenuFSM.root, F.text == "Прибыль")
async def reports_shortcut_profit(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "view_profit_reports"):
        return await msg.answer("Только для администраторов.")
    cur_state = await state.get_state()
    logging.info("reports_shortcut_profit: state=%s text=%s", cur_state, msg.text)
    await state.clear()
    await state.update_data(report_kind="Прибыль")
    await state.set_state(ReportsFSM.waiting_pick_period)
    await msg.answer("Прибыль: выбери период.", reply_markup=reports_period_kb())


@dp.message(StateFilter(None), F.text == "Типы оплат")
@dp.message(ReportsFSM.waiting_root, F.text == "Типы оплат")
@dp.message(AdminMenuFSM.root, F.text == "Типы оплат")
async def reports_shortcut_payment_types(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "view_payments_by_method"):
        return await msg.answer("Только для администраторов.")
    cur_state = await state.get_state()
    logging.info("reports_shortcut_payment_types: state=%s text=%s", cur_state, msg.text)
    await state.clear()
    await state.update_data(report_kind="Типы оплат")
    await state.set_state(ReportsFSM.waiting_pick_period)
    await msg.answer("Типы оплат: выбери период.", reply_markup=reports_period_kb())


@dp.message(ReportsFSM.waiting_pick_period, F.text == "День")
async def reports_run_period_day(msg: Message, state: FSMContext):
    data = await state.get_data()
    kind = data.get("report_kind")
    period = "day"
    if kind == "Касса":
        text = await get_cash_report_text(period)
    elif kind == "Прибыль":
        text = await get_profit_report_text(period)
    elif kind == "Типы оплат":
        text = await get_payments_by_method_report_text(period)
    else:
        text = "Неизвестный тип отчёта."
    await msg.answer(text, reply_markup=reports_period_kb())


@dp.message(ReportsFSM.waiting_pick_period, F.text == "Месяц")
async def reports_run_period_month(msg: Message, state: FSMContext):
    data = await state.get_data()
    kind = data.get("report_kind")
    period = "month"
    if kind == "Касса":
        text = await get_cash_report_text(period)
    elif kind == "Прибыль":
        text = await get_profit_report_text(period)
    elif kind == "Типы оплат":
        text = await get_payments_by_method_report_text(period)
    else:
        text = "Неизвестный тип отчёта."
    await msg.answer(text, reply_markup=reports_period_kb())


@dp.message(ReportsFSM.waiting_pick_period, F.text == "Год")
async def reports_run_period_year(msg: Message, state: FSMContext):
    data = await state.get_data()
    kind = data.get("report_kind")
    period = "year"
    if kind == "Касса":
        text = await get_cash_report_text(period)
    elif kind == "Прибыль":
        text = await get_profit_report_text(period)
    elif kind == "Типы оплат":
        text = await get_payments_by_method_report_text(period)
    else:
        text = "Неизвестный тип отчёта."
    await msg.answer(text, reply_markup=reports_period_kb())


async def _record_income(conn: asyncpg.Connection, method: str, amount: Decimal, comment: str):
    norm = norm_pay_method_py(method)
    tx = await conn.fetchrow(
        """
        INSERT INTO cashbook_entries(kind, method, amount, comment, order_id, master_id, happened_at)
        VALUES ('income', $1, $2, $3, NULL, NULL, now())
        RETURNING id, happened_at
        """,
        norm, amount, comment or "Приход",
    )
    return tx


async def _record_expense(conn: asyncpg.Connection, amount: Decimal, comment: str, method: str = "прочее"):
    tx = await conn.fetchrow(
        """
        INSERT INTO cashbook_entries(kind, method, amount, comment, order_id, master_id, happened_at)
        VALUES ('expense', $1, $2, $3, NULL, NULL, now())
        RETURNING id, happened_at
        """,
        method, amount, comment or "Расход",
    )
    return tx


# Payment method normalizer (Python side to mirror SQL norm_pay_method)
def norm_pay_method_py(p: str | None) -> str:
    """
    Map user input to canonical labels in PAYMENT_METHODS or GIFT_CERT_LABEL.
    """
    if not p:
        return "прочее"
    x = (p or "").strip().lower()
    while "  " in x:
        x = x.replace("  ", " ")
    # gift certificate
    if "подароч" in x:
        return GIFT_CERT_LABEL
    # cash
    if "нал" in x:
        return "Наличные"
    # cards
    if x.startswith("карта дима") or x.startswith("дима"):
        return "Карта Дима"
    if x.startswith("карта женя") or x.startswith("женя"):
        return "Карта Женя"
    # settlement account
    if "р/с" in x or "р\с" in x or "расчет" in x or "расчёт" in x or "счет" in x or "счёт" in x:
        return "р/с"
    return x

async def set_commands():
    cmds = [
        BotCommand(command="start", description="Старт"),
        BotCommand(command="help",  description="Помощь"),
        BotCommand(command="order", description="Добавить заказ (мастер-меню)"),
    ]
    await bot.set_my_commands(cmds, scope=BotCommandScopeDefault())

# ===== Admin commands (must be defined after dp is created) =====
@dp.message(Command("list_masters"))
async def list_masters(msg: Message):
    if not await has_permission(msg.from_user.id, "add_master"):
        return await msg.answer("Только для администраторов.")
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT s.id, s.tg_user_id, s.role, s.is_active, COALESCE(s.first_name,'') AS fn, COALESCE(s.last_name,'') AS ln "
            "FROM staff s WHERE role IN ('master','admin') ORDER BY role DESC, id"
        )
    if not rows:
        return await msg.answer("Список пуст.")
    lines = [f"#{r['id']} {r['role']} {r['fn']} {r['ln']} | tg={r['tg_user_id']} {'✅' if r['is_active'] else '⛔️'}" for r in rows]
    await msg.answer("Мастера/админы:\n" + "\n".join(lines))

@dp.message(Command("add_master"))
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


@dp.message(AddMasterFSM.waiting_first_name)
async def add_master_first(msg: Message, state: FSMContext):
    first = (msg.text or "").strip()
    if len(first) < 2:
        return await msg.answer("Имя слишком короткое. Введите корректное имя (>= 2 символов).")
    await state.update_data(first_name=first)
    await state.set_state(AddMasterFSM.waiting_last_name)
    await msg.answer("Фамилия мастера:")


@dp.message(AddMasterFSM.waiting_last_name)
async def add_master_last(msg: Message, state: FSMContext):
    last = (msg.text or "").strip()
    if len(last) < 2:
        return await msg.answer("Фамилия слишком короткая. Введите корректную фамилию (>= 2 символов).")
    await state.update_data(last_name=last)
    await state.set_state(AddMasterFSM.waiting_phone)
    await msg.answer("Телефон мастера (формат: +7XXXXXXXXXX или 8/9...):")


@dp.message(AddMasterFSM.waiting_phone)
async def add_master_phone(msg: Message, state: FSMContext):
    phone_norm = normalize_phone_for_db(msg.text)
    if not phone_norm or not phone_norm.startswith("+7"):
        return await msg.answer("Не распознал телефон. Пример: +7XXXXXXXXXX. Введите ещё раз.")
    data = await state.get_data()
    if len((data.get("first_name") or "").strip()) < 2 or len((data.get("last_name") or "").strip()) < 2:
        return await msg.answer("Имя/фамилия заданы некорректно. Перезапустите /add_master.")
    tg_id = int(data["tg_id"])
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO staff(tg_user_id, role, is_active, first_name, last_name, phone) "
            "VALUES ($1,'master',true,$2,$3,$4) "
            "ON CONFLICT (tg_user_id) DO UPDATE SET role='master', is_active=true, first_name=$2, last_name=$3, phone=$4",
            tg_id, data.get("first_name"), data.get("last_name"), phone_norm
        )
    await msg.answer(f"✅ Мастер добавлен: {data.get('first_name','')} {data.get('last_name','')}, tg={tg_id}")
    await state.clear()


@dp.message(Command("remove_master"))
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
    async with pool.acquire() as conn:
        await conn.execute("UPDATE staff SET is_active=false WHERE tg_user_id=$1 AND role='master'", target_id)
    await msg.answer(f"Пользователь {target_id} деактивирован как мастер.")


@dp.message(Command("admin_menu"))
async def admin_menu_start(msg: Message, state: FSMContext):
    # пускаем и супер-админа, и обычного админа (где есть право отчётов по заказам)
    if not await has_permission(msg.from_user.id, "view_orders_reports"):
        return await msg.answer("Только для администраторов.")
    await state.set_state(AdminMenuFSM.root)
    await msg.answer("Меню администратора:", reply_markup=admin_root_kb())


@dp.message(AdminMenuFSM.root, F.text == "Изъятие")
async def admin_withdraw_entry(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "record_cashflows"):
        return await msg.answer("Только для администраторов.")
    await state.set_state(WithdrawFSM.waiting_amount)
    return await msg.answer(
        "Введите сумму изъятия (рубли, целое или с копейками):",
        reply_markup=withdraw_nav_kb(),
    )


@dp.message(AdminMenuFSM.root, F.text == "Tx последние")
async def tx_last_menu(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "view_cash_reports"):
        return await msg.answer("Только для администраторов.")
    await msg.answer("Выберите, сколько показать:", reply_markup=tx_last_kb())


@dp.message(AdminMenuFSM.root, F.text == "Клиенты")
async def admin_clients_root(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "edit_client"):
        return await msg.answer("Только для администраторов.")
    await state.set_state(AdminMenuFSM.clients)
    await msg.answer("Клиенты: выбери действие.", reply_markup=admin_clients_kb())


@dp.message(AdminMenuFSM.root, F.text == "Мастера")
async def admin_masters_root(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "add_master"):
        return await msg.answer("Только для администраторов.")
    await state.set_state(AdminMenuFSM.masters)
    await msg.answer("Мастера: выбери действие.", reply_markup=admin_masters_kb())


@dp.message(AdminMenuFSM.clients, F.text == "Найти клиента")
async def client_find_start(msg: Message, state: FSMContext):
    await state.set_state(AdminClientsFSM.find_wait_phone)
    await msg.answer("Введите номер телефона клиента (8/ +7/ 9...):")


@dp.message(AdminMenuFSM.clients, F.text == "Редактировать клиента")
async def client_edit_start(msg: Message, state: FSMContext):
    await state.set_state(AdminClientsFSM.edit_wait_phone)
    await msg.answer("Введите номер телефона клиента для редактирования:")


@dp.message(AdminMenuFSM.clients, F.text == "Назад")
async def admin_clients_back(msg: Message, state: FSMContext):
    await state.set_state(AdminMenuFSM.root)
    await msg.answer("Меню администратора:", reply_markup=admin_root_kb())


@dp.message(AdminMenuFSM.clients, F.text == "Отмена")
async def admin_clients_cancel(msg: Message, state: FSMContext):
    await state.set_state(AdminMenuFSM.root)
    await msg.answer("Меню администратора:", reply_markup=admin_root_kb())


@dp.message(AdminMenuFSM.masters, F.text == "Назад")
async def admin_masters_back(msg: Message, state: FSMContext):
    await state.set_state(AdminMenuFSM.root)
    await msg.answer("Меню администратора:", reply_markup=admin_root_kb())


@dp.message(AdminMenuFSM.masters, F.text == "Отмена")
async def admin_masters_cancel(msg: Message, state: FSMContext):
    await state.set_state(AdminMenuFSM.root)
    await msg.answer("Меню администратора:", reply_markup=admin_root_kb())


@dp.message(AdminMenuFSM.masters, F.text == "Список мастеров")
async def admin_masters_list(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "add_master"):
        return await msg.answer("Только для администраторов.")
    async with pool.acquire() as conn:
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
    await state.set_state(AdminMenuFSM.root)
    await msg.answer("Меню администратора:", reply_markup=admin_root_kb())


@dp.message(AdminMenuFSM.masters, F.text == "Добавить мастера")
async def admin_masters_add(msg: Message, state: FSMContext):
    await state.clear()
    await state.set_state(AdminMenuFSM.root)
    await msg.answer(
        "Введите команду /add_master <tg_user_id> для начала. (Скоро сделаем диалог целиком по кнопкам)",
        reply_markup=admin_root_kb(),
    )


@dp.message(AdminMenuFSM.masters, F.text == "Деактивировать мастера")
async def admin_masters_remove_start(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "add_master"):
        return await msg.answer("Только для администраторов.")
    await state.set_state(AdminMastersFSM.remove_wait_phone)
    await msg.answer("Введите телефон мастера (8/+7/9...):")


@dp.message(AdminMastersFSM.remove_wait_phone)
async def admin_masters_remove_phone(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "add_master"):
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        return await msg.answer("Только для администраторов.", reply_markup=admin_root_kb())
    phone = normalize_phone_for_db(msg.text)
    if not phone or not phone.startswith("+7"):
        return await msg.answer("Неверный телефон. Пример: +7XXXXXXXXXX. Введите ещё раз.")
    async with pool.acquire() as conn:
        rec = await conn.fetchrow(
            "SELECT id FROM staff WHERE phone=$1 AND role='master' LIMIT 1",
            phone,
        )
        if not rec:
            await state.clear()
            await state.set_state(AdminMenuFSM.root)
            return await msg.answer("Мастер не найден по этому телефону.", reply_markup=admin_root_kb())
        await conn.execute("UPDATE staff SET is_active=false WHERE id=$1", rec["id"])
    await state.clear()
    await state.set_state(AdminMenuFSM.root)
    await msg.answer("Мастер деактивирован.", reply_markup=admin_root_kb())


async def get_master_wallet(conn, master_id: int) -> tuple[Decimal, Decimal]:
    """
    Возвращает (cash_on_hand, withdrawn_total) по тем же правилам, что и в отчёте «Мастер/Заказы/Оплаты».
    cash_on_hand = «Наличных у мастера»
    withdrawn_total = «Изъято у мастера»
    """
    cash_on_orders = await conn.fetchval(
        """
        SELECT COALESCE(SUM(amount),0)
        FROM cashbook_entries
        WHERE kind='income' AND method='Наличные'
          AND master_id=$1 AND order_id IS NOT NULL
        """,
        master_id,
    )
    withdrawn = await conn.fetchval(
        """
        SELECT COALESCE(SUM(amount),0)
        FROM cashbook_entries
        WHERE kind='income' AND method='Изъятие'
          AND master_id=$1 AND order_id IS NULL
        """,
        master_id,
    )

    from decimal import Decimal

    return Decimal(cash_on_orders or 0), Decimal(withdrawn or 0)


@dp.message(WithdrawFSM.waiting_amount, F.text.lower() == "назад")
async def withdraw_amount_back(msg: Message, state: FSMContext):
    logging.info(f"[withdraw] step=amount_back user={msg.from_user.id} text={msg.text}")
    await state.set_state(AdminMenuFSM.root)
    await msg.answer("Меню администратора:", reply_markup=admin_root_kb())


@dp.message(WithdrawFSM.waiting_amount, F.text.lower() == "отмена")
async def withdraw_amount_cancel(msg: Message, state: FSMContext):
    logging.info(f"[withdraw] step=amount_cancel user={msg.from_user.id} text={msg.text}")
    await state.set_state(AdminMenuFSM.root)
    await msg.answer("Меню администратора:", reply_markup=admin_root_kb())


@dp.message(WithdrawFSM.waiting_amount)
async def withdraw_amount_got(msg: Message, state: FSMContext):
    logging.info(f"[withdraw] step=amount_input user={msg.from_user.id} text={msg.text}")
    txt = (msg.text or "").replace(",", ".").strip()
    try:
        amt = Decimal(txt)
    except Exception:
        return await msg.answer("Не понял сумму. Пример: 2500 или 2500.50", reply_markup=withdraw_nav_kb())
    if amt <= 0:
        return await msg.answer("Сумма должна быть > 0. Введите заново:", reply_markup=withdraw_nav_kb())
    await state.update_data(withdraw_amount=str(amt))
    async with pool.acquire() as conn:
        kb = await build_masters_kb(conn)
    await state.set_state(WithdrawFSM.waiting_master)
    return await msg.answer(
        "Выберите мастера:\nДоступно к изъятию = только наличные, которые мастер получил по заказам. "
        "Изъятия уменьшают этот остаток.",
        reply_markup=kb,
    )


@dp.message(WithdrawFSM.waiting_master, F.text.lower() == "назад")
async def withdraw_master_back(msg: Message, state: FSMContext):
    logging.info(f"[withdraw] step=master_back user={msg.from_user.id} text={msg.text}")
    await state.set_state(WithdrawFSM.waiting_amount)
    await msg.answer("Введите сумму изъятия:", reply_markup=withdraw_nav_kb())


@dp.message(WithdrawFSM.waiting_master, F.text.lower() == "отмена")
async def withdraw_master_cancel(msg: Message, state: FSMContext):
    logging.info(f"[withdraw] step=master_cancel user={msg.from_user.id} text={msg.text}")
    await state.set_state(AdminMenuFSM.root)
    await msg.answer("Меню администратора:", reply_markup=admin_root_kb())


@dp.callback_query(WithdrawFSM.waiting_master)
async def withdraw_master_callback(query: CallbackQuery, state: FSMContext):
    logging.info(
        f"[withdraw] step=master_callback user={query.from_user.id} data={query.data}"
    )
    data = (query.data or "").strip()

    if data == "withdraw_nav:back":
        await query.answer()
        await state.set_state(WithdrawFSM.waiting_amount)
        await query.message.answer(
            "Введите сумму изъятия:", reply_markup=withdraw_nav_kb()
        )
        return

    if data == "withdraw_nav:cancel":
        await query.answer()
        await state.set_state(AdminMenuFSM.root)
        await query.message.answer(
            "Меню администратора:", reply_markup=admin_root_kb()
        )
        return

    if not data.startswith("withdraw_master:"):
        await query.answer("Неизвестное действие", show_alert=True)
        return

    try:
        master_id = int(data.split(":", 1)[1])
    except Exception:
        await query.answer("Некорректные данные", show_alert=True)
        return

    async with pool.acquire() as conn:
        cash_on_hand, withdrawn = await get_master_wallet(conn, master_id)

    stored = await state.get_data()
    amount = Decimal(stored.get("withdraw_amount", "0") or "0")

    if amount > cash_on_hand:
        await query.answer("Недостаточно наличных", show_alert=True)
        await state.set_state(WithdrawFSM.waiting_amount)
        await query.message.answer(
            f"У мастера на руках {cash_on_hand}₽ (изъято ранее {withdrawn}₽). "
            f"Вы запросили {amount}₽ — это больше доступного.\n"
            f"Введите сумму не больше {cash_on_hand}₽:",
            reply_markup=withdraw_nav_kb(),
        )
        return

    await query.answer()
    await state.update_data(withdraw_master_id=master_id)
    await state.set_state(WithdrawFSM.waiting_comment)
    await query.message.answer(
        "Комментарий (или «Без комментария»):",
        reply_markup=withdraw_nav_kb(),
    )


@dp.message(WithdrawFSM.waiting_master)
async def withdraw_master_got(msg: Message, state: FSMContext):
    logging.info(f"[withdraw] step=master_text_input user={msg.from_user.id} text={msg.text}")
    async with pool.acquire() as conn:
        kb = await build_masters_kb(conn)
    return await msg.answer("Пожалуйста, выберите мастера кнопкой ниже.", reply_markup=kb)


@dp.message(WithdrawFSM.waiting_comment, F.text.lower() == "назад")
async def withdraw_comment_back(msg: Message, state: FSMContext):
    logging.info(f"[withdraw] step=comment_back user={msg.from_user.id} text={msg.text}")
    async with pool.acquire() as conn:
        kb = await build_masters_kb(conn)
    await state.set_state(WithdrawFSM.waiting_master)
    await msg.answer("Выберите мастера:", reply_markup=kb)


@dp.message(WithdrawFSM.waiting_comment, F.text.lower() == "отмена")
async def withdraw_comment_cancel(msg: Message, state: FSMContext):
    logging.info(f"[withdraw] step=comment_cancel user={msg.from_user.id} text={msg.text}")
    await state.set_state(AdminMenuFSM.root)
    await msg.answer("Меню администратора:", reply_markup=admin_root_kb())


@dp.message(WithdrawFSM.waiting_comment)
async def withdraw_finish(msg: Message, state: FSMContext):
    logging.info(f"[withdraw] step=finish user={msg.from_user.id} text={msg.text}")
    data = await state.get_data()
    try:
        amount = Decimal(data.get("withdraw_amount", "0"))
        master_id = int(data.get("withdraw_master_id"))
    except Exception:
        await state.set_state(AdminMenuFSM.root)
        return await msg.answer("Сессия изъятия потеряна. Попробуйте снова.", reply_markup=admin_root_kb())
    comment = (msg.text or "").strip()
    if comment.lower() == "без комментария":
        comment = "Изъятие у мастера"
    async with pool.acquire() as conn:
        tx = await conn.fetchrow(
            """
            INSERT INTO cashbook_entries(kind, method, amount, comment, order_id, master_id, happened_at)
            VALUES ('income', 'Изъятие', $1, $2, NULL, $3, now())
            RETURNING id
            """,
            amount,
            comment,
            master_id,
        )
    await state.set_state(AdminMenuFSM.root)
    return await msg.answer(
        f"✅ Изъятие зафиксировано: №{tx['id']} | {amount}₽ | мастер id={master_id}\nКомментарий: {comment}",
        reply_markup=admin_root_kb(),
    )


@dp.message(StateFilter(AdminClientsFSM.find_wait_phone, AdminClientsFSM.edit_wait_phone, AdminClientsFSM.edit_pick_field, AdminClientsFSM.edit_wait_value), F.text == "Назад")
async def admin_clients_states_back(msg: Message, state: FSMContext):
    await state.clear()
    await state.set_state(AdminMenuFSM.clients)
    await msg.answer("Клиенты: выбери действие.", reply_markup=admin_clients_kb())


@dp.message(StateFilter(AdminClientsFSM.find_wait_phone, AdminClientsFSM.edit_wait_phone, AdminClientsFSM.edit_pick_field, AdminClientsFSM.edit_wait_value), F.text == "Отмена")
async def admin_clients_states_cancel(msg: Message, state: FSMContext):
    await state.clear()
    await state.set_state(AdminMenuFSM.root)
    await msg.answer("Меню администратора:", reply_markup=admin_root_kb())


@dp.message(AdminClientsFSM.find_wait_phone)
async def client_find_got_phone(msg: Message, state: FSMContext):
    async with pool.acquire() as conn:
        rec = await _find_client_by_phone(conn, msg.text)
    if not rec:
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        return await msg.answer("Клиент не найден.", reply_markup=admin_root_kb())
    await state.clear()
    await state.set_state(AdminMenuFSM.root)
    return await msg.answer(_fmt_client_row(rec), reply_markup=admin_root_kb())


@dp.message(AdminClientsFSM.edit_wait_phone)
async def client_edit_got_phone(msg: Message, state: FSMContext):
    async with pool.acquire() as conn:
        rec = await _find_client_by_phone(conn, msg.text)
    if not rec:
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        return await msg.answer("Клиент не найден.", reply_markup=admin_root_kb())
    await state.update_data(client_id=rec["id"])
    await state.set_state(AdminClientsFSM.edit_pick_field)
    await msg.answer("Что изменить?", reply_markup=client_edit_fields_kb())


@dp.message(AdminClientsFSM.edit_pick_field, F.text.in_({"Имя", "Телефон", "ДР", "Бонусы установить", "Бонусы добавить/убавить"}))
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


@dp.message(AdminClientsFSM.edit_wait_value)
async def client_edit_apply(msg: Message, state: FSMContext):
    data = await state.get_data()
    client_id = data.get("client_id")
    field = data.get("edit_field")
    if not client_id or not field:
        await state.clear()
        return await msg.answer("Сессия сброшена, попробуйте заново.", reply_markup=admin_root_kb())

    async with pool.acquire() as conn:
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
    await state.set_state(AdminMenuFSM.root)
    return await msg.answer("Готово. Клиент обновлён.", reply_markup=admin_root_kb())


@dp.message(Command("admin_panel"))
async def admin_panel_alias(msg: Message, state: FSMContext):
    await admin_menu_start(msg, state)


@dp.message(Command("help"))
async def help_cmd(msg: Message):
    global pool
    async with pool.acquire() as conn:
        rec = await conn.fetchrow(
            "SELECT role, is_active FROM staff WHERE tg_user_id=$1 LIMIT 1",
            msg.from_user.id,
        )
    role = rec["role"] if rec else None

    if role in ("admin", "superadmin"):
        text = (
            "Команды администратора:\n"
            "/admin_panel — открыть меню администратора\n"
            "\n"
            "/whoami — кто я, мои права\n"
            "\n"
            "/tx_last 10 — последние 10 транзакций\n"
            "\n"
            "/cash day — касса за день\n"
            "\n"
            "/profit day — прибыль за день\n"
            "\n"
            "/payments day — приход по типам оплаты за день\n"
            "\n"
            "/order — открыть добавление заказа (клавиатура мастера)\n"
        )
    else:
        text = (
            "Команды мастера:\n"
            "/whoami — кто я, мои права\n"
            "\n"
            "Для оформления заказа используйте кнопки внизу."
        )

    await msg.answer(text)


@dp.message(Command("order"))
async def order_open_master_flow(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer(
        "Мастер: оформление заказа.\nНажми «🧾 Я ВЫПОЛНИЛ ЗАКАЗ» и следуй шагам.",
        reply_markup=master_main_kb()
    )


@dp.message(Command("whoami"))
async def whoami(msg: Message):
    global pool
    async with pool.acquire() as conn:
        rec = await conn.fetchrow(
            "SELECT role, is_active, first_name, last_name FROM staff WHERE tg_user_id=$1 LIMIT 1",
            msg.from_user.id,
        )
        role = rec["role"] if rec else None
        is_active = bool(rec["is_active"]) if rec else False
        first = rec["first_name"] if rec else None
        last = rec["last_name"] if rec else None
        perms = []
        if role:
            rows = await conn.fetch(
                """
                SELECT p.name
                FROM role_permissions rp
                JOIN permissions p ON p.id = rp.permission_id
                WHERE rp.role = $1
                ORDER BY p.name
                """,
                role,
            )
            perms = [r["name"] for r in rows]
    await msg.answer(
        "\n".join([
            f"Ваш id: {msg.from_user.id}",
            f"Роль: {role or '—'}",
            f"Активен: {'✅' if is_active else '⛔️'}",
            f"Имя: {((first or '').strip() + (' ' + (last or '').strip() if (last or '').strip() else '')).strip() or '—'}",
            f"ADMIN_TG_IDS={sorted(ADMIN_TG_IDS)}",
            ("Права: " + (", ".join(perms) if perms else "—"))
        ])
    )

# ===== Client admin edit commands =====
@dp.message(Command("client_info"))
async def client_info(msg: Message):
    if not await has_permission(msg.from_user.id, "edit_client"):
        return await msg.answer("Только для администраторов.")
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        return await msg.answer("Формат: /client_info <телефон>")
    phone_q = parts[1].strip()
    async with pool.acquire() as conn:
        rec = await _find_client_by_phone(conn, phone_q)
    if not rec:
        return await msg.answer("Клиент не найден по этому номеру.")
    return await msg.answer(_fmt_client_row(rec))

@dp.message(Command("client_set_name"))
async def client_set_name(msg: Message):
    if not await has_permission(msg.from_user.id, "edit_client"):
        return await msg.answer("Только для администраторов.")
    parts = msg.text.split(maxsplit=2)
    if len(parts) < 3:
        return await msg.answer("Формат: /client_set_name <телефон> <новое_имя>")
    phone_q = parts[1].strip()
    new_name = parts[2].strip()
    async with pool.acquire() as conn:
        rec = await _find_client_by_phone(conn, phone_q)
        if not rec:
            return await msg.answer("Клиент не найден по этому номеру.")
        await conn.execute("UPDATE clients SET full_name=$1, last_updated=NOW() WHERE id=$2", new_name, rec["id"])
        rec2 = await conn.fetchrow("SELECT id, full_name, phone, birthday, bonus_balance, status FROM clients WHERE id=$1", rec["id"])
    return await msg.answer("Имя обновлено:\n" + _fmt_client_row(rec2))

@dp.message(Command("client_set_birthday"))
async def client_set_birthday(msg: Message):
    if not await has_permission(msg.from_user.id, "edit_client"):
        return await msg.answer("Только для администраторов.")
    try:
        parts = msg.text.split(maxsplit=2)
        if len(parts) < 3:
            return await msg.answer("Формат: /client_set_birthday <телефон> <ДР: DD.MM.YYYY или YYYY-MM-DD>")
        phone_q = parts[1].strip()
        bday_raw = parts[2].strip()

        # 1) нормализация даты → Python date
        bday_date = parse_birthday_str(bday_raw)
        if not bday_date:
            return await msg.answer("Не распознал дату. Форматы: DD.MM.YYYY (допускаются 1-2 цифры) или YYYY-MM-DD.")

        # 2) поиск клиента и обновление
        async with pool.acquire() as conn:
            rec = await _find_client_by_phone(conn, phone_q)
            if not rec:
                norm = normalize_phone_for_db(phone_q)
                digits = re.sub(r"[^0-9]", "", norm or phone_q)
                return await msg.answer(f"Клиент не найден по номеру.\nИскали: {phone_q}\nНормализовано: {norm}\nЦифры: {digits}")

            await conn.execute(
                "UPDATE clients SET birthday=$1, last_updated=NOW() WHERE id=$2",
                bday_date, rec["id"]
            )
            rec2 = await conn.fetchrow(
                "SELECT id, full_name, phone, birthday, bonus_balance, status FROM clients WHERE id=$1",
                rec["id"]
            )

        return await msg.answer("ДР обновлён:\n" + _fmt_client_row(rec2))

    except Exception as e:
        logging.exception("client_set_birthday failed")
        return await msg.answer(f"Ошибка при обновлении ДР: {e}")

@dp.message(Command("client_set_bonus"))
async def client_set_bonus(msg: Message):
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
    async with pool.acquire() as conn:
        rec = await _find_client_by_phone(conn, phone_q)
        if not rec:
            return await msg.answer("Клиент не найден по этому номеру.")
        await conn.execute("UPDATE clients SET bonus_balance=$1, last_updated=NOW() WHERE id=$2", amount, rec["id"])
        rec2 = await conn.fetchrow("SELECT id, full_name, phone, birthday, bonus_balance, status FROM clients WHERE id=$1", rec["id"])
    return await msg.answer("Бонусы установлены:\n" + _fmt_client_row(rec2))

@dp.message(Command("client_add_bonus"))
async def client_add_bonus(msg: Message):
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
    async with pool.acquire() as conn:
        rec = await _find_client_by_phone(conn, phone_q)
        if not rec:
            return await msg.answer("Клиент не найден по этому номеру.")
        new_bonus = int(rec["bonus_balance"] or 0) + delta
        if new_bonus < 0:
            new_bonus = 0
        await conn.execute("UPDATE clients SET bonus_balance=$1, last_updated=NOW() WHERE id=$2", new_bonus, rec["id"])
        rec2 = await conn.fetchrow("SELECT id, full_name, phone, birthday, bonus_balance, status FROM clients WHERE id=$1", rec["id"])
    return await msg.answer("Бонусы обновлены:\n" + _fmt_client_row(rec2))

@dp.message(Command("client_set_phone"))
async def client_set_phone(msg: Message):
    if not await has_permission(msg.from_user.id, "edit_client"):
        return await msg.answer("Только для администраторов.")
    parts = msg.text.split(maxsplit=2)
    if len(parts) < 3:
        return await msg.answer("Формат: /client_set_phone <старый_телефон> <новый_телефон>")
    phone_q = parts[1].strip()
    new_phone_raw = parts[2].strip()
    new_phone_norm = normalize_phone_for_db(new_phone_raw)
    if not new_phone_norm or not new_phone_norm.startswith("+7") or len(re.sub(r"[^0-9]", "", new_phone_norm)) != 11:
        return await msg.answer("Не распознал новый телефон. Пример: +7XXXXXXXXXX")
    async with pool.acquire() as conn:
        rec = await _find_client_by_phone(conn, phone_q)
        if not rec:
            return await msg.answer("Клиент не найден по этому номеру.")
        try:
            await conn.execute("UPDATE clients SET phone=$1, last_updated=NOW() WHERE id=$2", new_phone_norm, rec["id"])
        except asyncpg.exceptions.UniqueViolationError:
            # конфликт по уникальному phone/phone_digits
            other = await conn.fetchrow(
                "SELECT id, full_name FROM clients WHERE phone_digits = regexp_replace($1,'[^0-9]','','g') AND id <> $2",
                new_phone_norm, rec["id"]
            )
            if other:
                return await msg.answer(f"Номер уже используется клиентом id={other['id']} ({other['full_name'] or '—'}).")
            return await msg.answer("Номер уже используется другим клиентом.")
        rec2 = await conn.fetchrow("SELECT id, full_name, phone, birthday, bonus_balance, status FROM clients WHERE id=$1", rec["id"])
    return await msg.answer("Телефон обновлён:\n" + _fmt_client_row(rec2))

# ===== /payroll admin command =====
@dp.message(Command("payroll"))
async def payroll_report(msg: Message):
    if not await has_permission(msg.from_user.id, "view_salary_reports"):
        return await msg.answer("Только для администраторов.")
    # формат: /payroll 2025-09
    parts = msg.text.split(maxsplit=1)
    period = (parts[1] if len(parts) > 1 else "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}", period):
        return await msg.answer("Формат: /payroll YYYY-MM")
    year, month = map(int, period.split("-"))
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT s.tg_user_id,
                   SUM(pi.base_pay) AS base_pay,
                   SUM(pi.fuel_pay) AS fuel_pay,
                   SUM(pi.upsell_pay) AS upsell_pay,
                   SUM(pi.total_pay) AS total_pay,
                   COUNT(*) as orders
            FROM payroll_items pi
            JOIN staff s ON s.id = pi.master_id
            WHERE date_trunc('month', (SELECT o.created_at FROM orders o WHERE o.id = pi.order_id)) = $1::date
            GROUP BY s.tg_user_id
            ORDER BY total_pay DESC
            """,
            f"{year:04d}-{month:02d}-01"
        )
    if not rows:
        return await msg.answer("Нет данных за указанный период.")
    lines = [
        f"tg={r['tg_user_id']} | заказы: {r['orders']} | оплата: {r['total_pay']} (база {r['base_pay']} + бенз {r['fuel_pay']} + доп {r['upsell_pay']})"
        for r in rows
    ]
    await msg.answer(f"ЗП за {period}:\n" + "\n".join(lines))

# ---- helper for /cash (aggregates; year -> monthly details)
async def get_cash_report_text(period: str) -> str:
    """
    Build cash report text for:
      period in {"day","month","year"} or specific "YYYY-MM" / "YYYY-MM-DD".
    For 'year' the details are aggregated by months, not by days.
    """
    import re
    def trunc(unit: str) -> str:
        # compute bounds on DB side
        return f"date_trunc('{unit}', NOW())"

    if period in ("day", "month", "year"):
        period_label = {"day": "сегодня", "month": "текущий месяц", "year": "текущий год"}[period]
        unit = period
        start_sql = trunc(unit)
        end_sql = f"{trunc(unit)} + interval '1 {unit}'"
        detail_by_months = (period == "year")
    else:
        mday = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", period or "")
        mmon = re.fullmatch(r"(\d{4})-(\d{2})", period or "")
        if mday:
            y, m, d = map(int, mday.groups())
            period_label = f"{y:04d}-{m:02d}-{d:02d}"
            start_sql = f"TIMESTAMP WITH TIME ZONE '{y:04d}-{m:02d}-{d:02d} 00:00:00+00'"
            end_sql   = f"{start_sql} + interval '1 day'"
            detail_by_months = False
        elif mmon:
            y, m = map(int, mmon.groups())
            period_label = f"{y:04d}-{m:02d}"
            start_sql = f"TIMESTAMP WITH TIME ZONE '{y:04d}-{m:02d}-01 00:00:00+00'"
            end_sql   = f"{start_sql} + interval '1 month'"
            detail_by_months = False
        else:
            return "Формат: /cash [day|month|year|YYYY-MM|YYYY-MM-DD]"

    async with pool.acquire() as conn:
        rec = await conn.fetchrow(
            f"""
            SELECT
              COALESCE(SUM(income),  0)::numeric(12,2) AS income,
              COALESCE(SUM(expense), 0)::numeric(12,2) AS expense,
              COALESCE(SUM(delta),   0)::numeric(12,2) AS delta
            FROM v_cash_summary
            WHERE day >= {start_sql} AND day < {end_sql};
            """
        )
        if detail_by_months:
            rows = await conn.fetch(
                f"""
                SELECT date_trunc('month', day) AS g,
                       COALESCE(SUM(income),0)::numeric(12,2)  AS income,
                       COALESCE(SUM(expense),0)::numeric(12,2) AS expense,
                       COALESCE(SUM(delta),0)::numeric(12,2)   AS delta
                FROM v_cash_summary
                WHERE day >= {start_sql} AND day < {end_sql}
                GROUP BY 1
                ORDER BY 1 DESC
                LIMIT 12;
                """
            )
            detail_label = "Детализация по месяцам (последние):"
        else:
            rows = await conn.fetch(
                f"""
                SELECT day::date AS g,
                       COALESCE(income,0)::numeric(12,2)  AS income,
                       COALESCE(expense,0)::numeric(12,2) AS expense,
                       COALESCE(delta,0)::numeric(12,2)   AS delta
                FROM v_cash_summary
                WHERE day >= {start_sql} AND day < {end_sql}
                ORDER BY day DESC
                LIMIT 31;
                """
            )
            detail_label = "Детализация по дням (последние):"

    income  = rec["income"] or 0
    expense = rec["expense"] or 0
    delta   = rec["delta"] or 0

    lines = [
        f"Касса за {period_label}:",
        f"➕ Приход: {income}₽",
        f"➖ Расход: {expense}₽",
        f"= Дельта: {delta}₽",
    ]
    if rows:
        lines.append(f"\n{detail_label}")
        for r in rows:
            g = r["g"]
            # g can be date/datetime
            try:
                # choose format by detail type
                label = g.strftime("%Y-%m") if "месяц" in detail_label else g.strftime("%Y-%m-%d")
            except Exception:
                label = str(g)
            lines.append(f"{label}: +{r['income']} / -{r['expense']} = {r['delta']}₽")
    return "\n".join(lines)

# ===== /cash admin command =====
@dp.message(Command("cash"))
async def cash_report(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "view_cash_reports"):
        return await msg.answer("Только для администраторов.")
    parts = msg.text.split(maxsplit=1)
    if len(parts) == 1:
        # без аргумента — открыть выбор периода, как по кнопке "Касса"
        await state.clear()
        await state.update_data(report_kind="Касса")
        await state.set_state(ReportsFSM.waiting_pick_period)
        return await msg.answer("Касса: выбери период.", reply_markup=reports_period_kb())
    period = parts[1].strip().lower()
    text = await get_cash_report_text(period)
    await msg.answer(text)

# ---- helper for /profit (aggregates; year -> monthly details)
async def get_profit_report_text(period: str) -> str:
    """
    Build profit report text for:
      period in {"day","month","year"} or specific "YYYY-MM" / "YYYY-MM-DD".
    For 'year' the details are aggregated by months, not by days.
    """
    import re
    def trunc(unit: str) -> str:
        return f"date_trunc('{unit}', NOW())"

    if period in ("day", "month", "year"):
        period_label = {"day": "сегодня", "month": "текущий месяц", "year": "текущий год"}[period]
        unit = period
        start_sql = trunc(unit)
        end_sql = f"{trunc(unit)} + interval '1 {unit}'"
        by_months = (period == "year")
    else:
        mday = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", period or "")
        mmon = re.fullmatch(r"(\d{4})-(\d{2})", period or "")
        if mday:
            y, m, d = map(int, mday.groups())
            period_label = f"{y:04d}-{m:02d}-{d:02d}"
            start_sql = f"TIMESTAMP WITH TIME ZONE '{y:04d}-{m:02d}-{d:02d} 00:00:00+00'"
            end_sql   = f"{start_sql} + interval '1 day'"
            by_months = False
        elif mmon:
            y, m = map(int, mmon.groups())
            period_label = f"{y:04d}-{m:02d}"
            start_sql = f"TIMESTAMP WITH TIME ZONE '{y:04d}-{m:02d}-01 00:00:00+00'"
            end_sql   = f"{start_sql} + interval '1 month'"
            by_months = False
        else:
            return "Формат: /profit [day|month|year|YYYY-MM|YYYY-MM-DD]"

    group_sql = "date_trunc('month', day)" if by_months else "date_trunc('day', day)"
    detail_label = "По месяцам (последние):" if by_months else "По дням (последние):"

    async with pool.acquire() as conn:
        rev = await conn.fetchval(
            f"""
            SELECT COALESCE(SUM(o.amount_cash), 0)::numeric(12,2)
            FROM orders o
            WHERE o.created_at >= {start_sql} AND o.created_at < {end_sql}
            """
        )
        exp = await conn.fetchval(
            f"""
            SELECT COALESCE(SUM(c.amount), 0)::numeric(12,2)
            FROM cashbook_entries c
            WHERE c.kind='expense' AND c.happened_at >= {start_sql} AND c.happened_at < {end_sql}
            """
        )
        rows = await conn.fetch(
            f"""
            WITH
            r AS (
              SELECT date_trunc('day', o.created_at) AS day, SUM(o.amount_cash) AS revenue
              FROM orders o
              WHERE o.created_at >= {start_sql} AND o.created_at < {end_sql}
              GROUP BY 1
            ),
            e AS (
              SELECT date_trunc('day', c.happened_at) AS day, SUM(c.amount) AS expense
              FROM cashbook_entries c
              WHERE c.kind='expense' AND c.happened_at >= {start_sql} AND c.happened_at < {end_sql}
              GROUP BY 1
            ),
            d AS (
              SELECT COALESCE(r.day, e.day) AS day,
                     COALESCE(r.revenue, 0)   AS revenue,
                     COALESCE(e.expense, 0)   AS expense
              FROM r FULL OUTER JOIN e ON r.day = e.day
            )
            SELECT {group_sql} AS g,
                   COALESCE(SUM(revenue),0)::numeric(12,2) AS revenue,
                   COALESCE(SUM(expense),0)::numeric(12,2) AS expense,
                   (COALESCE(SUM(revenue),0) - COALESCE(SUM(expense),0))::numeric(12,2) AS profit
            FROM d
            GROUP BY 1
            ORDER BY 1 DESC
            LIMIT 31;
            """
        )

    profit = (rev or 0) - (exp or 0)
    lines = [
        f"Прибыль за {period_label}:",
        f"💰 Выручка: {rev or 0}₽",
        f"💸 Расходы: {exp or 0}₽",
        f"= Прибыль: {profit}₽",
    ]
    if rows:
        lines.append(f"\n{detail_label}")
        for r in rows:
            g = r["g"]
            try:
                s = g.strftime("%Y-%m") if by_months else g.strftime("%Y-%m-%d")
            except Exception:
                s = str(g)
            lines.append(f"{s}: выручка {r['revenue']} / расходы {r['expense']} → прибыль {r['profit']}₽")
    return "\n".join(lines)


async def get_payments_by_method_report_text(period: str) -> str:
    """
    Суммируем приходы по cashbook_entries.kind='income' с группировкой по method
    за указанный период. Поддержка period как в других отчётах.
    """
    import re

    def trunc(unit: str) -> str:
        return f"date_trunc('{unit}', NOW())"

    if period in ("day", "month", "year"):
        period_label = {"day": "сегодня", "month": "текущий месяц", "year": "текущий год"}[period]
        unit = period
        start_sql = trunc(unit)
        end_sql = f"{trunc(unit)} + interval '1 {unit}'"
    else:
        mday = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", period or "")
        mmon = re.fullmatch(r"(\d{4})-(\d{2})", period or "")
        if mday:
            y, m, d = map(int, mday.groups())
            period_label = f"{y:04d}-{m:02d}-{d:02d}"
            start_sql = f"TIMESTAMP WITH TIME ZONE '{y:04d}-{m:02d}-{d:02d} 00:00:00+00'"
            end_sql = f"{start_sql} + interval '1 day'"
        elif mmon:
            y, m = map(int, mmon.groups())
            period_label = f"{y:04d}-{m:02d}"
            start_sql = f"TIMESTAMP WITH TIME ZONE '{y:04d}-{m:02d}-01 00:00:00+00'"
            end_sql = f"{start_sql} + interval '1 month'"
        else:
            return "Формат: /payments [day|month|year|YYYY-MM|YYYY-MM-DD]"

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT COALESCE(method,'прочее') AS method,
                   COUNT(*)::int AS cnt,
                   COALESCE(SUM(amount),0)::numeric(12,2) AS total
            FROM cashbook_entries
            WHERE kind='income'
              AND happened_at >= {start_sql} AND happened_at < {end_sql}
            GROUP BY 1
            ORDER BY total DESC, method
            """
        )
        total_income = await conn.fetchval(
            f"""
            SELECT COALESCE(SUM(amount),0)::numeric(12,2)
            FROM cashbook_entries
            WHERE kind='income'
              AND happened_at >= {start_sql} AND happened_at < {end_sql}
            """
        )

    if not rows:
        return f"Типы оплат за {period_label}: данных нет."

    lines = [f"Типы оплат за {period_label}: (итого {total_income}₽)"]
    for r in rows:
        method = r["method"]
        lines.append(f"- {method}: {r['total']}₽ ({r['cnt']} шт.)")
    return "\n".join(lines)


# ===== /profit admin command =====
@dp.message(Command("profit"))
async def profit_report(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "view_profit_reports"):
        return await msg.answer("Только для администраторов.")
    parts = msg.text.split(maxsplit=1)
    if len(parts) == 1:
        # без аргумента — открыть выбор периода, как по кнопке "Прибыль"
        await state.clear()
        await state.update_data(report_kind="Прибыль")
        await state.set_state(ReportsFSM.waiting_pick_period)
        return await msg.answer("Прибыль: выбери период.", reply_markup=reports_period_kb())
    period = parts[1].strip().lower()
    text = await get_profit_report_text(period)
    await msg.answer(text)


@dp.message(Command("payments"))
async def payments_report(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "view_payments_by_method"):
        return await msg.answer("Только для администраторов.")
    parts = msg.text.split(maxsplit=1)
    if len(parts) == 1:
        # без аргумента — открыть выбор периода, как по кнопке "Типы оплат"
        await state.clear()
        await state.update_data(report_kind="Типы оплат")
        await state.set_state(ReportsFSM.waiting_pick_period)
        return await msg.answer("Типы оплат: выбери период.", reply_markup=reports_period_kb())
    period = parts[1].strip().lower()
    text = await get_payments_by_method_report_text(period)
    await msg.answer(text)


@dp.message(Command("orders"))
async def orders_report(msg: Message):
    if not await has_permission(msg.from_user.id, "view_orders_report"):
        return await msg.answer("Только для администраторов.")

    # Форматы:
    # /orders                         -> сегодня
    # /orders day|month|year          -> текущий период
    # /orders YYYY-MM                 -> конкретный месяц
    # /orders YYYY-MM-DD              -> конкретный день
    # Дополнительно: master:<tg_id>   -> фильтр по мастеру (tg_user_id)
    #                master_id:<id>   -> фильтр по staff.id
    # /orders 2025-10 master:123456

    txt = (msg.text or "")
    parts = txt.split()
    # parts[0] = '/orders'
    args = parts[1:] if len(parts) > 1 else []

    # разбор периода
    period_arg = args[0].lower() if args else "day"
    mday = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", period_arg)
    mmon = re.fullmatch(r"(\d{4})-(\d{2})", period_arg)

    def trunc(unit: str) -> str:
        return f"date_trunc('{unit}', NOW())"

    if period_arg in ("day","month","year"):
        period_label = {"day":"сегодня", "month":"текущий месяц", "year":"текущий год"}[period_arg]
        unit = period_arg
        start_sql = trunc(unit)
        end_sql   = f"{trunc(unit)} + interval '1 {unit}'"
        rest_args = args[1:]
    elif mday:
        y,m,d = map(int, mday.groups())
        period_label = f"{y:04d}-{m:02d}-{d:02d}"
        start_sql = f"TIMESTAMP WITH TIME ZONE '{y:04d}-{m:02d}-{d:02d} 00:00:00+00'"
        end_sql   = f"{start_sql} + interval '1 day'"
        rest_args = args[1:]
    elif mmon:
        y,m = map(int, mmon.groups())
        period_label = f"{y:04d}-{m:02d}"
        start_sql = f"TIMESTAMP WITH TIME ZONE '{y:04d}-{m:02d}-01 00:00:00+00'"
        end_sql   = f"{start_sql} + interval '1 month'"
        rest_args = args[1:]
    else:
        # периода нет в начале — считаем, что period=day, а все args — дальше
        period_label = "сегодня"
        start_sql = trunc("day")
        end_sql   = f"{trunc('day')} + interval '1 day'"
        rest_args = args

    # фильтры по мастеру
    master_tg = None
    master_id = None
    for a in rest_args:
        a = a.strip()
        if a.startswith("master:"):
            try:
                master_tg = int(a.split(":",1)[1])
            except Exception:
                pass
        elif a.startswith("master_id:"):
            try:
                master_id = int(a.split(":",1)[1])
            except Exception:
                pass

    where_master = "TRUE"
    params = []
    if master_id is not None:
        where_master = "o.master_id = $1"
        params.append(master_id)
    elif master_tg is not None:
        where_master = "o.master_id = (SELECT id FROM staff WHERE tg_user_id = $1)"
        params.append(master_tg)

    # ограничение на список последних заказов
    limit = 20

    async with pool.acquire() as conn:
        # итоги по периоду
        totals = await conn.fetchrow(
            f"""
            SELECT
              COUNT(*)                               AS orders_cnt,
              COALESCE(SUM(o.amount_cash),  0)::numeric(12,2) AS money_cash,
              COALESCE(SUM(CASE WHEN o.payment_method='Подарочный сертификат' THEN o.amount_total ELSE 0 END), 0)::numeric(12,2) AS gift_total
            FROM orders o
            WHERE o.created_at >= {start_sql}
              AND o.created_at <  {end_sql}
              AND {where_master};
            """,
            *params
        )

        # последние N заказов
        rows = await conn.fetch(
            f"""
            SELECT
              o.id,
              o.created_at AT TIME ZONE 'UTC' AS created_utc,
              COALESCE(c.full_name,'—') AS client_name,
              s.tg_user_id               AS master_tg,
              o.payment_method,
              o.amount_cash::numeric(12,2)  AS cash,
              o.amount_total::numeric(12,2) AS total
            FROM orders o
            LEFT JOIN clients c ON c.id = o.client_id
            LEFT JOIN staff   s ON s.id = o.master_id
            WHERE o.created_at >= {start_sql}
              AND o.created_at <  {end_sql}
              AND {where_master}
            ORDER BY o.created_at DESC
            LIMIT {limit};
            """,
            *params
        )

    cnt   = totals["orders_cnt"] or 0
    money = totals["money_cash"] or 0
    gift  = totals["gift_total"] or 0

    header = [f"Заказы за {period_label}:"]
    if master_id is not None:
        header.append(f"(фильтр: master_id={master_id})")
    elif master_tg is not None:
        header.append(f"(фильтр: master={master_tg})")
    header.append(f"Всего: {cnt} | Деньги: {money}₽")
    if gift and gift > 0:
        header.append(f"(сертификатами: {gift}₽)")

    lines = [" ".join(header)]
    if rows:
        lines.append("\nПоследние заказы:")
        for r in rows:
            dt = r["created_utc"].strftime("%Y-%m-%d %H:%M")
            lines.append(
                f"#{r['id']} | {dt} | {r['client_name']} | m:{r['master_tg']} | {r['payment_method']} | {r['cash']}₽/{r['total']}₽"
            )
    else:
        lines.append("Данных нет.")

    await msg.answer("\n".join(lines))


@dp.message(Command("reports"))
async def reports_start(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "view_orders_reports"):
        return await msg.answer("Только для администраторов.")
    await msg.answer("Выберите отчёт:", reply_markup=reports_root_kb())
    await state.set_state(ReportsFSM.waiting_root)

@dp.message(ReportsFSM.waiting_root, F.text.casefold() == "мастер/заказы/оплаты")
async def rep_master_orders_entry(msg: Message, state: FSMContext):
    async with pool.acquire() as conn:
        masters = await conn.fetch(
            "SELECT id, tg_user_id, coalesce(first_name,'') AS fn, coalesce(last_name,'') AS ln "
            "FROM staff WHERE role IN ('master','admin') AND is_active ORDER BY id LIMIT 10"
        )
    if masters:
        kb = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text=f"{r['fn']} {r['ln']} | tg:{r['tg_user_id']}")] for r in masters] +
                     [[KeyboardButton(text="Ввести tg id вручную")],
                      [KeyboardButton(text="Назад"), KeyboardButton(text="Отмена")]],
            resize_keyboard=True,
            one_time_keyboard=True,
        )
        await msg.answer("Выберите мастера или введите tg id:", reply_markup=kb)
    else:
        await msg.answer(
            "Введите tg id мастера:",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="Назад"), KeyboardButton(text="Отмена")]],
                resize_keyboard=True,
                one_time_keyboard=True,
            )
        )
    await state.update_data(report_kind="master_orders")
    await state.set_state(ReportsFSM.waiting_pick_master)


@dp.message(ReportsFSM.waiting_root, F.text.casefold() == "мастер/зарплата")
async def rep_master_salary_entry(msg: Message, state: FSMContext):
    async with pool.acquire() as conn:
        masters = await conn.fetch(
            "SELECT id, tg_user_id, COALESCE(first_name,'') AS fn, COALESCE(last_name,'') AS ln "
            "FROM staff WHERE role IN ('master','admin') AND is_active ORDER BY id LIMIT 10"
        )
    if masters:
        kb = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text=f"{r['fn']} {r['ln']} | tg:{r['tg_user_id']}")] for r in masters] +
                     [[KeyboardButton(text="Ввести tg id вручную")],
                      [KeyboardButton(text="Назад"), KeyboardButton(text="Отмена")]],
            resize_keyboard=True,
            one_time_keyboard=True,
        )
        await msg.answer("Выберите мастера или введите tg id:", reply_markup=kb)
    else:
        kb = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Назад"), KeyboardButton(text="Отмена")]],
            resize_keyboard=True,
            one_time_keyboard=True,
        )
        await msg.answer("Введите tg id мастера:", reply_markup=kb)
    await state.update_data(report_kind="master_salary")
    await state.set_state(ReportsFSM.waiting_pick_master)


@dp.message(ReportsFSM.waiting_root, F.text.in_({"Касса", "Прибыль"}))
async def reports_pick_period(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "view_orders_reports"):
        return await msg.answer("Только для администраторов.")
    kind = msg.text
    await state.update_data(report_kind=kind)
    await state.set_state(ReportsFSM.waiting_pick_period)
    await msg.answer(f"{kind}: выбери период.", reply_markup=reports_period_kb())


# Stub: "Типы оплат" → пока только выбор периода
@dp.message(ReportsFSM.waiting_root, F.text == "Типы оплат")
async def reports_payment_methods(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "view_payments_by_method"):
        return await msg.answer("Только для администраторов.")
    await state.update_data(report_kind="Типы оплат")
    await state.set_state(ReportsFSM.waiting_pick_period)
    await msg.answer("Типы оплат: выбери период.", reply_markup=reports_period_kb())


@dp.message(ReportsFSM.waiting_pick_period, F.text == "Назад")
async def rep_period_back(msg: Message, state: FSMContext):
    await state.set_state(ReportsFSM.waiting_root)
    await msg.answer("Отчёты: выбери раздел.", reply_markup=reports_root_kb())


@dp.message(ReportsFSM.waiting_pick_period, F.text == "Выйти")
async def reports_exit_to_admin(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer("Меню администратора:", reply_markup=admin_root_kb())


@dp.message(ReportsFSM.waiting_pick_master, F.text.casefold() == "назад")
async def rep_master_back(msg: Message, state: FSMContext):
    await state.set_state(ReportsFSM.waiting_root)
    return await msg.answer("Выберите отчёт:", reply_markup=reports_root_kb())


@dp.message(ReportsFSM.waiting_root, F.text == "Назад")
async def reports_root_back(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer("Меню администратора:", reply_markup=admin_root_kb())


@dp.message(ReportsFSM.waiting_root, F.text == "Отмена")
@dp.message(ReportsFSM.waiting_pick_period, F.text == "Отмена")
async def reports_cancel(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer("Отменено. Возврат в меню администратора.", reply_markup=admin_root_kb())


@dp.message(AdminMenuFSM.root, F.text == "Отчёты")
async def adm_root_reports(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "view_orders_reports"):
        return await msg.answer("Только для администраторов.")
    await state.set_state(ReportsFSM.waiting_root)
    await msg.answer("Отчёты: выбери раздел.", reply_markup=reports_root_kb())


@dp.message(AdminMenuFSM.root, F.text.casefold() == "касса")
async def adm_root_cash(msg: Message, state: FSMContext):
    await msg.answer("Период для кассы: /cash day | /cash month | /cash year", reply_markup=admin_root_kb())


@dp.message(AdminMenuFSM.root, F.text.casefold() == "прибыль")
async def adm_root_profit(msg: Message, state: FSMContext):
    await msg.answer("Период для прибыли: /profit day | /profit month | /profit year", reply_markup=admin_root_kb())


@dp.message(AdminMenuFSM.root, F.text.casefold() == "tx последние")
async def adm_root_tx_last(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "view_cash_reports"):
        return await msg.answer("Только для администраторов.")
    await msg.answer("Выберите, сколько показать:", reply_markup=tx_last_kb())


@dp.message(AdminMenuFSM.root, F.text.casefold() == "назад")
async def admin_root_back(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "view_orders_reports"):
        return await msg.answer("Только для администраторов.")
    await state.set_state(AdminMenuFSM.root)
    await msg.answer("Меню администратора:", reply_markup=admin_root_kb())


@dp.message(AdminMenuFSM.root, F.text.casefold() == "выйти")
async def admin_root_exit(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "view_orders_reports"):
        return await msg.answer("Только для администраторов.")
    await state.set_state(AdminMenuFSM.root)
    await msg.answer("Меню администратора:", reply_markup=admin_root_kb())


@dp.message(AdminMenuFSM.root, F.text.casefold() == "кто я")
async def adm_root_whoami(msg: Message, state: FSMContext):
    return await whoami(msg)


@dp.message(AdminMenuFSM.root, F.text.casefold() == "приход")
async def income_wizard_start(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "record_cashflows"):
        return await msg.answer("Только для администраторов.")
    await state.set_state(IncomeFSM.waiting_method)
    await msg.answer("Выберите тип оплаты:", reply_markup=admin_payment_method_kb())


@dp.message(IncomeFSM.waiting_method)
async def income_wizard_pick_method(msg: Message, state: FSMContext):
    if (msg.text or "").strip().casefold() == "отмена":
        await state.clear()
        return await msg.answer("Ок, отменено.", reply_markup=ReplyKeyboardRemove())
    method = norm_pay_method_py(msg.text)
    await state.update_data(method=method)
    await state.set_state(IncomeFSM.waiting_amount)
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Отмена")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await msg.answer("Введите сумму прихода:", reply_markup=kb)


@dp.message(IncomeFSM.waiting_amount)
async def income_wizard_amount(msg: Message, state: FSMContext):
    txt = (msg.text or "").strip().replace(" ", "").replace(",", ".")
    if txt.casefold() == "отмена":
        await state.clear()
        return await msg.answer("Ок, отменено.", reply_markup=ReplyKeyboardRemove())
    try:
        amount = Decimal(txt)
    except Exception:
        return await msg.answer("Сумма должна быть числом. Повторите ввод или «Отмена».")
    if amount <= 0:
        return await msg.answer("Сумма должна быть > 0. Повторите ввод или «Отмена».")
    await state.update_data(amount=str(amount))
    await state.set_state(IncomeFSM.waiting_comment)
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Без комментария")], [KeyboardButton(text="Отмена")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await msg.answer("Комментарий? (введите текст или нажмите «Без комментария»)", reply_markup=kb)


@dp.message(IncomeFSM.waiting_comment)
async def income_wizard_comment(msg: Message, state: FSMContext):
    txt = (msg.text or "").strip()
    if txt.casefold() == "отмена":
        await state.clear()
        return await msg.answer("Ок, отменено.", reply_markup=ReplyKeyboardRemove())
    if txt.casefold() == "без комментария" or not txt:
        txt = "поступление денег в кассу"
    data = await state.get_data()
    method = data.get("method")
    amount = Decimal(data.get("amount"))
    async with pool.acquire() as conn:
        tx = await _record_income(conn, method, amount, txt)
    when = tx["happened_at"].strftime("%Y-%m-%d %H:%M")
    await msg.answer(
        f"Приход №{tx['id']}: {amount}₽ | {method} — {when}\nКомментарий: {txt}",
        reply_markup=ReplyKeyboardRemove(),
    )
    await state.clear()


@dp.message(AdminMenuFSM.root, F.text.casefold() == "расход")
async def expense_wizard_start(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "record_cashflows"):
        return await msg.answer("Только для администраторов.")
    await state.set_state(ExpenseFSM.waiting_amount)
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Отмена")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await msg.answer("Введите сумму расхода:", reply_markup=kb)


@dp.message(ExpenseFSM.waiting_amount)
async def expense_wizard_amount(msg: Message, state: FSMContext):
    txt = (msg.text or "").strip().replace(" ", "").replace(",", ".")
    if txt.casefold() == "отмена":
        await state.clear()
        return await msg.answer("Ок, отменено.", reply_markup=ReplyKeyboardRemove())
    try:
        amount = Decimal(txt)
    except Exception:
        return await msg.answer("Сумма должна быть числом. Повторите ввод или «Отмена».")
    if amount <= 0:
        return await msg.answer("Сумма должна быть > 0. Повторите ввод или «Отмена».")
    await state.update_data(amount=str(amount))
    await state.set_state(ExpenseFSM.waiting_comment)
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Без комментария")], [KeyboardButton(text="Отмена")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await msg.answer(
        "Комментарий? (введите текст или нажмите «Без комментария»)",
        reply_markup=kb,
    )


@dp.message(ExpenseFSM.waiting_comment)
async def expense_wizard_comment(msg: Message, state: FSMContext):
    txt = (msg.text or "").strip()
    if txt.casefold() == "отмена":
        await state.clear()
        return await msg.answer("Ок, отменено.", reply_markup=ReplyKeyboardRemove())
    if txt.casefold() == "без комментария":
        txt = "Расход"
    data = await state.get_data()
    amount = Decimal(data.get("amount"))
    async with pool.acquire() as conn:
        tx = await _record_expense(conn, amount, txt, method="прочее")
    when = tx["happened_at"].strftime("%Y-%m-%d %H:%M")
    await msg.answer(
        f"Расход №{tx['id']}: {amount}₽ — {when}\nКомментарий: {txt}",
        reply_markup=ReplyKeyboardRemove(),
    )
    await state.clear()


@dp.message(AdminMenuFSM.masters, F.text.casefold() == "назад")
async def masters_back(msg: Message, state: FSMContext):
    await state.set_state(AdminMenuFSM.root)
    await msg.answer("Меню администратора:", reply_markup=admin_root_kb())


@dp.message(AdminMenuFSM.masters, F.text.casefold() == "добавить мастера")
async def masters_add(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer("Формат: /add_master <tg_user_id>")


@dp.message(AdminMenuFSM.masters, F.text.casefold() == "список мастеров")
async def masters_list(msg: Message, state: FSMContext):
    await state.clear()
    return await list_masters(msg)


@dp.message(AdminMenuFSM.masters, F.text.casefold() == "деактивировать мастера")
async def masters_remove(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer("Формат: /remove_master <tg_user_id>")


@dp.message(ReportsFSM.waiting_pick_master, ~F.text.startswith("/"))
async def rep_master_pick(msg: Message, state: FSMContext):
    txt = (msg.text or "").strip()
    m = re.search(r"tg:(\d+)", txt)
    tg_id = None
    if m:
        tg_id = int(m.group(1))
    elif txt.isdigit():
        tg_id = int(txt)
    if not tg_id:
        return await msg.answer("Укажи tg id мастера (число).")
    await state.update_data(master_tg=tg_id)
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="день"),   KeyboardButton(text="неделя")],
            [KeyboardButton(text="месяц"), KeyboardButton(text="год")],
            [KeyboardButton(text="Назад"), KeyboardButton(text="Отмена")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await msg.answer("Выберите период:", reply_markup=kb)
    await state.set_state(ReportsFSM.waiting_pick_period)


@dp.message(ReportsFSM.waiting_pick_period, ~F.text.startswith("/"))
async def rep_master_period(msg: Message, state: FSMContext):
    period = (msg.text or "").strip().lower()
    if period not in ("день", "неделя", "месяц", "год"):
        return await msg.answer("Выберите один из вариантов: день / неделя / месяц / год")

    if period == "день":
        start_sql = "date_trunc('day', NOW())"
        end_sql = "date_trunc('day', NOW()) + interval '1 day'"
        label = "за сегодня"
    elif period == "неделя":
        start_sql = "date_trunc('week', NOW())"
        end_sql = "date_trunc('week', NOW()) + interval '1 week'"
        label = "за неделю"
    elif period == "месяц":
        start_sql = "date_trunc('month', NOW())"
        end_sql = "date_trunc('month', NOW()) + interval '1 month'"
        label = "за месяц"
    else:
        start_sql = "date_trunc('year', NOW())"
        end_sql = "date_trunc('year', NOW()) + interval '1 year'"
        label = "за год"

    data = await state.get_data()
    report_kind = data.get("report_kind", "master_orders")

    if report_kind == "master_salary":
        tg_id = int(data["master_tg"])
        async with pool.acquire() as conn:
            mid = await conn.fetchval("SELECT id FROM staff WHERE tg_user_id=$1", tg_id)
            if not mid:
                await state.clear()
                return await msg.answer("Мастер с таким tg id не найден.")

            rec = await conn.fetchrow(
                f"""
                WITH ord AS (
                  SELECT o.id
                  FROM orders o
                  WHERE o.master_id = $1
                    AND o.created_at >= {start_sql}
                    AND o.created_at <  {end_sql}
                )
                SELECT
                  COUNT(*)                                   AS orders,
                  COALESCE(SUM(pi.base_pay),   0)::numeric(12,2) AS base_pay,
                  COALESCE(SUM(pi.fuel_pay),   0)::numeric(12,2) AS fuel_pay,
                  COALESCE(SUM(pi.upsell_pay), 0)::numeric(12,2) AS upsell_pay,
                  COALESCE(SUM(pi.total_pay),  0)::numeric(12,2) AS total_pay
                FROM payroll_items pi
                JOIN ord ON ord.id = pi.order_id
                WHERE pi.master_id = $1;
                """,
                mid,
            )
            fio = await conn.fetchval(
                "SELECT trim(COALESCE(first_name,'')||' '||COALESCE(last_name,'')) FROM staff WHERE id=$1",
                mid,
            )

        lines = [
            f"Зарплата мастера: {fio or '—'} (tg:{tg_id}) — {label}",
            f"Заказов: {rec['orders'] or 0}",
            f"База: {rec['base_pay'] or 0}₽",
            f"Бензин: {rec['fuel_pay'] or 0}₽",
        ]
        if (rec['upsell_pay'] or 0) > 0:
            lines.append(f"Доп. услуги: {rec['upsell_pay']}₽")
        lines.append(f"Итого к выплате: {rec['total_pay'] or 0}₽")

        await msg.answer("\n".join(lines), reply_markup=ReplyKeyboardRemove())
        await state.clear()
        return

    if report_kind == "paytypes":
        async with pool.acquire() as conn:
            rec = await conn.fetchrow(
                f"""
                SELECT
                  COALESCE(SUM(CASE WHEN payment_method='Наличные'     THEN amount_cash  ELSE 0 END),0)::numeric(12,2) AS s_cash,
                  COALESCE(SUM(CASE WHEN payment_method='Карта Женя'   THEN amount_cash  ELSE 0 END),0)::numeric(12,2) AS s_card_jenya,
                  COALESCE(SUM(CASE WHEN payment_method='Карта Дима'   THEN amount_cash  ELSE 0 END),0)::numeric(12,2) AS s_card_dima,
                  COALESCE(SUM(CASE WHEN payment_method='р/с'          THEN amount_cash  ELSE 0 END),0)::numeric(12,2) AS s_rs,
                  COALESCE(SUM(CASE WHEN payment_method='Подарочный сертификат' THEN amount_total ELSE 0 END),0)::numeric(12,2) AS s_gift_total,
                  COALESCE(SUM(CASE WHEN payment_method='Наличные'     THEN 1 ELSE 0 END),0) AS c_cash,
                  COALESCE(SUM(CASE WHEN payment_method='Карта Женя'   THEN 1 ELSE 0 END),0) AS c_card_jenya,
                  COALESCE(SUM(CASE WHEN payment_method='Карта Дима'   THEN 1 ELSE 0 END),0) AS c_card_dima,
                  COALESCE(SUM(CASE WHEN payment_method='р/с'          THEN 1 ELSE 0 END),0) AS c_rs,
                  COALESCE(SUM(CASE WHEN payment_method='Подарочный сертификат' THEN 1 ELSE 0 END),0) AS c_gift
                FROM orders
                WHERE created_at >= {start_sql} AND created_at < {end_sql};
                """
            )

        lines = [f"Типы оплат — {label}:"]

        total_money = (rec['s_cash'] or 0) + (rec['s_card_jenya'] or 0) + (rec['s_card_dima'] or 0) + (rec['s_rs'] or 0)
        if rec['c_cash'] > 0:
            lines.append(f"Наличные: {rec['s_cash']}₽ ({rec['c_cash']})")
        if rec['c_card_jenya'] > 0:
            lines.append(f"Карта Женя: {rec['s_card_jenya']}₽ ({rec['c_card_jenya']})")
        if rec['c_card_dima'] > 0:
            lines.append(f"Карта Дима: {rec['s_card_dima']}₽ ({rec['c_card_dima']})")
        if rec['c_rs'] > 0:
            lines.append(f"р/с: {rec['s_rs']}₽ ({rec['c_rs']})")
        if rec['c_gift'] > 0:
            lines.append(f"Подарочный сертификат: {rec['s_gift_total']}₽ ({rec['c_gift']})")

        lines.append(f"Итого денег: {total_money}₽")

        await msg.answer("\n".join(lines), reply_markup=ReplyKeyboardRemove())
        await state.clear()
        return

    tg_id = int(data["master_tg"])

    async with pool.acquire() as conn:
        mid = await conn.fetchval("SELECT id FROM staff WHERE tg_user_id=$1", tg_id)
        if not mid:
            await state.clear()
            return await msg.answer("Мастер с таким tg id не найден.")

        rec = await conn.fetchrow(
            f"""
            WITH scope AS (
              SELECT o.*
              FROM orders o
              WHERE o.master_id = $1
                AND o.created_at >= {start_sql}
                AND o.created_at <  {end_sql}
            ),
            w AS (
              SELECT COALESCE(SUM(c.amount),0)::numeric(12,2) AS withdrawn
              FROM cashbook_entries c
              WHERE c.kind='expense' AND c.method='Изъятие' AND c.master_id=$1
                AND c.happened_at >= {start_sql} AND c.happened_at < {end_sql}
            )
            SELECT
              COUNT(*) AS cnt,
              COALESCE(SUM(CASE WHEN payment_method='Наличные'              THEN amount_cash  ELSE 0 END),0)::numeric(12,2) AS s_cash,
              COALESCE(SUM(CASE WHEN payment_method='Карта Женя'            THEN amount_cash  ELSE 0 END),0)::numeric(12,2) AS s_card_jenya,
              COALESCE(SUM(CASE WHEN payment_method='Карта Дима'            THEN amount_cash  ELSE 0 END),0)::numeric(12,2) AS s_card_dima,
              COALESCE(SUM(CASE WHEN payment_method='р/с'                   THEN amount_cash  ELSE 0 END),0)::numeric(12,2) AS s_rs,
              COALESCE(SUM(CASE WHEN payment_method='Подарочный сертификат' THEN amount_total ELSE 0 END),0)::numeric(12,2) AS s_gift_total,
              (SELECT withdrawn FROM w) AS withdrawn
            FROM scope;
            """,
            mid,
        )

        fio = await conn.fetchval(
            "SELECT trim(coalesce(first_name,'')||' '||coalesce(last_name,'')) FROM staff WHERE id=$1",
            mid,
        )

    lines = [
        f"Мастер: {fio or '—'} ({tg_id}) — {label}",
        f"Заказов выполнено: {rec['cnt']}"
    ]
    if rec["s_cash"] > 0:
        lines.append(f"Оплачено наличными: {rec['s_cash']}₽")
    if rec["s_card_jenya"] > 0:
        lines.append(f"Оплачено Карта Женя: {rec['s_card_jenya']}₽")
    if rec["s_card_dima"] > 0:
        lines.append(f"Оплачено Карта Дима: {rec['s_card_dima']}₽")
    if rec["s_rs"] > 0:
        lines.append(f"Оплачено р/с: {rec['s_rs']}₽")
    if rec["s_gift_total"] > 0:
        lines.append(f"Оплачено сертификатом: {rec['s_gift_total']}₽")

    withdrawn = rec["withdrawn"] or Decimal(0)
    on_hand = (rec["s_cash"] or Decimal(0)) - withdrawn
    if withdrawn and withdrawn > 0:
        lines.append(f"Изъято у мастера: {withdrawn}₽")
    lines.append(f"Итого на руках наличных: {on_hand}₽")

    await msg.answer("\n".join(lines), reply_markup=ReplyKeyboardRemove())
    await state.clear()

# ===== Leads import (admin) =====
@dp.message(Command("import_leads_dryrun"))
async def import_leads_dryrun(msg: Message):
    # only admins/superadmins
    if not await has_permission(msg.from_user.id, "import_leads"):
        return await msg.answer("Только для администраторов.")

    async with pool.acquire() as conn:
        # ensure helper functions and staging table exist
        await conn.execute(
            """
            CREATE OR REPLACE FUNCTION norm_phone_ru(p text) RETURNS text AS $$
            DECLARE s text := COALESCE(p, '');
            DECLARE m text;  -- first valid subsequence of digits
            DECLARE ch text;
            DECLARE d text := '';
            DECLARE first text := NULL;
            BEGIN
              -- scan left-to-right, collect the first valid phone subsequence only
              FOR i IN 1..char_length(s) LOOP
                ch := substr(s, i, 1);
                IF ch ~ '\\d' THEN
                  IF first IS NULL THEN
                    IF ch IN ('7','8','9') THEN
                      first := ch;
                      d := d || ch;
                    END IF;
                  ELSE
                    d := d || ch;
                  END IF;
                  IF first IN ('7','8') AND length(d) = 11 THEN EXIT; END IF;
                  IF first = '9' AND length(d) = 10 THEN EXIT; END IF;
                END IF;
              END LOOP;

              IF d = '' THEN
                RETURN NULL;
              END IF;

              IF length(d)=10 AND d LIKE '9%' THEN
                RETURN '+7' || d;
              ELSIF length(d)=11 AND d LIKE '8%' THEN
                RETURN '+7' || substr(d,2);
              ELSIF length(d)=11 AND d LIKE '7%' THEN
                RETURN '+' || d;
              ELSE
                RETURN NULL;
              END IF;
            END $$ LANGUAGE plpgsql IMMUTABLE;
            """
        )
        await conn.execute(
            """
            CREATE OR REPLACE FUNCTION is_bad_name(name text) RETURNS boolean AS $$
            DECLARE low text := lower(coalesce(name,''));
            DECLARE digits text := regexp_replace(low,'[^0-9]','','g');
            BEGIN
              IF name IS NULL OR name = '' THEN RETURN FALSE; END IF;
              IF low ~ '(^|\\s)пропущенн' THEN RETURN TRUE; END IF;
              IF low ~ '(^|\\s)входящ' THEN RETURN TRUE; END IF;
              IF low ~ 'гугл\\s*карты' OR low ~ 'google\\s*maps' THEN RETURN TRUE; END IF;
              IF low ~ 'яндекс' OR low ~ 'сарафан' THEN RETURN TRUE; END IF;
              IF length(digits) BETWEEN 10 AND 11 THEN RETURN TRUE; END IF;
              RETURN FALSE;
            END $$ LANGUAGE plpgsql IMMUTABLE;
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS clients_raw (
              full_name     text,
              phone         text,
              bonus_balance integer,
              birthday      date,
              address       text
            );
            """
        )
        # dry-run report (no changes), assumes CSV is already loaded into clients_raw
        rec = await conn.fetchrow(
            """
            WITH
            cleaned AS (
              SELECT NULLIF(trim(full_name),'') AS full_name,
                     norm_phone_ru(phone)       AS phone,
                     COALESCE(bonus_balance,0)  AS bonus_balance,
                     birthday,
                     NULLIF(trim(address),'')   AS address,
                     row_number() OVER (ORDER BY ctid) AS src_pos
              FROM clients_raw
            ),
            valid_no_dedup AS (
              SELECT COUNT(*) AS cnt
              FROM cleaned
              WHERE phone IS NOT NULL
            ),
            dedup AS (
              SELECT DISTINCT ON (phone) full_name, phone, bonus_balance, birthday, address
              FROM cleaned
              WHERE phone IS NOT NULL
              ORDER BY phone, (address IS NULL), src_pos
            ),
            src AS (SELECT COUNT(*) AS total FROM clients_raw),
            valid_distinct AS (SELECT COUNT(*) AS cnt FROM dedup),
            new AS (
              SELECT COUNT(*) AS inserted FROM dedup d
              LEFT JOIN clients c ON c.phone=d.phone
              WHERE c.id IS NULL
            ),
            upd AS (
              SELECT COUNT(*) AS updated FROM dedup d
              JOIN clients c ON c.phone=d.phone
              WHERE c.status <> 'client'
            ),
            skp AS (
              SELECT COUNT(*) AS skipped_existing_clients FROM dedup d
              JOIN clients c ON c.phone=d.phone
              WHERE c.status='client'
            )
            SELECT 
              (SELECT total FROM src)                        AS src_rows,
              (SELECT cnt FROM valid_no_dedup)               AS valid_phones_total,
              (SELECT cnt FROM valid_distinct)               AS valid_phones_distinct,
              (SELECT inserted FROM new)                     AS would_insert,
              (SELECT updated FROM upd)                      AS would_update,
              (SELECT skipped_existing_clients FROM skp)     AS would_skip_clients;
            """
        )
    text = (
        "Проверка загрузки (ничего не меняем):\n"
        f"• Исходных строк — {rec['src_rows']} (строк в файле)\n"
        f"• Телефонов валидно — {rec['valid_phones_total']} (подходит для загрузки)\n"
        f"• Уникальных телефонов — {rec['valid_phones_distinct']} (уникальные записи)\n"
        f"• Будет добавлено (новых) — {rec['would_insert']}\n"
        f"• Будет обновлено (текущих не-клиентов) — {rec['would_update']}\n"
        f"• Не будет загружено (уже клиенты) — {rec['would_skip_clients']}\n"
        "\nЕсли всё ок: загрузите CSV в clients_raw и выполните /import_leads, чтобы применить изменения."
    )
    await msg.answer(text)


@dp.message(Command("import_leads"))
async def import_leads(msg: Message):
    # only admins/superadmins
    if not await has_permission(msg.from_user.id, "import_leads"):
        return await msg.answer("Только для администраторов.")

    async with pool.acquire() as conn:
        async with conn.transaction():
            # ensure helpers exist (same as in dryrun)
            await conn.execute(
                """
                CREATE OR REPLACE FUNCTION norm_phone_ru(p text) RETURNS text AS $$
                DECLARE s text := COALESCE(p, '');
                DECLARE m text;  -- first valid subsequence of digits
                DECLARE ch text;
                DECLARE d text := '';
                DECLARE first text := NULL;
                BEGIN
                  -- scan left-to-right, collect the first valid phone subsequence only
                  FOR i IN 1..char_length(s) LOOP
                    ch := substr(s, i, 1);
                    IF ch ~ '\\d' THEN
                      IF first IS NULL THEN
                        IF ch IN ('7','8','9') THEN
                          first := ch;
                          d := d || ch;
                        END IF;
                      ELSE
                        d := d || ch;
                      END IF;
                      IF first IN ('7','8') AND length(d) = 11 THEN EXIT; END IF;
                      IF first = '9' AND length(d) = 10 THEN EXIT; END IF;
                    END IF;
                  END LOOP;

                  IF d = '' THEN
                    RETURN NULL;
                  END IF;

                  IF length(d)=10 AND d LIKE '9%' THEN
                    RETURN '+7' || d;
                  ELSIF length(d)=11 AND d LIKE '8%' THEN
                    RETURN '+7' || substr(d,2);
                  ELSIF length(d)=11 AND d LIKE '7%' THEN
                    RETURN '+' || d;
                  ELSE
                    RETURN NULL;
                  END IF;
                END $$ LANGUAGE plpgsql IMMUTABLE;
                """
            )
            await conn.execute(
                """
                CREATE OR REPLACE FUNCTION is_bad_name(name text) RETURNS boolean AS $$
                DECLARE low text := lower(coalesce(name,''));
                DECLARE digits text := regexp_replace(low,'[^0-9]','','g');
                BEGIN
                  IF name IS NULL OR name = '' THEN RETURN FALSE; END IF;
                  IF low ~ '(^|\\s)пропущенн' THEN RETURN TRUE; END IF;
                  IF low ~ '(^|\\s)входящ' THEN RETURN TRUE; END IF;
                  IF low ~ 'гугл\\s*карты' OR low ~ 'google\\s*maps' THEN RETURN TRUE; END IF;
                  IF low ~ 'яндекс' OR low ~ 'сарафан' THEN RETURN TRUE; END IF;
                  IF length(digits) BETWEEN 10 AND 11 THEN RETURN TRUE; END IF;
                  RETURN FALSE;
                END $$ LANGUAGE plpgsql IMMUTABLE;
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS clients_raw (
                  full_name     text,
                  phone         text,
                  bonus_balance integer,
                  birthday      date,
                  address       text
                );
                """
            )
# Prepare cleaned and deduplicated datasets
            await conn.execute("""
                CREATE TEMP TABLE tmp_cleaned AS
                SELECT NULLIF(trim(full_name),'') AS full_name,
                       norm_phone_ru(phone)       AS phone,
                       COALESCE(bonus_balance,0)  AS bonus_balance,
                       birthday,
                       NULLIF(trim(address),'')   AS address,
                       row_number() OVER (ORDER BY ctid) AS src_pos
                FROM clients_raw;

                CREATE TEMP TABLE tmp_dedup AS
                SELECT DISTINCT ON (phone) full_name, phone, bonus_balance, birthday, address
                FROM tmp_cleaned
                WHERE phone IS NOT NULL
                ORDER BY phone, (address IS NULL), src_pos;
            """)

            # Pre-change stats (to report skipped clients and valid counts)
            pre = await conn.fetchrow("""
                WITH src AS (SELECT COUNT(*) AS total FROM clients_raw),
                     valid_no_dedup AS (SELECT COUNT(*) AS cnt FROM tmp_cleaned WHERE phone IS NOT NULL),
                     valid_distinct AS (SELECT COUNT(*) AS cnt FROM tmp_dedup),
                     would_insert AS (
                       SELECT COUNT(*) AS c FROM tmp_dedup d
                       LEFT JOIN clients c ON c.phone=d.phone
                       WHERE c.id IS NULL
                     ),
                     would_update AS (
                       SELECT COUNT(*) AS c FROM tmp_dedup d
                       JOIN clients c ON c.phone=d.phone
                       WHERE c.status <> 'client'
                     ),
                     would_skip AS (
                       SELECT COUNT(*) AS c FROM tmp_dedup d
                       JOIN clients c ON c.phone=d.phone
                       WHERE c.status = 'client'
                     )
                SELECT (SELECT total FROM src)              AS src_rows,
                       (SELECT cnt FROM valid_no_dedup)     AS valid_phones_total,
                       (SELECT cnt FROM valid_distinct)     AS valid_phones_distinct,
                       (SELECT c FROM would_insert)         AS would_insert,
                       (SELECT c FROM would_update)         AS would_update,
                       (SELECT c FROM would_skip)           AS would_skip_clients;
            """)

            # Real INSERTs with RETURNING to count actually inserted
            inserted_rows = await conn.fetch("""
                INSERT INTO clients(full_name, phone, bonus_balance, birthday, status)
                SELECT
                  d.full_name,
                  d.phone,
                  d.bonus_balance,
                  d.birthday,
                  CASE
                    WHEN d.address IS NOT NULL THEN 'client'
                    ELSE 'lead'
                  END
                FROM tmp_dedup d
                LEFT JOIN clients c ON c.phone=d.phone
                WHERE c.id IS NULL
                RETURNING phone;
            """)

            # Real UPDATEs for non-clients (do NOT touch status)
            updated_rows = await conn.fetch("""
                UPDATE clients c
                SET
                  full_name     = COALESCE(d.full_name, c.full_name),
                  bonus_balance = COALESCE(d.bonus_balance, c.bonus_balance),
                  birthday      = COALESCE(d.birthday, c.birthday)
                FROM tmp_dedup d
                WHERE c.phone = d.phone
                  AND c.status <> 'client'
                RETURNING c.phone;
            """)

            inserted_count = len(inserted_rows)
            updated_count  = len(updated_rows)

        text = (
            "Импорт лидов выполнен:\n"
            f"Исходных строк: {pre['src_rows']}\n"
            f"Телефонов валидно (всего): {pre['valid_phones_total']}\n"
            f"Телефонов валидно (уникальных): {pre['valid_phones_distinct']}\n"
            f"Добавлено (новых): {inserted_count}\n"
            f"Обновлено (не-клиенты): {updated_count}\n"
            f"Пропущено (уже clients): {pre['would_skip_clients']}\n"
            "\nНапоминание: статус автоматически станет 'client' после первого заказа."
        )
        await msg.answer(text)




@dp.message(Command("db_apply_cash_trigger"))
async def db_apply_cash_trigger(msg: Message):
    # доступ только для суперадмина
    async with pool.acquire() as conn:
        role = await get_user_role(conn, msg.from_user.id)
    if role != 'superadmin':
        return await msg.answer("Эта команда доступна только суперадмину.")
    sql = """
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='cashbook_entries' AND column_name='master_id'
      ) THEN
        ALTER TABLE cashbook_entries ADD COLUMN master_id integer REFERENCES staff(id);
        CREATE INDEX IF NOT EXISTS ix_cashbook_master ON cashbook_entries(master_id);
      END IF;
    END$$;

    CREATE OR REPLACE FUNCTION orders_to_cashbook_ai()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    BEGIN
      IF NEW.payment_method = 'Подарочный сертификат' THEN
        INSERT INTO cashbook_entries(kind, method, amount, comment, order_id, master_id, happened_at)
        VALUES ('income', NEW.payment_method, 0, 'Поступление по заказу (сертификат)', NEW.id, NEW.master_id, NEW.created_at);
        RETURN NEW;
      END IF;

      INSERT INTO cashbook_entries(kind, method, amount, comment, order_id, master_id, happened_at)
      VALUES ('income', NEW.payment_method, COALESCE(NEW.amount_cash,0), 'Поступление по заказу', NEW.id, NEW.master_id, NEW.created_at);
      RETURN NEW;
    END$$;

    DROP TRIGGER IF EXISTS trg_orders_to_cashbook ON orders;
    CREATE TRIGGER trg_orders_to_cashbook
    AFTER INSERT ON orders
    FOR EACH ROW
    EXECUTE FUNCTION orders_to_cashbook_ai();
    """
    async with pool.acquire() as conn:
        await conn.execute(sql)
    await msg.answer("✅ Колонка master_id, функция и триггер `orders_to_cashbook_ai` обновлены.")
# ===== Admin: WIPE TEST DATA =====
@dp.message(Command("wipe_test_data"))
async def wipe_test_data(msg: Message):
    # only admins/superadmins
    if not await has_permission(msg.from_user.id, "import_leads"):
        return await msg.answer("Только для администраторов.")
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Backup responsibility is external (psql \\copy). Here we just cleanup test data.
            # 1) Clear staging
            await conn.execute("TRUNCATE TABLE clients_raw RESTART IDENTITY;")
            # 2) Clear operational tables (keep RBAC: staff/permissions/role_permissions)
            for tbl in [
                "orders",
                "payroll_items",
                "order_payroll",
                "payroll",
                "cashbook_entries",
                "bonus_transactions",
                "cashbook",
                "clients"
            ]:
                await conn.execute(f"TRUNCATE TABLE {tbl} RESTART IDENTITY CASCADE;")
    await msg.answer("Тестовые данные удалены. RBAC-таблицы сохранены.")

# ===== Admin: UPLOAD CSV TO clients_raw =====
from aiogram.types import ContentType, FSInputFile
from aiogram import types


class UploadFSM(StatesGroup):
    waiting_csv = State()

@dp.message(Command("upload_clients"))
async def upload_clients_start(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "import_leads"):
        return await msg.answer("Только для администраторов.")
    await state.set_state(UploadFSM.waiting_csv)
    return await msg.answer("Отправьте CSV-файл (UTF-8, ; или , разделитель) с колонками: full_name, phone, bonus_balance, birthday, address.", reply_markup=cancel_kb)

@dp.message(UploadFSM.waiting_csv, F.document)
async def upload_clients_file(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "import_leads"):
        await state.clear()
        return await msg.answer("Только для администраторов.")
    file = await bot.get_file(msg.document.file_id)
    file_bytes = await bot.download_file(file.file_path)
    data = file_bytes.read()
    # Try to decode as utf-8
    try:
        text = data.decode("utf-8")
    except Exception:
        await state.clear()
        return await msg.answer("Ошибка: файл должен быть в кодировке UTF-8.")
    # Parse CSV (robust: handle BOM, CRLF, and ; or , delimiter)
    first_line = text.splitlines()[0] if text else ""
    # strip UTF-8 BOM if present
    if first_line.startswith("\ufeff"):
        text = text.lstrip("\ufeff")
        first_line = first_line.lstrip("\ufeff")
    delimiter = ";" if (";" in first_line and first_line.count(";") >= first_line.count(",")) else ","
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)

    # normalize headers: strip, lower, drop BOM
    def _norm(h: str) -> str:
        return (h or "").strip().lstrip("\ufeff").lower()

    required = {"full_name", "phone", "bonus_balance", "birthday", "address"}
    headers = [_norm(h) for h in (reader.fieldnames or [])]
    # map normalized -> original header name for safe access
    header_map = {_norm(orig): orig for orig in (reader.fieldnames or [])}

    missing = required - set(headers)
    if missing:
        await state.clear()
        return await msg.answer(f"В CSV отсутствуют колонки: {', '.join(sorted(missing))}")

    rows = []
    for row in reader:
        # access by normalized keys via header_map
        def getv(key: str) -> str:
            orig = header_map.get(key, "")
            return (row.get(orig) or "").strip()

        bday_iso = parse_birthday_str(getv("birthday"))
        bb_raw = getv("bonus_balance")
        try:
            bb = int(bb_raw) if bb_raw != "" else 0
        except Exception:
            bb = 0

        rows.append({
            "full_name": getv("full_name") or None,
            "phone": getv("phone") or None,
            "bonus_balance": bb,
            "birthday": bday_iso,
            "address": getv("address") or None,
        })
    if not rows:
        await state.clear()
        return await msg.answer("Файл пуст.")
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS clients_raw (
                    full_name     text,
                    phone         text,
                    bonus_balance integer,
                    birthday      date,
                    address       text
                );
            """)
            # clear staging before load
            await conn.execute("TRUNCATE TABLE clients_raw;")
            # bulk insert
            insert_sql = """
                INSERT INTO clients_raw(full_name, phone, bonus_balance, birthday, address)
                VALUES ($1, $2, $3, $4, $5)
            """
            args = [(r["full_name"], r["phone"], r["bonus_balance"], r["birthday"], r["address"]) for r in rows]
            # execute many
            await conn.executemany(insert_sql, args)
    await state.clear()
    return await msg.answer(f"Загружено строк в staging (clients_raw): {len(rows)}.\nТеперь выполните /import_leads_dryrun, затем /import_leads.")

# ===== /income admin command =====
@dp.message(Command("income"))
async def add_income(msg: Message):
    if not await has_permission(msg.from_user.id, "record_cashflows"):
        return await msg.answer("Только для администраторов.")

    # Разбор аргументов из текста: /income <сумма> <метод> <комментарий>
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        return await msg.answer("Формат: /income <сумма> <метод> <комментарий>\nНапр.: /income 1500 нал Оплата заказа #123")

    tail = parts[1].strip()
    # Пытаемся выделить сумму (первый токен), метод (следующий токен или две лексемы для 'карта дима' / 'карта женя'), и комментарий
    tokens = tail.split()
    if len(tokens) < 2:
        return await msg.answer("Нужно указать сумму и метод. Формат: /income <сумма> <метод> <комментарий>")

    amount_str = tokens[0]
    # метод может быть из двух слов: 'карта дима' / 'карта женя'
    if len(tokens) >= 3 and (tokens[1].lower() == 'карта' and tokens[2].lower() in ('дима','женя')):
        method_raw = tokens[1] + ' ' + tokens[2]
        comment = ' '.join(tokens[3:]) if len(tokens) > 3 else ''
    else:
        method_raw = tokens[1]
        comment = ' '.join(tokens[2:]) if len(tokens) > 2 else ''

    if not comment:
        return await msg.answer("Не указан комментарий. Формат: /income <сумма> <метод> <комментарий>")

    try:
        amount = Decimal(amount_str)
        if amount <= 0:
            return await msg.answer("Сумма должна быть положительным числом.")
    except Exception:
        return await msg.answer(f"Ошибка: '{amount_str}' не является корректной суммой.")

    method = norm_pay_method_py(method_raw)

    async with pool.acquire() as conn:
        rec = await _record_income(conn, method, amount, comment)

    lines = [
        f"✅ Приход №{rec['id']}",
        f"Сумма: {amount}₽",
        f"Тип оплаты: {method}",
        f"Когда: {rec['happened_at']:%Y-%m-%d %H:%M}",
        f"Комментарий: {comment}",
    ]
    await msg.answer("\n".join(lines))

# ===== /expense admin command =====
@dp.message(Command("expense"))
async def add_expense(msg: Message, command: CommandObject):
    if not await has_permission(msg.from_user.id, "record_cashflows"):
        return await msg.answer("Только для администраторов.")

    # command.args — всё после /expense, например: "123 Тест расхода"
    if not command.args:
        return await msg.answer("Формат: /expense <сумма> <комментарий>")

    parts = command.args.split(maxsplit=1)
    if len(parts) < 2:
        return await msg.answer("Не указан комментарий. Формат: /expense <сумма> <комментарий>")

    amount_str, comment = parts

    try:
        amount = Decimal(amount_str)
        if amount <= 0:
            return await msg.answer("Сумма должна быть положительным числом.")
    except Exception:
        return await msg.answer(f"Ошибка: '{amount_str}' не является корректной суммой.")

    async with pool.acquire() as conn:
        rec = await _record_expense(conn, amount, comment, method="прочее")
    await msg.answer(
        "\n".join([
            f"✅ Расход №{rec['id']}",
            f"Сумма: {amount}₽",
            f"Когда: {rec['happened_at']:%Y-%m-%d %H:%M}",
            f"Комментарий: {comment}",
        ])
    )

# ===== /tx_last admin command =====
@dp.message(Command("tx_last"))
async def tx_last_cmd(msg: Message):
    if not await has_permission(msg.from_user.id, "view_cash_reports"):
        return await msg.answer("Только для администраторов.")
    # парсим N
    parts = msg.text.split(maxsplit=1)
    try:
        n = int(parts[1]) if len(parts) > 1 else 10
    except Exception:
        n = 10
    if n < 1:
        n = 1
    if n > 100:
        n = 100

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, kind, method, amount, happened_at, order_id, comment, is_void
            FROM cashbook_entries
            ORDER BY id DESC
            LIMIT $1
            """,
            n,
        )
    if not rows:
        return await msg.answer("Транзакций нет.")

    def fmt(r):
        ok = "✅" if not r["is_void"] else "⛔️"
        return (
            f"№{r['id']} | {ok} | {r['kind']} | {r['amount']}₽ | {r['method'] or 'прочее'} | "
            f"{r['happened_at']:%Y-%m-%d %H:%M} | order=#{r['order_id']} | {r['comment'] or ''}"
        )

    text = "Последние транзакции:\n" + "\n".join(fmt(r) for r in rows)
    await msg.answer(text)

# ===== /tx_delete superadmin command =====
@dp.message(Command("tx_delete"))
async def tx_delete(msg: Message):
    # only superadmin can delete transactions
    async with pool.acquire() as conn:
        role = await get_user_role(conn, msg.from_user.id)
    if role != 'superadmin':
        return await msg.answer("Удаление транзакций доступно только суперадмину.")

    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        return await msg.answer("Формат: /tx_delete <id>")
    tx_id = int(parts[1].strip())

    async with pool.acquire() as conn:
        rec = await conn.fetchrow(
            "UPDATE cashbook_entries SET is_deleted = TRUE, deleted_at = NOW() "
            "WHERE id = $1 AND COALESCE(is_deleted, FALSE) = FALSE RETURNING id",
            tx_id
        )
    if not rec:
        return await msg.answer("Транзакция не найдена или уже удалена.")
    await msg.answer(f"🗑️ Транзакция №{tx_id} помечена как удалённая.")


@dp.message(Command("withdraw"))
async def withdraw_start(msg: Message, state: FSMContext):
    return await admin_withdraw_entry(msg, state)


@dp.message(Command("mysalary"))
async def my_salary(msg: Message):
    # доступ только для мастеров
    if not await has_permission(msg.from_user.id, "view_own_salary"):
        return await msg.answer("Доступно только мастерам.")
    parts = msg.text.split(maxsplit=1)
    period = parts[1].strip().lower() if len(parts) > 1 else "month"
    period_map = {
        "day": "day",
        "week": "week",
        "month": "month",
        "year": "year",
    }
    if period not in period_map:
        return await msg.answer("Формат: /mysalary [day|week|month|year]")
    period_key = period_map[period]
    async with pool.acquire() as conn:
        rec = await conn.fetchrow(
            f"""
            SELECT
                COALESCE(SUM(pi.base_pay), 0) AS base_pay,
                COALESCE(SUM(pi.fuel_pay), 0) AS fuel_pay,
                COALESCE(SUM(pi.upsell_pay), 0) AS upsell_pay,
                COALESCE(SUM(pi.total_pay), 0) AS total_pay
            FROM payroll_items pi
            JOIN orders o ON o.id = pi.order_id
            WHERE pi.master_id = (
                SELECT id FROM staff WHERE tg_user_id = $1 AND is_active LIMIT 1
            )
              AND o.created_at >= date_trunc('{period_key}', NOW())
            """,
            msg.from_user.id,
        )
    if not rec:
        return await msg.answer("Нет данных для указанного периода.")
    base_pay = rec["base_pay"]
    fuel_pay = rec["fuel_pay"]
    upsell_pay = rec["upsell_pay"]
    total_pay = rec["total_pay"]
    text = (
        f"Зарплата за {period}:\n"
        f"Базовая оплата: {base_pay}₽\n"
        f"Оплата за бензин: {fuel_pay}₽\n"
        f"Оплата за доп. продажи: {upsell_pay}₽\n"
        f"Итого: {total_pay}₽"
    )
    await msg.answer(text)

### 2. Добавить обработчик `/myincome` (дневная выручка по типу оплаты)

@dp.message(Command("myincome"))
async def my_income(msg: Message):
    # доступ только для мастеров
    if not await has_permission(msg.from_user.id, "view_own_income"):
        return await msg.answer("Доступно только мастерам.")
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT o.payment_method AS method,
                   SUM(o.amount_cash) AS total
            FROM orders o
            WHERE o.master_id = (
                SELECT id FROM staff WHERE tg_user_id = $1 AND is_active LIMIT 1
            )
              AND date_trunc('day', o.created_at) = date_trunc('day', NOW())
            GROUP BY o.payment_method
            """,
            msg.from_user.id,
        )
    if not rows:
        return await msg.answer("Нет данных за сегодня.")
    lines = [f"{row['method']}: {row['total']}₽" for row in rows]
    await msg.answer("Сегодняшний приход по типам оплаты:\n" + "\n".join(lines))

MASTER_SALARY_LABEL = "💼 Зарплата"
MASTER_INCOME_LABEL = "💰 Приход"

master_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🧾 Заказ"), KeyboardButton(text="🔍 Клиент")],
        [KeyboardButton(text=MASTER_SALARY_LABEL), KeyboardButton(text=MASTER_INCOME_LABEL)],
    ],
    resize_keyboard=True
)


def master_main_kb() -> ReplyKeyboardMarkup:
    return master_kb
salary_period_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="День"), KeyboardButton(text="Неделя")],
        [KeyboardButton(text="Месяц"), KeyboardButton(text="Год")],
    ],
    resize_keyboard=True
)

cancel_kb = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="Отмена")]],
    resize_keyboard=True
)


@dp.message(F.text.lower() == "отмена")
async def cancel_any(msg: Message, state: FSMContext):
    await state.clear()
    # возвращаем клавиатуру мастера
    return await msg.answer("Отменено.", reply_markup=master_kb)


@dp.message(AdminMenuFSM.root, F.text, ~F.text.startswith("/"))
async def admin_root_fallback(msg: Message, state: FSMContext):
    await msg.answer("Выберите действие на клавиатуре ниже.", reply_markup=admin_root_kb())

# Legacy env-based admin check kept for backward compatibility
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_TG_IDS

async def ensure_master(user_id: int) -> bool:
    # Master access is defined by permission to create orders/clients
    return await has_permission(user_id, "create_orders_clients")

@dp.message(CommandStart())
async def start_handler(msg: Message, state: FSMContext):
    await state.clear()
    global pool
    async with pool.acquire() as conn:
        role = await get_user_role(conn, msg.from_user.id)

    if role in ("admin", "superadmin"):
        await admin_menu_start(msg, state)
        return

    await msg.answer(
        "Привет! Это внутренний бот. Нажми нужную кнопку.",
        reply_markup=master_main_kb()
    )

# ---- /find ----
@dp.message(Command("find"))
async def find_cmd(msg: Message):
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        return await msg.answer("Формат: /find +7XXXXXXXXXX, 8XXXXXXXXXX или 9XXXXXXXXX")
    user_input = parts[1].strip()  # берем введённый аргумент
    # проверяем формат номера
    if not is_valid_phone_format(user_input):
        return await msg.answer("Формат: /find +7XXXXXXXXXX, 8XXXXXXXXXX или 9XXXXXXXXX")
    phone_in = normalize_phone_for_db(user_input)  # нормализуем, если формат корректный
    async with pool.acquire() as conn:
        rec = await conn.fetchrow(
            "SELECT full_name, phone, bonus_balance, birthday, status "
            "FROM clients WHERE regexp_replace(phone,'[^0-9]+','','g')=regexp_replace($1,'[^0-9]+','','g')",
            phone_in
        )
    if not rec:
        return await msg.answer("Не найдено.")
    bd = rec["birthday"].isoformat() if rec["birthday"] else "—"
    status = rec["status"] or "—"
    text = (
        f"👤 {rec['full_name'] or 'Без имени'}\n"
        f"📞 {rec['phone']}\n"
        f"💳 {rec['bonus_balance']}\n"
        f"🎂 {bd}\n"
        f"🏷️ {status}"
    )
    if status == 'lead':
        text += "\n\nЭто лид. Нажмите «🧾 Заказ», чтобы оформить первый заказ и обновить имя."
    await msg.answer(text, reply_markup=main_kb)

# ===== FSM: Я ВЫПОЛНИЛ ЗАКАЗ =====
class OrderFSM(StatesGroup):
    phone = State()
    name = State()
    amount = State()
    upsell_flag = State()
    upsell_amount = State()
    bonus_spend = State()
    waiting_payment_method = State()
    maybe_bday = State()
    name_fix = State()
    confirm = State()

main_kb = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="🧾 Заказ")]],
    resize_keyboard=True
)

# ---- Master menu states ----
class MasterFSM(StatesGroup):
    waiting_phone = State()
    waiting_salary_period = State()

@dp.message(F.text.in_(["🧾 Я ВЫПОЛНИЛ ЗАКАЗ", "🧾 Заказ"]))
async def start_order(msg: Message, state: FSMContext):
    if not await ensure_master(msg.from_user.id):
        return await msg.answer("У вас нет прав мастера. Обратитесь к администратору.")
    await state.clear()
    await state.set_state(OrderFSM.phone)
    await msg.answer(
    "Введите номер клиента (9XXXXXXXXX, 8XXXXXXXXXX или +7XXXXXXXXXX):",
    reply_markup=cancel_kb
)

@dp.message(OrderFSM.phone, F.text)
async def got_phone(msg: Message, state: FSMContext):
    user_input = msg.text.strip()
    # если формат неправильный — вернуть сообщение об ошибке и сбросить состояние
    if not is_valid_phone_format(user_input):
        await state.clear()
        return await msg.answer(
            "Формат номера: 9XXXXXXXXX, 8XXXXXXXXXX или +7XXXXXXXXXX",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="Отмена")]],
                resize_keyboard=True
            )
        )
    # если всё хорошо — нормализуем номер
    phone_in = normalize_phone_for_db(user_input)
    async with pool.acquire() as conn:
        client = await conn.fetchrow(
            "SELECT id, full_name, phone, bonus_balance, birthday, status "
            "FROM clients WHERE regexp_replace(phone,'[^0-9]+','','g')=regexp_replace($1,'[^0-9]+','','g')",
            phone_in
        )
    data = {"phone_in": phone_in}
    if client:
        data["client_id"] = client["id"]
        data["client_name"] = client["full_name"]
        data["bonus_balance"] = int(client["bonus_balance"] or 0)
        data["birthday"] = client["birthday"]
        await state.update_data(**data)

        # Если имя некорректное ИЛИ запись помечена как lead — попросим мастера исправить
        if is_bad_name(client["full_name"] or "") or (client["status"] == "lead"):
            await state.set_state(OrderFSM.name_fix)
            return await msg.answer(
                "Найден лид/некорректное имя.\n"
                "Введите правильное имя клиента (или нажмите ‘Отмена’):",
                reply_markup=cancel_kb
            )

        await state.set_state(OrderFSM.amount)
        return await msg.answer(
            f"Клиент найден: {client['full_name'] or 'Без имени'}\n"
            f"Бонусов: {data['bonus_balance']}\n"
            "Введите сумму чека (руб):",
            reply_markup=cancel_kb
        )
    else:
        data["client_id"] = None
        data["bonus_balance"] = 0
        await state.update_data(**data)
        await state.set_state(OrderFSM.name)
        return await msg.answer("Клиент не найден. Введите имя клиента:", reply_markup=cancel_kb)


# Новый обработчик для исправления некорректного имени клиента
@dp.message(OrderFSM.name_fix, F.text)
async def fix_name(msg: Message, state: FSMContext):
    new_name = msg.text.strip()
    if not new_name:
        return await msg.answer("Имя не может быть пустым. Введите имя или нажмите ‘Отмена’.", reply_markup=cancel_kb)
    if is_bad_name(new_name):
        return await msg.answer("Имя похоже на номер/метку. Введите корректное имя.", reply_markup=cancel_kb)

    await state.update_data(client_name=new_name)
    await state.set_state(OrderFSM.amount)
    await msg.answer("Имя обновлено. Введите сумму чека (руб):", reply_markup=cancel_kb)

def parse_money(s: str) -> Decimal | None:
    s = s.replace(",", ".").strip()
    try:
        v = Decimal(s)
        if v < 0: return None
        return v.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    except Exception:
        return None

@dp.message(OrderFSM.name, F.text)
async def got_name(msg: Message, state: FSMContext):
    await state.update_data(client_name=msg.text.strip())
    await state.set_state(OrderFSM.amount)
    await msg.answer("Введите сумму чека (руб):", reply_markup=cancel_kb)

@dp.message(OrderFSM.amount, F.text)
async def got_amount(msg: Message, state: FSMContext):
    amount = parse_money(msg.text)
    if amount is None:
        return await msg.answer(
            "Нужно число ≥ 0. Введите сумму чека ещё раз:",
            reply_markup=cancel_kb
        )
    await state.update_data(amount_total=amount)
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Да"), KeyboardButton(text="Нет")],
                  [KeyboardButton(text="Отмена")]],
        resize_keyboard=True
    )
    await state.set_state(OrderFSM.upsell_flag)
    await msg.answer("Была доп. продажа? (Да/Нет)", reply_markup=kb)

@dp.message(OrderFSM.upsell_flag, F.text.lower().in_(["да","нет"]))
async def got_upsell_flag(msg: Message, state: FSMContext):
    if msg.text.lower() == "да":
        await state.set_state(OrderFSM.upsell_amount)
        return await msg.answer("Введите сумму доп. продажи (руб):", reply_markup=cancel_kb)
    else:
        await state.update_data(upsell_amount=Decimal("0"))
        return await ask_bonus(msg, state)

@dp.message(OrderFSM.upsell_amount, F.text)
async def got_upsell_amount(msg: Message, state: FSMContext):
    v = parse_money(msg.text)
    if v is None:
        return await msg.answer(
            "Нужно число ≥ 0. Введите сумму доп. продажи ещё раз:",
            reply_markup=cancel_kb
        )
    await state.update_data(upsell_amount=v)
    return await ask_bonus(msg, state)

async def ask_bonus(msg: Message, state: FSMContext):
    data = await state.get_data()
    amount = Decimal(str(data["amount_total"]))
    balance = Decimal(str(data.get("bonus_balance", 0)))

    # считаем ограничения
    max_by_rate = (amount * MAX_BONUS_RATE).quantize(Decimal("1"), rounding=ROUND_DOWN)
    max_by_min_cash = (amount - MIN_CASH).quantize(Decimal("1"), rounding=ROUND_DOWN)
    bonus_max = max(Decimal("0"), min(max_by_rate, balance, max_by_min_cash))

    # === Если бонусов нет к списанию — пропускаем шаг ===
    if balance <= 0 or bonus_max <= 0:
        await state.update_data(bonus_max=Decimal("0"), bonus_spent=Decimal("0"), amount_cash=amount)
        await state.set_state(OrderFSM.waiting_payment_method)
        return await msg.answer(
            "Бонусов нет — пропускаем списание.\n"
            f"Оплата деньгами: {amount}\nВыберите способ оплаты:",
            reply_markup=payment_method_kb()
        )

    # иначе — задаём выбор списания
    await state.update_data(bonus_max=bonus_max)
    await state.set_state(OrderFSM.bonus_spend)
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Списать 0"), KeyboardButton(text="Списать 50%"), KeyboardButton(text="Списать MAX")],
            [KeyboardButton(text="Другая сумма")],
            [KeyboardButton(text="Отмена")]
        ],
        resize_keyboard=True
    )
    return await msg.answer(f"Можно списать до {bonus_max} бонусов.\nВыберите:", reply_markup=kb)

@dp.message(OrderFSM.bonus_spend, F.text)
async def got_bonus_spend(msg: Message, state: FSMContext):
    data = await state.get_data()
    amount = Decimal(str(data["amount_total"]))
    bonus_max = Decimal(str(data["bonus_max"]))
    choice = msg.text.lower()
    if "50%" in choice:
        spend = (amount * Decimal("0.5")).quantize(Decimal("1"), rounding=ROUND_DOWN)
    elif "max" in choice:
        spend = bonus_max
    elif "0" in choice:
        spend = Decimal("0")
    else:
        v = re.sub(r"[^\d]", "", msg.text)
        if not v:
            return await msg.answer("Введите целую сумму бонусов (руб), например: 300")
        spend = Decimal(v)
    if spend > bonus_max:
        return await msg.answer(f"Нельзя списать больше {bonus_max}. Введите сумму не превышающую лимит.")
    cash_payment = amount - spend
    if cash_payment < MIN_CASH:
        return await msg.answer(f"Минимальная оплата деньгами {MIN_CASH}. Уменьшите списание бонусов.")
    await state.update_data(bonus_spent=spend, amount_cash=cash_payment)
    await state.set_state(OrderFSM.waiting_payment_method)
    return await msg.answer(
        f"Оплата деньгами: {cash_payment}\nВыберите способ оплаты:",
        reply_markup=payment_method_kb()
    )

@dp.message(OrderFSM.waiting_payment_method, F.text)
async def order_pick_method(msg: Message, state: FSMContext):
    method_raw = (msg.text or "").strip()
    method = norm_pay_method_py(method_raw)
    allowed_methods = PAYMENT_METHODS + [GIFT_CERT_LABEL]
    if method not in allowed_methods:
        return await msg.answer("Выберите способ оплаты с клавиатуры.")

    if method == GIFT_CERT_LABEL:
        data = await state.get_data()
        amt_cash = data.get("amount_cash")
        if amt_cash is None:
            return await msg.answer("Сначала введите сумму чека, затем выберите способ оплаты.")
        data["amount_total"] = amt_cash
        data["amount_cash"] = Decimal(0)
        data["payment_method"] = GIFT_CERT_LABEL
        await state.update_data(**data)
        await msg.answer(
            "Выбран Подарочный сертификат. Сумма чека будет использована как номинал, в кассу поступит 0₽.",
            reply_markup=ReplyKeyboardRemove()
        )
        return await proceed_order_finalize(msg, state)

    data = await state.get_data()
    amount_cash = Decimal(str(data.get("amount_cash", 0)))
    if data.get("amount_total") is None and data.get("amount_cash") is not None:
        data["amount_total"] = data["amount_cash"]
    data["payment_method"] = method
    await state.update_data(payment_method=method, amount_total=data.get("amount_total"))

    await msg.answer("Метод оплаты сохранён.", reply_markup=ReplyKeyboardRemove())

    return await proceed_order_finalize(msg, state)


async def proceed_order_finalize(msg: Message, state: FSMContext):
    data = await state.get_data()
    if data.get("birthday"):
        await state.set_state(OrderFSM.confirm)
        return await show_confirm(msg, state)

    await state.set_state(OrderFSM.maybe_bday)
    return await msg.answer(
        "Если знаете ДР клиента, введите ДД.ММ (или '-' чтобы пропустить):",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="-")], [KeyboardButton(text="Отмена")]],
            resize_keyboard=True
        )
    )


@dp.message(OrderFSM.maybe_bday, F.text)
async def got_bday(msg: Message, state: FSMContext):
    val = msg.text.strip()
    if val != "-" and not re.fullmatch(r"\d{2}\.\d{2}", val):
        return await msg.answer("Формат ДР: ДД.MM (например 05.11) или '-' чтобы пропустить")
    if val != "-":
        d, m = map(int, val.split("."))
        await state.update_data(new_birthday=date(2000, m, d))
    await state.set_state(OrderFSM.confirm)
    return await show_confirm(msg, state)

async def show_confirm(msg: Message, state: FSMContext):
    data = await state.get_data()
    amount = Decimal(str(data["amount_total"]))
    upsell = Decimal(str(data.get("upsell_amount", 0)))
    bonus_spent = Decimal(str(data.get("bonus_spent", 0)))
    cash_payment = Decimal(str(data["amount_cash"]))
    payment_method = data.get("payment_method")
    gross = amount if payment_method == GIFT_CERT_LABEL else cash_payment
    bonus_earned = qround_ruble(cash_payment * BONUS_RATE)
    base_pay = qround_ruble(gross * (MASTER_PER_3000 / Decimal(3000)))
    if base_pay < Decimal("1000"):
        base_pay = Decimal("1000")
    upsell_pay = qround_ruble(upsell * (UPSELL_PER_3000 / Decimal(3000)))
    total_pay = base_pay + FUEL_PAY + upsell_pay
    await state.update_data(bonus_earned=int(bonus_earned), base_pay=base_pay, upsell_pay=upsell_pay, fuel_pay=FUEL_PAY, total_pay=total_pay)
    name = data.get("client_name") or "Без имени"
    bday_text = data.get("birthday") or data.get("new_birthday") or "—"
    text = (
        f"Проверьте:\n"
        f"👤 {name}\n"
        f"📞 {data['phone_in']}\n"
        f"💈 Чек: {amount} (доп: {upsell})\n"
        f"💳 Оплата деньгами: {cash_payment}\n"
        f"🎁 Списано бонусов: {bonus_spent}\n"
        f"➕ Начислить бонусов: {int(bonus_earned)}\n"
        f"🎂 ДР: {bday_text}\n"
        f"👷 ЗП мастера: {total_pay} (база {base_pay} + бензин {FUEL_PAY} + доп {upsell_pay})\n\n"
        f"Отправьте 'подтвердить' или 'отмена'"
    )
    await msg.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="подтвердить")],[KeyboardButton(text="отмена")]], resize_keyboard=True))

@dp.message(OrderFSM.confirm, F.text.lower() == "отмена")
async def cancel_order(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer("Отменено.", reply_markup=master_kb)

@dp.message(OrderFSM.confirm, F.text.lower() == "подтвердить")
async def commit_order(msg: Message, state: FSMContext):
    data = await state.get_data()
    phone_in = data["phone_in"]
    amount_cash = Decimal(str(data.get("amount_cash") or 0))
    raw_total = data.get("amount_total")
    if raw_total is None:
        raw_total = amount_cash
    amount_total = Decimal(str(raw_total))
    payment_method = data.get("payment_method")
    upsell = Decimal(str(data.get("upsell_amount", 0)))
    bonus_spent = int(Decimal(str(data.get("bonus_spent", 0))))
    cash_payment = amount_cash
    bonus_earned = int(Decimal(str(data["bonus_earned"])))
    base_pay = Decimal(str(data["base_pay"]))
    upsell_pay = Decimal(str(data["upsell_pay"]))
    fuel_pay = Decimal(str(data["fuel_pay"]))
    total_pay = Decimal(str(data["total_pay"]))
    name = data.get("client_name")
    new_bday = data.get("new_birthday")  # date|None

    async with pool.acquire() as conn:
        async with conn.transaction():
            client = await conn.fetchrow(
                "INSERT INTO clients (full_name, phone, bonus_balance, birthday, status) "
                "VALUES ($1, $2, 0, $3, 'client') "
                "ON CONFLICT (phone) DO UPDATE SET "
                "  full_name = COALESCE(EXCLUDED.full_name, clients.full_name), "
                "  birthday  = COALESCE(EXCLUDED.birthday, clients.birthday), "
                "  status='client' "
                "RETURNING id, bonus_balance",
                name, phone_in, new_bday
            )
            client_id = client["id"]

            order = await conn.fetchrow(
                "INSERT INTO orders (client_id, master_id, phone_digits, amount_total, amount_cash, amount_upsell, "
                " bonus_spent, bonus_earned, payment_method) "
                "VALUES ($1, "
                "       (SELECT id FROM staff WHERE tg_user_id=$2 AND is_active LIMIT 1), "
                "       regexp_replace($3,'[^0-9]+','','g'), $4, $5, $6, $7, $8, $9) "
                "RETURNING id",
                client_id, msg.from_user.id, phone_in, amount_total, cash_payment, upsell,
                bonus_spent, bonus_earned, payment_method
            )
            order_id = order["id"]

            await conn.execute(
                "INSERT INTO staff(tg_user_id, role, is_active) "
                "VALUES ($1,'master',true) ON CONFLICT (tg_user_id) DO UPDATE SET is_active=true",
                msg.from_user.id
            )

            if bonus_spent > 0:
                await conn.execute(
                    "INSERT INTO bonus_transactions (client_id, delta, reason, order_id) VALUES ($1, $2, 'spend', $3)",
                    client_id, -bonus_spent, order_id
                )
            if bonus_earned > 0:
                await conn.execute(
                    "INSERT INTO bonus_transactions (client_id, delta, reason, order_id) VALUES ($1, $2, 'accrual', $3)",
                    client_id, bonus_earned, order_id
                )

            # стало: пересчитываем по сумме всех транзакций клиента
            await conn.execute(
                """
                UPDATE clients c
                SET bonus_balance = GREATEST(
                    0,
                    COALESCE((
                        SELECT SUM(bt.delta)::integer
                        FROM bonus_transactions bt
                        WHERE bt.client_id = c.id
                    ), 0)
                )
                WHERE c.id = $1
                """,
                client_id
            )

            await conn.execute(
                "INSERT INTO payroll_items (order_id, master_id, base_pay, fuel_pay, upsell_pay, total_pay, calc_info) "
                "VALUES ($1, (SELECT id FROM staff WHERE tg_user_id=$2), $3, $4, $5, $6, "
                "        jsonb_build_object('cash_payment', to_jsonb(($7)::numeric), 'rules', '1000/3000 + 150 + 500/3000'))",
                order_id, msg.from_user.id, base_pay, fuel_pay, upsell_pay, total_pay, cash_payment
            )

    await state.clear()
    await msg.answer("Готово ✅ Заказ сохранён.\nСпасибо!", reply_markup=master_kb)

# ---- Master menu handlers ----

# 🔍 Клиент — поиск клиента по номеру
@dp.message(F.text == "🔍 Клиент")
async def master_find_start(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "view_own_salary"):
        return await msg.answer("Доступно только мастерам.")
    await state.set_state(MasterFSM.waiting_phone)
    await msg.answer("Введите номер телефона клиента:", reply_markup=cancel_kb)

@dp.message(MasterFSM.waiting_phone, F.text)
async def master_find_phone(msg: Message, state: FSMContext):
    user_input = msg.text.strip()
    # если формат неправильный — вернуть сообщение об ошибке
    if not is_valid_phone_format(user_input):
        cancel_kb = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Отмена")]],
            resize_keyboard=True
        )
        return await msg.answer(
            "Формат номера: 9XXXXXXXXX, 8XXXXXXXXXX или +7XXXXXXXXXX",
            reply_markup=cancel_kb
        )

    phone_in = normalize_phone_for_db(user_input)
    async with pool.acquire() as conn:
        rec = await conn.fetchrow(
            "SELECT full_name, phone, bonus_balance, birthday, status "
            "FROM clients WHERE regexp_replace(phone,'[^0-9]+','','g')=regexp_replace($1,'[^0-9]+','','g')",
            phone_in
        )
    await state.clear()
    if not rec:
        return await msg.answer("Не найдено.", reply_markup=master_kb)
    bd = rec["birthday"].isoformat() if rec["birthday"] else "—"
    status = rec["status"] or "—"
    text = (
        f"👤 {rec['full_name'] or 'Без имени'}\n"
        f"📞 {rec['phone']}\n"
        f"💳 {rec['bonus_balance']}\n"
        f"🎂 {bd}\n"
        f"🏷️ {status}"
    )
    if status == 'lead':
        text += "\n\nЭто лид. Нажмите «🧾 Заказ», чтобы оформить первый заказ и обновить имя."
    await msg.answer(text, reply_markup=master_kb)

# 💼 Зарплата — запрос периода
@dp.message(F.text == MASTER_SALARY_LABEL)
async def master_salary_prompt(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "view_own_salary"):
        return await msg.answer("Доступно только мастерам.")
    await state.set_state(MasterFSM.waiting_salary_period)
    await msg.answer(
        "Выберите период:",
        reply_markup=salary_period_kb
    )

@dp.message(MasterFSM.waiting_salary_period, F.text)
async def master_salary_calc(msg: Message, state: FSMContext):
    mapping = {
        "День": "day",
        "Неделя": "week",
        "Месяц": "month",
        "Год": "year",
    }
    period_label = msg.text.strip().capitalize()
    period = mapping.get(period_label)
    if not period:
        return await msg.answer(
            "Период должен быть одним из: День, Неделя, Месяц, Год.",
            reply_markup=salary_period_kb
        )
    async with pool.acquire() as conn:
        rec = await conn.fetchrow(
            f"""
            SELECT
                COALESCE(SUM(pi.base_pay),0) AS base_pay,
                COALESCE(SUM(pi.fuel_pay),0) AS fuel_pay,
                COALESCE(SUM(pi.upsell_pay),0) AS upsell_pay,
                COALESCE(SUM(pi.total_pay),0) AS total_pay
            FROM payroll_items pi
            JOIN orders o ON o.id = pi.order_id
            WHERE pi.master_id = (
                SELECT id FROM staff WHERE tg_user_id=$1 AND is_active LIMIT 1
            )
              AND o.created_at >= date_trunc('{period}', NOW())
            """,
            msg.from_user.id
        )
    await state.clear()
    if not rec:
        return await msg.answer("Нет данных для указанного периода.", reply_markup=master_kb)
    base_pay, fuel_pay, upsell_pay, total_pay = rec["base_pay"], rec["fuel_pay"], rec["upsell_pay"], rec["total_pay"]
    await msg.answer(
        f"Зарплата за {period_label}:\n"
        f"Базовая оплата: {base_pay}₽\n"
        f"Оплата за бензин: {fuel_pay}₽\n"
        f"Оплата за доп. продажи: {upsell_pay}₽\n"
        f"Итого: {total_pay}₽",
        reply_markup=master_kb
    )

# 💰 Приход — выручка за сегодня
@dp.message(F.text == MASTER_INCOME_LABEL)
async def master_income(msg: Message):
    if not await has_permission(msg.from_user.id, "view_own_income"):
        return await msg.answer("Доступно только мастерам.")
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT o.payment_method AS method,
                   SUM(o.amount_cash) AS total
            FROM orders o
            WHERE o.master_id = (
                SELECT id FROM staff WHERE tg_user_id=$1 AND is_active LIMIT 1
            )
              AND date_trunc('day', o.created_at) = date_trunc('day', NOW())
            GROUP BY o.payment_method
            """,
            msg.from_user.id,
        )
    if not rows:
        return await msg.answer("Нет данных за сегодня.", reply_markup=master_kb)
    lines = [f"{row['method']}: {row['total']}₽" for row in rows]
    await msg.answer("Сегодняшний приход по типам оплаты:\n" + "\n".join(lines), reply_markup=master_kb)

# fallback

@dp.message(F.text, ~F.text.startswith("/"))
async def unknown(msg: Message, state: FSMContext):
    # Если пользователь находится в процессе любого сценария — не вмешиваемся
    cur = await state.get_state()
    if cur is not None:
        return
    kb = master_kb if await has_permission(msg.from_user.id, "view_own_salary") else main_kb
    await msg.answer("Команда не распознана. Нажми «🧾 Я ВЫПОЛНИЛ ЗАКАЗ» или /help", reply_markup=kb)

async def main():
    global pool
    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=1, max_size=5)
    await set_commands()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
