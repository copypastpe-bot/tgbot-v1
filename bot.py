import asyncio, os, re, logging
import csv, io
from decimal import Decimal, ROUND_DOWN
from datetime import date, datetime, timezone, timedelta, time
from zoneinfo import ZoneInfo
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
    ContentType,
)
from aiogram.filters import CommandStart, Command, CommandObject, StateFilter
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ===== FSM State Groups =====
class AdminMenuFSM(StatesGroup):
    root    = State()
    masters = State()
    clients = State()


class AdminClientsFSM(StatesGroup):
    find_wait_phone = State()
    view_client      = State()
    edit_wait_phone = State()
    edit_pick_field = State()
    edit_wait_value = State()


class AdminMastersFSM(StatesGroup):
    remove_wait_phone = State()


class IncomeFSM(StatesGroup):
    waiting_method = State()
    waiting_amount = State()
    waiting_comment = State()
    waiting_confirm = State()


class ExpenseFSM(StatesGroup):
    waiting_amount = State()
    waiting_comment = State()
    waiting_confirm = State()


class WithdrawFSM(StatesGroup):
    waiting_amount  = State()
    waiting_master  = State()
    waiting_comment = State()
    waiting_confirm = State()

class TxDeleteFSM(StatesGroup):
    waiting_date = State()
    waiting_pick = State()
    waiting_confirm = State()


class OrderDeleteFSM(StatesGroup):
    waiting_date = State()
    waiting_pick = State()
    waiting_confirm = State()


class AddMasterFSM(StatesGroup):
    waiting_tg_id = State()
    waiting_phone = State()
    waiting_name  = State()


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

# chat ids for notifications (2 чата: «Заказы подтверждения» и «Ракета деньги»)
ORDERS_CONFIRM_CHAT_ID = int(os.getenv("ORDERS_CONFIRM_CHAT_ID", "0") or "0")  # Заказы подтверждения (в т.ч. З/П)
MONEY_FLOW_CHAT_ID     = int(os.getenv("MONEY_FLOW_CHAT_ID", "0") or "0")      # «Ракета деньги»

# env rules
MIN_CASH = Decimal(os.getenv("MIN_CASH", "2500"))
BONUS_RATE = Decimal(os.getenv("BONUS_RATE_PERCENT", "5")) / Decimal(100)
MAX_BONUS_RATE = Decimal(os.getenv("MAX_BONUS_SPEND_RATE_PERCENT", "50")) / Decimal(100)
FUEL_PAY = Decimal(os.getenv("FUEL_PAY", "150"))
MASTER_PER_3000 = Decimal(os.getenv("MASTER_RATE_PER_3000", "1000"))
UPSELL_PER_3000 = Decimal(os.getenv("UPSELL_RATE_PER_3000", "500"))

MOSCOW_TZ = ZoneInfo("Europe/Moscow")
BONUS_BIRTHDAY_VALUE = Decimal("300")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
bot = Bot(BOT_TOKEN)
dp = Dispatcher()

# === Ignore group/supergroup/channel updates; work only in private chats ===
from aiogram import BaseMiddleware

class IgnoreNonPrivateMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        chat = data.get("event_chat")
        # If event has no chat (rare), or chat is not private — swallow
        if chat and getattr(chat, "type", None) != "private":
            return
        return await handler(event, data)

# Apply to all message & callback updates
dp.message.middleware(IgnoreNonPrivateMiddleware())
dp.callback_query.middleware(IgnoreNonPrivateMiddleware())

pool: asyncpg.Pool | None = None
daily_reports_task: asyncio.Task | None = None
birthday_task: asyncio.Task | None = None

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


PERMISSIONS_CANON = [
    "view_orders_reports",
    "view_cash_reports",
    "view_profit_reports",
    "view_payments_by_method",
    "view_last_transactions",
    "manage_income",
    "manage_expense",
    "withdraw_cash",
    "record_cashflows",
    "manage_clients",
    "edit_client",
    "manage_masters",
    "add_master",
    "create_orders_clients",
    "view_salary_reports",
    "view_own_salary",
    "view_own_income",
    "import_leads",
]

ROLE_MATRIX = {
    "superadmin": PERMISSIONS_CANON,
    "admin": [
        "view_orders_reports",
        "view_cash_reports",
        "view_profit_reports",
        "view_payments_by_method",
        "view_last_transactions",
        "manage_income",
        "manage_expense",
        "withdraw_cash",
        "record_cashflows",
        "manage_clients",
        "edit_client",
        "manage_masters",
        "add_master",
        "create_orders_clients",
        "view_salary_reports",
        "view_own_salary",
        "view_own_income",
        "import_leads",
    ],
    "master": [
        "create_orders_clients",
        "view_own_salary",
        "view_own_income",
    ],
}


async def init_permissions(conn):
    for p in PERMISSIONS_CANON:
        await conn.execute(
            """
            INSERT INTO permissions(name)
            VALUES ($1)
            ON CONFLICT (name) DO NOTHING
            """,
            p,
        )
    for role, perms in ROLE_MATRIX.items():
        await conn.execute("DELETE FROM role_permissions WHERE role=$1", role)
        if not perms:
            continue
        await conn.executemany(
            """
            INSERT INTO role_permissions(role, permission_id)
            SELECT $1, id FROM permissions WHERE name=$2
            """,
            [(role, perm) for perm in perms],
        )

# ===== helpers =====
def only_digits(s: str) -> str:
    return re.sub(r"[^0-9]", "", s or "")

def normalize_phone_for_db(s: str) -> str:
    """
    Extract first valid RU phone subsequence from mixed text and normalize to +7XXXXXXXXXX.
    Rules:
    - If the first collected digit is '7' or '8' → take exactly 11 digits.
    - If it's '9' → take exactly 10 digits.
    - Stop as soon as enough digits are collected; ignore everything after.
    - Return +7XXXXXXXXXX for 8XXXXXXXXXX/7XXXXXXXXXX/9XXXXXXXXX.
    If nothing is detected, fall back to best-effort normalization of all digits.
    """
    if not s:
        return s
    first: str | None = None
    buf: list[str] = []
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
    if buf:
        d = ''.join(buf)
        if len(d) == 10 and d.startswith('9'):
            return '+7' + d
        if len(d) == 11 and d.startswith('8'):
            return '+7' + d[1:]
        if len(d) == 11 and d.startswith('7'):
            return '+' + d
    # Fallback: use all digits we can find
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

def mask_phone_last4(phone: str | None) -> str:
    d = re.sub(r"[^0-9]", "", phone or "")
    if len(d) >= 4:
        return f"…{d[-4:]}"
    return "…"

def extract_street(addr: str | None) -> str | None:
    """
    Возвращает только название улицы из адреса, если удаётся.
    Простая эвристика: берем фрагмент до первой запятой.
    """
    if not addr:
        return None
    x = (addr or "").strip()
    part = x.split(",")[0].strip()
    if not part:
        return None
    return part

BAD_NAME_PATTERNS = [
    r"^пропущенный\b",      # Пропущенный ...
    r"\bгугл\s*карты\b",    # (.. Гугл Карты)
    r"\bgoogle\s*maps\b",   # на случай англ. подписи
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


async def build_report_masters_kb(conn) -> tuple[str, ReplyKeyboardMarkup]:
    """
    Построить клавиатуру выбора мастера для отчётов по мастерам.
    Возвращает текст подсказки и клавиатуру.
    """
    masters = await conn.fetch(
        "SELECT id, tg_user_id, COALESCE(first_name,'') AS fn, COALESCE(last_name,'') AS ln "
        "FROM staff WHERE role IN ('master','admin') AND is_active ORDER BY id LIMIT 10"
    )
    if masters:
        kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text=f"{r['fn']} {r['ln']} | tg:{r['tg_user_id']}")] for r in masters
            ] + [
                [KeyboardButton(text="Ввести tg id вручную")],
                [KeyboardButton(text="Назад"), KeyboardButton(text="Отмена")],
            ],
            resize_keyboard=True,
            one_time_keyboard=True,
        )
        return "Выберите мастера или введите tg id:", kb

    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Назад"), KeyboardButton(text="Отмена")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    return "Введите tg id мастера:", kb


def admin_root_kb() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="Отчёты")],
        [KeyboardButton(text="Приход"), KeyboardButton(text="Расход"), KeyboardButton(text="Изъятие")],
        [KeyboardButton(text="Мастера"), KeyboardButton(text="Клиенты")],
        [KeyboardButton(text="Кто я")],
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


def admin_cancel_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Отмена")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

def admin_masters_remove_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Назад"), KeyboardButton(text="Отмена")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def client_edit_fields_kb() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="Имя"), KeyboardButton(text="Телефон")],
        [KeyboardButton(text="ДР"), KeyboardButton(text="Бонусы установить")],
        [KeyboardButton(text="Бонусы добавить/убавить")],
        [KeyboardButton(text="Назад"), KeyboardButton(text="Отмена")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=True)


def client_view_kb() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="Редактировать")],
        [KeyboardButton(text="Назад"), KeyboardButton(text="Отмена")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=True)


def client_find_phone_kb() -> ReplyKeyboardMarkup:
    rows = [[KeyboardButton(text="Назад"), KeyboardButton(text="Отмена")]]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=True)


def tx_last_kb() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="/tx_last 10"), KeyboardButton(text="/tx_last 30"), KeyboardButton(text="/tx_last 50")],
        [KeyboardButton(text="Назад"), KeyboardButton(text="Выйти")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=True)


async def _send_tx_last(msg: Message, limit: int) -> None:
    # проверку прав оставляем как сейчас — через view_cash_reports
    if not await has_permission(msg.from_user.id, "view_cash_reports"):
        await msg.answer("Только для администраторов.")
        return

    if not (1 <= limit <= 200):
        limit = 30

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, happened_at, kind, method, amount,
                   COALESCE(order_id, 0) AS order_id,
                   COALESCE(master_id, 0) AS master_id,
                   COALESCE(comment,'') AS comment
            FROM cashbook_entries
            ORDER BY id DESC
            LIMIT $1
            """,
            limit,
        )

    if not rows:
        await msg.answer("Транзакций нет.")
        return

    lines = [f"Последние транзакции (показать: {limit}):"]
    for r in rows:
        sign = "+" if r["kind"] == "income" else "-"
        amt = format_money(Decimal(r["amount"] or 0))
        dt = (r["happened_at"] or datetime.now()).strftime("%d.%m.%Y %H:%M")
        base = f"#{r['id']} {dt} {sign}{amt}₽ [{r['kind']}/{r['method']}]"
        extras = []
        if r["order_id"]:
            extras.append(f"order:{r['order_id']}")
        if r["master_id"]:
            extras.append(f"master:{r['master_id']}")
        c = (r["comment"] or "").strip()
        if c:
            extras.append(c[:80])
        if extras:
            base += " — " + " | ".join(extras)
        lines.append(base)

    await msg.answer("\n".join(lines))
    await msg.answer("Быстрый выбор:", reply_markup=tx_last_kb())


async def get_master_cash_on_orders(conn, master_id: int) -> Decimal:
    """
    Возвращает сумму наличных, полученных мастером от заказов (все время).
    Считается по таблице cashbook_entries, kind='income', method='Наличные'.
    """
    cash_sum = await conn.fetchval(
        """
        SELECT COALESCE(SUM(amount),0)
        FROM cashbook_entries
        WHERE kind='income' AND method='Наличные'
          AND master_id=$1 AND order_id IS NOT NULL
          AND COALESCE(is_deleted,false)=FALSE
        """,
        master_id,
    )
    return Decimal(cash_sum or 0)


async def _ensure_bonus_posted_column(conn):
    await conn.execute(
        """
        DO $$
        BEGIN
            BEGIN
                ALTER TABLE orders ADD COLUMN bonus_posted boolean NOT NULL DEFAULT false;
            EXCEPTION WHEN duplicate_column THEN
                PERFORM 1;
            END;
            BEGIN
                CREATE INDEX IF NOT EXISTS idx_orders_bonus_posted ON orders(bonus_posted);
            EXCEPTION WHEN others THEN
                PERFORM 1;
            END;
        END$$;
        """
    )


async def bonus_baseline_init(conn, client_id: int | None = None) -> int:
    if client_id is None:
        await conn.execute(
            """
            WITH agg AS (
                SELECT o.client_id, COALESCE(SUM(o.bonus_earned - o.bonus_spent),0) AS bal
                FROM orders o
                GROUP BY o.client_id
            )
            UPDATE clients c
            SET bonus_balance = COALESCE(a.bal, 0)
            FROM agg a
            WHERE a.client_id = c.id;
            """
        )
        await conn.execute("UPDATE orders SET bonus_posted = true;")
        rec = await conn.fetchval("SELECT COUNT(*) FROM clients")
        return int(rec or 0)
    await conn.execute(
        """
        WITH agg AS (
            SELECT o.client_id, COALESCE(SUM(o.bonus_earned - o.bonus_spent),0) AS bal
            FROM orders o
            WHERE o.client_id = $1
            GROUP BY o.client_id
        )
        UPDATE clients c
        SET bonus_balance = COALESCE((SELECT bal FROM agg WHERE client_id=c.id), 0)
        WHERE c.id = $1;
        """,
        client_id,
    )
    await conn.execute("UPDATE orders SET bonus_posted = true WHERE client_id = $1;", client_id)
    return 1


async def post_order_bonus_delta(conn, order_id: int) -> bool:
    row = await conn.fetchrow(
        """
        SELECT o.client_id, o.bonus_earned, o.bonus_spent, o.bonus_posted
        FROM orders o
        WHERE o.id = $1
        LIMIT 1
        """,
        order_id,
    )
    if not row:
        return False
    if row["bonus_posted"]:
        logging.info("[bonus_delta] order=%s already posted", order_id)
        return False

    delta = Decimal(row["bonus_earned"] or 0) - Decimal(row["bonus_spent"] or 0)
    async with conn.transaction():
        await conn.execute(
            """
            UPDATE clients
            SET bonus_balance = bonus_balance + $1
            WHERE id = $2
            """,
            delta,
            row["client_id"],
        )
        await conn.execute(
            """
            UPDATE orders SET bonus_posted = true WHERE id = $1
            """,
            order_id,
        )
    logging.info(
        "[bonus_delta] order=%s client=%s delta=%s applied=%s",
        order_id,
        row["client_id"],
        str(delta),
        True,
    )
    return True


def format_money(amount: Decimal) -> str:
    q = (amount or Decimal(0)).quantize(Decimal("0.1"))
    int_part, frac_part = f"{q:.1f}".split('.')
    int_formatted = f"{int(int_part):,}".replace(',', ' ')
    return f"{int_formatted},{frac_part}"


def _withdrawal_filter_sql(alias: str = "e") -> str:
    """SQL-предикат для строк-изъятий из наличных мастера (не расходы компании)."""
    return (
        f"({alias}.kind='expense' AND {alias}.method='Наличные' "
        f"AND {alias}.order_id IS NULL AND {alias}.master_id IS NOT NULL "
        f"AND ({alias}.comment ILIKE '[WDR]%' OR {alias}.comment ILIKE 'изъят%'))"
    )

async def get_cash_balance_excluding_withdrawals(conn) -> Decimal:
    """
    Остаток кассы: приход - расход, где изъятия [WDR] НЕ считаются расходом.
    """
    row = await conn.fetchrow(
        """
        SELECT
          COALESCE(SUM(CASE WHEN kind='income' THEN amount ELSE 0 END),0) AS income_sum,
          COALESCE(SUM(CASE WHEN kind='expense'
                             AND NOT (comment ILIKE '[WDR]%' OR (method='Наличные' AND order_id IS NULL AND master_id IS NOT NULL))
                            THEN amount ELSE 0 END),0) AS expense_sum
        FROM cashbook_entries
        WHERE COALESCE(is_deleted,false)=FALSE
        """
    )
    inc = Decimal(row["income_sum"] or 0)
    exp = Decimal(row["expense_sum"] or 0)
    return inc - exp


async def build_masters_kb(conn) -> ReplyKeyboardMarkup | None:
    """
    Построить reply-клавиатуру выбора мастера:
    - по одной кнопке в ряд для мастеров
    - нижний ряд: Отмена
    """
    masters = await conn.fetch(
        "SELECT id, COALESCE(first_name,'') AS fn, COALESCE(last_name,'') AS ln "
        "FROM staff WHERE role='master' AND is_active=true ORDER BY fn, ln, id"
    )

    rows: list[list[KeyboardButton]] = []
    for r in masters:
        cash_on_orders, withdrawn_total = await get_master_wallet(conn, r['id'])
        available = cash_on_orders - withdrawn_total
        if available < Decimal(0):
            available = Decimal(0)
        display_name = f"{r['fn']} {r['ln']}".strip()
        if not display_name:
            display_name = f"Мастер #{r['id']}"
        amount_str = format_money(available)
        label_core = f"{display_name} — {amount_str}₽"
        suffix = f" id:{r['id']}"
        max_len = 62
        if len(label_core) + len(suffix) > max_len:
            available_len = max_len - len(suffix) - 1  # reserve space and ellipsis
            label_core = label_core[:max(0, available_len)] + "…"
        label = label_core + suffix
        rows.append([KeyboardButton(text=label)])

    if not rows:
        return None

    rows.append([KeyboardButton(text="Отмена")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def _amo_get_cell(row: dict[str, str], key: str) -> str:
    val = row.get(key)
    if val is None:
        return ""
    if isinstance(val, str):
        return val.strip()
    return str(val).strip()


def _amo_normalize_phone(raw: str) -> tuple[str | None, str | None]:
    if not raw:
        return None, None
    cleaned = raw.replace("'", "").replace('"', "").strip()
    normalized = normalize_phone_for_db(cleaned)
    digits = only_digits(normalized)
    if len(digits) == 10:
        normalized = "+7" + digits
    elif len(digits) == 11 and digits.startswith("8"):
        normalized = "+7" + digits[1:]
        digits = "7" + digits[1:]
    elif len(digits) == 11 and digits.startswith("7"):
        normalized = "+" + digits
    elif not digits:
        return None, None
    return normalized, digits


def _amo_parse_decimal(value: str) -> Decimal | None:
    if not value:
        return None
    try:
        return Decimal(value.replace(" ", "").replace(",", "."))
    except Exception:
        return None


def _amo_parse_datetime(value: str) -> datetime | None:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    for fmt in ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%d.%m.%Y"):
        try:
            dt = datetime.strptime(value, fmt)
            if fmt == "%d.%m.%Y":
                dt = datetime.combine(dt.date(), time())
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def _ensure_dt_aware(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _format_amocrm_counters(counters: dict[str, int]) -> list[str]:
    return [
        f"Всего строк в файле: {counters['rows']}",
        f"Уникальных телефонов: {counters['phones']}",
        f"Клиентов добавлено: {counters['clients_inserted']}",
        f"Клиентов обновлено: {counters['clients_updated']}",
        f"Клиентов переведено из leads: {counters['clients_promoted']}",
        f"Лидов добавлено: {counters['leads_inserted']}",
        f"Лидов обновлено: {counters['leads_updated']}",
        f"Лидов удалено: {counters['leads_deleted']}",
        f"Пропущено без телефонов: {counters['skipped_no_phone']}",
    ]


def _last_birthday_date(birthday: date, today: date) -> date:
    year = today.year
    while True:
        try:
            candidate = birthday.replace(year=year)
        except ValueError:
            candidate = date(year, 2, 28)
        if candidate <= today:
            return candidate
        year -= 1


def _format_amocrm_counters(counters: dict[str, int]) -> list[str]:
    return [
        f"Всего строк в файле: {counters['rows']}",
        f"Уникальных телефонов: {counters['phones']}",
        f"Клиентов добавлено: {counters['clients_inserted']}",
        f"Клиентов обновлено: {counters['clients_updated']}",
        f"Клиентов переведено из leads: {counters['clients_promoted']}",
        f"Лидов добавлено: {counters['leads_inserted']}",
        f"Лидов обновлено: {counters['leads_updated']}",
        f"Лидов удалено: {counters['leads_deleted']}",
        f"Пропущено без телефонов: {counters['skipped_no_phone']}",
    ]


def _amo_split_services(value: str) -> set[str]:
    if not value:
        return set()
    raw = value.replace("\r", "\n").replace(";", "\n")
    parts = []
    for chunk in raw.split("\n"):
        if not chunk:
            continue
        parts.extend(filter(None, [p.strip() for p in chunk.split(",")]))
    return {p for p in parts if p}


def _amo_merge_services(existing: str | None, new_services: set[str]) -> tuple[str | None, bool]:
    if not new_services:
        return existing, False
    normalized_map: dict[str, str] = {}
    merged: list[str] = []
    if existing:
        for part in [p.strip() for p in re.split(r"[;,]", existing) if p.strip()]:
            key = re.sub(r"\s+", " ", part).lower()
            if key not in normalized_map:
                normalized_map[key] = part
                merged.append(part)
    changed = False
    for service in new_services:
        clean = re.sub(r"\s+", " ", service).strip()
        if not clean:
            continue
        key = clean.lower()
        if key not in normalized_map:
            normalized_map[key] = clean
            merged.append(clean)
            changed = True
    if not merged:
        return None, changed
    return ", ".join(merged), changed


def _format_amocrm_counters(counters: dict[str, int]) -> list[str]:
    return [
        f"Всего строк в файле: {counters['rows']}",
        f"Уникальных телефонов: {counters['phones']}",
        f"Клиентов добавлено: {counters['clients_inserted']}",
        f"Клиентов обновлено: {counters['clients_updated']}",
        f"Клиентов переведено из leads: {counters['clients_promoted']}",
        f"Лидов добавлено: {counters['leads_inserted']}",
        f"Лидов обновлено: {counters['leads_updated']}",
        f"Лидов удалено: {counters['leads_deleted']}",
        f"Пропущено без телефонов: {counters['skipped_no_phone']}",
    ]


async def process_amocrm_csv(
    conn: asyncpg.Connection,
    csv_text: str,
    dry_run: bool = False,
) -> tuple[dict[str, int], list[str]]:
    stream = io.StringIO(csv_text)
    reader = csv.DictReader(stream, delimiter=";")
    if reader.fieldnames:
        reader.fieldnames = [fn.strip().lstrip("\ufeff") for fn in reader.fieldnames]

    entries: dict[str, dict] = {}
    skipped_no_phone = 0
    total_rows = 0

    for idx, row in enumerate(reader, start=2):
        total_rows += 1
        sanitized = {k: (_amo_get_cell(row, k)) for k in reader.fieldnames or []}

        phone_raw = ""
        for key in [
            "Рабочий телефон (контакт)",
            "Телефон",
            "Мобильный телефон (контакт)",
            "Рабочий прямой телефон (контакт)",
            "Другой телефон (контакт)",
            "Домашний телефон (контакт)",
        ]:
            phone_raw = sanitized.get(key, "")
            if phone_raw:
                break

        normalized_phone, digits = _amo_normalize_phone(phone_raw)
        if not digits:
            skipped_no_phone += 1
            continue

        entry = entries.get(digits)
        if not entry:
            fallback_phone = None
            if not normalized_phone and len(digits) >= 10:
                fallback_phone = "+7" + digits[-10:]
            elif not normalized_phone:
                fallback_phone = "+" + digits

            entry = {
                "digits": digits,
                "normalized_phone": normalized_phone or fallback_phone,
                "best_order_dt": None,
                "best_order_row": None,
                "max_closed_at": None,
                "first_row": None,
                "full_name": None,
                "bonus_balance": None,
                "birthday_str": None,
                "services": set(),
                "order_address": None,
                "district": None,
                "address_contact": None,
                "source_contact": None,
                "source_deal": None,
                "deal_name": None,
                "last_contact_dt": None,
                "rows": [],
            }
            entries[digits] = entry

        entry["rows"].append((idx, sanitized))
        if entry["first_row"] is None:
            entry["first_row"] = sanitized

        full_name = sanitized.get("Основной контакт")
        if full_name and not entry["full_name"]:
            entry["full_name"] = full_name

        bonus_str = sanitized.get("Бонусные баллы (контакт)")
        if bonus_str and entry["bonus_balance"] is None:
            entry["bonus_balance"] = _amo_parse_decimal(bonus_str)

        birthday_val = sanitized.get("День рождения (контакт)")
        if birthday_val and not entry["birthday_str"]:
            entry["birthday_str"] = birthday_val

        service_val = sanitized.get("Услуга")
        entry["services"].update(_amo_split_services(service_val))

        order_address = sanitized.get("Адрес")
        if order_address:
            entry["order_address"] = order_address

        district_val = sanitized.get("Район города")
        if district_val:
            entry["district"] = district_val

        address_contact = sanitized.get("Адрес (контакт)")
        if address_contact:
            entry["address_contact"] = address_contact

        if sanitized.get("Источник трафика (контакт)"):
            entry["source_contact"] = sanitized["Источник трафика (контакт)"]
        elif sanitized.get("Источник траффика"):
            entry["source_contact"] = entry["source_contact"] or sanitized["Источник траффика"]

        if sanitized.get("Источник сделки"):
            entry["source_deal"] = sanitized["Источник сделки"]

        deal_name = sanitized.get("Основной контакт") or sanitized.get("Название сделки")
        if deal_name and not entry["deal_name"]:
            entry["deal_name"] = deal_name

        order_dt = _amo_parse_datetime(sanitized.get("Дата и время заказа"))
        if order_dt:
            entry["last_contact_dt"] = order_dt if entry["last_contact_dt"] is None or order_dt > entry["last_contact_dt"] else entry["last_contact_dt"]
            if entry["best_order_dt"] is None or order_dt > entry["best_order_dt"]:
                entry["best_order_dt"] = order_dt
                entry["best_order_row"] = sanitized
        elif entry["best_order_row"] is None:
            entry["best_order_row"] = sanitized

        closed_dt = _amo_parse_datetime(sanitized.get("Дата закрытия"))
        if closed_dt and (entry["max_closed_at"] is None or closed_dt > entry["max_closed_at"]):
            entry["max_closed_at"] = closed_dt

    now_ts = datetime.now(timezone.utc)
    counters = {
        "rows": total_rows,
        "phones": len(entries),
        "clients_updated": 0,
        "clients_inserted": 0,
        "clients_promoted": 0,
        "leads_inserted": 0,
        "leads_updated": 0,
        "leads_deleted": 0,
        "skipped_no_phone": skipped_no_phone,
    }
    errors: list[str] = []

    txn = conn.transaction()
    await txn.start()
    try:
        for digits, entry in entries.items():
            normalized_phone = entry["normalized_phone"] or ("+7" + digits[-10:] if len(digits) >= 10 else None)
            best_row = entry["best_order_row"] or entry["first_row"]
            if not best_row:
                errors.append(f"{digits}: нет данных по строке")
                continue

            has_address_or_order = bool(entry["order_address"] or entry["address_contact"] or entry["best_order_dt"])

            bonus_val = entry["bonus_balance"]
            birthday_val = parse_birthday_str(entry["birthday_str"]) if entry["birthday_str"] else None

            services_set = entry["services"]
            new_service_str = ", ".join(services_set) if services_set else None

            lead_source = entry["source_contact"] or entry["source_deal"] or ""
            lead_name = entry["full_name"] or entry["deal_name"] or "Без имени"
            last_address = entry["order_address"] or entry["address_contact"]
            last_contact_dt = entry["last_contact_dt"]
            max_closed_dt = entry["max_closed_at"]

            client_row = await conn.fetchrow(
                "SELECT * FROM clients WHERE phone_digits=$1",
                digits,
            )
            if client_row is None:
                client_row = await conn.fetchrow(
                    "SELECT * FROM clients WHERE regexp_replace(phone, '[^0-9]+', '', 'g') = $1 LIMIT 1",
                    digits,
                )

            if client_row:
                updates: dict[str, object] = {}
                changed = False

                if normalized_phone and client_row.get("phone") != normalized_phone:
                    updates["phone"] = normalized_phone

                if bonus_val is not None and client_row.get("bonus_balance") is None:
                    updates["bonus_balance"] = int(bonus_val)
                    changed = True

                if birthday_val and client_row.get("birthday") is None:
                    updates["birthday"] = birthday_val
                    changed = True

                if entry["best_order_dt"]:
                    existing_order = _ensure_dt_aware(client_row.get("last_order_at"))
                    candidate_dt = _ensure_dt_aware(entry["best_order_dt"])
                    if candidate_dt and (existing_order is None or candidate_dt > existing_order):
                        updates["last_order_at"] = candidate_dt
                        changed = True

                if services_set:
                    merged_services, merge_changed = _amo_merge_services(client_row.get("last_service"), services_set)
                    if merge_changed:
                        updates["last_service"] = merged_services
                        changed = True
                    elif client_row.get("last_service") is None and merged_services:
                        updates["last_service"] = merged_services
                        changed = True

                if entry["order_address"]:
                    if client_row.get("last_order_addr") != entry["order_address"]:
                        updates["last_order_addr"] = entry["order_address"]
                        changed = True

                if entry["district"]:
                    if client_row.get("district") != entry["district"]:
                        updates["district"] = entry["district"]
                        changed = True

                address_contact = entry["address_contact"]
                if address_contact:
                    if client_row.get("address") != address_contact:
                        updates["address"] = address_contact
                        changed = True

                promote = client_row.get("status") != "client" and has_address_or_order
                if promote:
                    updates["status"] = "client"
                    if entry["full_name"]:
                        updates["full_name"] = entry["full_name"]
                    changed = True

                if changed or promote:
                    updates["last_updated"] = now_ts
                    set_clauses = ", ".join(f"{col} = ${idx}" for idx, col in enumerate(updates.keys(), start=1))
                    values = list(updates.values())
                    values.append(client_row["id"])
                    await conn.execute(
                        f"UPDATE clients SET {set_clauses} WHERE id=${len(values)}",
                        *values,
                    )
                    counters["clients_updated"] += 1
                    if promote:
                        counters["clients_promoted"] += 1

                if promote:
                    lead_row = await conn.fetchrow(
                        "SELECT id FROM leads WHERE regexp_replace(phone, '[^0-9]+', '', 'g') = $1 LIMIT 1",
                        digits,
                    )
                    if lead_row:
                        await conn.execute("DELETE FROM leads WHERE id=$1", lead_row["id"])
                        counters["leads_deleted"] += 1

                continue

            lead_row = await conn.fetchrow(
                "SELECT * FROM leads WHERE regexp_replace(phone, '[^0-9]+', '', 'g') = $1 LIMIT 1",
                digits,
            )

            if has_address_or_order:
                service_str = ", ".join(sorted(services_set)) if services_set else None
                await conn.fetchval(
                    """
                    INSERT INTO clients (
                        full_name, phone, bonus_balance, birthday,
                        status, last_updated, last_order_at, last_service,
                        last_order_addr, district, address
                    )
                    VALUES ($1, $2, $3, $4, 'client', $5, $6, $7, $8, $9, $10)
                    RETURNING id
                    """,
                    entry["full_name"],
                    normalized_phone or (f"+7{digits[-10:]}" if len(digits) >= 10 else f"+{digits}"),
                    int(bonus_val) if bonus_val is not None else 0,
                    birthday_val,
                    now_ts,
                    _ensure_dt_aware(entry["best_order_dt"]),
                    service_str,
                    entry["order_address"],
                    entry["district"],
                    entry["address_contact"] or entry["order_address"],
                )
                counters["clients_inserted"] += 1

                if lead_row:
                    await conn.execute("DELETE FROM leads WHERE id=$1", lead_row["id"])
                    counters["leads_deleted"] += 1
                    counters["clients_promoted"] += 1
                continue

            lead_updates: dict[str, object] = {}
            lead_changed = False
            last_updated_value = max_closed_dt or now_ts

            if lead_row:
                if normalized_phone and lead_row.get("phone") != normalized_phone:
                    lead_updates["phone"] = normalized_phone
                if lead_row.get("name") != lead_name:
                    lead_updates["name"] = lead_name
                    lead_changed = True
                if entry["full_name"] and lead_row.get("full_name") != entry["full_name"]:
                    lead_updates["full_name"] = entry["full_name"]
                    lead_changed = True
                if lead_source and lead_row.get("source") != lead_source:
                    lead_updates["source"] = lead_source
                    lead_changed = True
                if services_set:
                    service_str = ", ".join(sorted(services_set))
                    if lead_row.get("last_service") != service_str:
                        lead_updates["last_service"] = service_str
                        lead_changed = True
                if entry["district"] and lead_row.get("district") != entry["district"]:
                    lead_updates["district"] = entry["district"]
                    lead_changed = True
                if last_address and lead_row.get("last_address") != last_address:
                    lead_updates["last_address"] = last_address
                    lead_changed = True
                if last_contact_dt:
                    existing_contact = _ensure_dt_aware(lead_row.get("last_contact_at"))
                    candidate_contact = _ensure_dt_aware(last_contact_dt)
                    if candidate_contact and (existing_contact is None or candidate_contact > existing_contact):
                        lead_updates["last_contact_at"] = candidate_contact
                        lead_changed = True
                existing_updated = _ensure_dt_aware(lead_row.get("last_updated"))
                candidate_updated = _ensure_dt_aware(last_updated_value)
                if candidate_updated and (existing_updated is None or candidate_updated > existing_updated):
                    lead_updates["last_updated"] = candidate_updated
                    lead_changed = True

                if lead_updates:
                    set_clauses = ", ".join(f"{col} = ${idx}" for idx, col in enumerate(lead_updates.keys(), start=1))
                    values = list(lead_updates.values())
                    values.append(lead_row["id"])
                    await conn.execute(
                        f"UPDATE leads SET {set_clauses} WHERE id=${len(values)}",
                        *values,
                    )
                    counters["leads_updated"] += 1
                continue

            service_str = ", ".join(sorted(services_set)) if services_set else None
            await conn.execute(
                """
                INSERT INTO leads (
                    name, phone, source, status, created_at,
                    full_name, last_contact_at, last_service,
                    district, last_address, last_updated
                )
                VALUES ($1, $2, $3, 'lead', $4, $5, $6, $7, $8, $9, $10)
                """,
                lead_name,
                normalized_phone or (f"+7{digits[-10:]}" if len(digits) >= 10 else f"+{digits}"),
                lead_source or None,
                now_ts,
                entry["full_name"],
                _ensure_dt_aware(last_contact_dt),
                service_str,
                entry["district"],
                last_address,
                _ensure_dt_aware(last_updated_value),
            )
            counters["leads_inserted"] += 1

    finally:
        if dry_run:
            await txn.rollback()
        else:
            await txn.commit()

    counters["skipped_no_phone"] = skipped_no_phone
    return counters, errors


async def _accrue_birthday_bonuses(conn: asyncpg.Connection) -> tuple[int, list[str]]:
    today_local = datetime.now(MOSCOW_TZ).date()
    errors: list[str] = []

    rows = await conn.fetch(
        """
        SELECT id, full_name, phone, bonus_balance, birthday
        FROM clients
        WHERE birthday IS NOT NULL
          AND EXTRACT(MONTH FROM birthday) = $1
          AND EXTRACT(DAY FROM birthday) = $2
        """,
        today_local.month,
        today_local.day,
    )

    if not rows:
        return 0, errors

    processed = 0
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

        amount = BONUS_BIRTHDAY_VALUE.quantize(Decimal("1"))
        expires_at = (datetime.now(MOSCOW_TZ) + timedelta(days=365)).astimezone(timezone.utc)
        try:
            await conn.execute(
                """
                INSERT INTO bonus_transactions (client_id, delta, reason, created_at, happened_at, expires_at, meta)
                VALUES ($1, $2, 'birthday', NOW(), NOW(), $3::timestamptz, jsonb_build_object('bonus_type','birthday'))
                """,
                client_id,
                int(amount),
                expires_at,
            )
            await conn.execute(
                "UPDATE clients SET bonus_balance = COALESCE(bonus_balance,0) + $1, last_updated = NOW() WHERE id=$2",
                int(amount),
                client_id,
            )
            processed += 1
        except Exception as exc:  # noqa: BLE001
            logging.exception("Birthday accrual failed for client %s: %s", client_id, exc)
            errors.append(f"client {client_id}: {exc}")

    return processed, errors


async def _expire_old_bonuses(conn: asyncpg.Connection) -> tuple[int, list[str]]:
    now_utc = datetime.now(timezone.utc)
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
        return 0, []

    expired = 0
    errors: list[str] = []
    for row in rows:
        client_id = row["client_id"]
        if client_id is None:
            continue
        delta = int(row["delta"])
        if delta <= 0:
            continue
        try:
            balance = await conn.fetchval("SELECT COALESCE(bonus_balance,0) FROM clients WHERE id=$1", client_id) or 0
            expire_amount = min(balance, delta)
            if expire_amount <= 0:
                continue
            await conn.execute(
                """
                INSERT INTO bonus_transactions (client_id, delta, reason, created_at, happened_at, meta)
                VALUES ($1, $2, 'expire', NOW(), NOW(), jsonb_build_object('expires_source', $3::text))
                """,
                client_id,
                -expire_amount,
                str(row["id"]),
            )
            await conn.execute(
                "UPDATE clients SET bonus_balance = bonus_balance - $1, last_updated = NOW() WHERE id=$2",
                expire_amount,
                client_id,
            )
            expired += 1
        except Exception as exc:  # noqa: BLE001
            logging.exception("Bonus expire failed for client %s: %s", client_id, exc)
            errors.append(f"client {client_id}: {exc}")

    return expired, errors


async def run_birthday_jobs() -> None:
    async with pool.acquire() as conn:
        accrued, accrual_errors = await _accrue_birthday_bonuses(conn)
        expired, expire_errors = await _expire_old_bonuses(conn)

    lines = [
        "🎉 Итоги по бонусам:",
        f"Начислено именинникам: {accrued}",
        f"Списано по сроку: {expired}",
    ]
    errors = (accrual_errors + expire_errors)
    if errors:
        lines.append("\nОшибки:")
        for err in errors[:10]:
            lines.append(f"- {err}")
        if len(errors) > 10:
            lines.append(f"… ещё {len(errors) - 10} строк")

    if MONEY_FLOW_CHAT_ID:
        try:
            await bot.send_message(MONEY_FLOW_CHAT_ID, "\n".join(lines))
        except Exception as exc:  # noqa: BLE001
            logging.exception("Failed to send birthday bonus summary: %s", exc)


async def schedule_daily_job(hour_msk: int, minute_msk: int, job_coro, job_name: str) -> None:
    while True:
        now_local = datetime.now(MOSCOW_TZ)
        target = now_local.replace(hour=hour_msk, minute=minute_msk, second=0, microsecond=0)
        if target <= now_local:
            target += timedelta(days=1)
        wait_seconds = (target - now_local).total_seconds()
        logging.info("Next %s run scheduled in %.0f seconds", job_name, wait_seconds)
        await asyncio.sleep(wait_seconds)
        try:
            await job_coro()
        except Exception as exc:  # noqa: BLE001
            logging.exception("Daily job %s failed: %s", job_name, exc)
            await asyncio.sleep(60)

def withdraw_nav_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Назад"), KeyboardButton(text="Отмена")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def withdraw_confirm_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="Подтвердить", callback_data="withdraw_confirm:yes")
    kb.button(text="Отмена", callback_data="withdraw_confirm:cancel")
    kb.adjust(2)
    return kb.as_markup()


def confirm_inline_kb(prefix: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="Подтвердить", callback_data=f"{prefix}:yes")
    kb.button(text="Отмена", callback_data=f"{prefix}:cancel")
    kb.adjust(2)
    return kb.as_markup()


def _is_withdraw_entry(row) -> bool:
    if row["kind"] != "expense":
        return False
    if row.get("method") != "Наличные":
        return False
    if row.get("order_id") is not None:
        return False
    if row.get("master_id") is None:
        return False
    comment = (row.get("comment") or "").strip().lower()
    return comment.startswith("[wdr]") or comment.startswith("изъят")


def _tx_type_label(row) -> str:
    if _is_withdraw_entry(row):
        return "Изъятие"
    if row["kind"] == "income":
        return "Приход"
    return "Расход"


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
    text = await _build_report_text(data.get("report_kind"), data, "day", state)
    await msg.answer(text, reply_markup=reports_period_kb())


@dp.message(ReportsFSM.waiting_pick_period, F.text == "Месяц")
async def reports_run_period_month(msg: Message, state: FSMContext):
    data = await state.get_data()
    text = await _build_report_text(data.get("report_kind"), data, "month", state)
    await msg.answer(text, reply_markup=reports_period_kb())


@dp.message(ReportsFSM.waiting_pick_period, F.text == "Год")
async def reports_run_period_year(msg: Message, state: FSMContext):
    data = await state.get_data()
    text = await _build_report_text(data.get("report_kind"), data, "year", state)
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
    # notify money-flow chat
    try:
        if MONEY_FLOW_CHAT_ID:
            balance = await get_cash_balance_excluding_withdrawals(conn)
            line1 = f"✅-{format_money(Decimal(amount))}₽ {(comment or '').strip() or 'Приход'}"
            line2 = f"Касса - {format_money(balance)}₽"
            await bot.send_message(MONEY_FLOW_CHAT_ID, line1 + "\n" + line2)
    except Exception as _e:
        logging.warning("money-flow income notify failed: %s", _e)
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
    # notify money-flow chat
    try:
        if MONEY_FLOW_CHAT_ID:
            balance = await get_cash_balance_excluding_withdrawals(conn)
            line1 = f"❎-{format_money(Decimal(amount))}₽ {(comment or '').strip() or 'Расход'}"
            line2 = f"Касса - {format_money(balance)}₽"
            await bot.send_message(MONEY_FLOW_CHAT_ID, line1 + "\n" + line2)
    except Exception as _e:
        logging.warning("money-flow expense notify failed: %s", _e)
    return tx


async def _record_order_income(
    conn: asyncpg.Connection,
    method: str,
    amount: Decimal,
    order_id: int,
    master_id: int,
    notify_label: str | None = None,
):
    norm = norm_pay_method_py(method)
    comment = f"Поступление по заказу #{order_id}"
    existing = await conn.fetchrow(
        """
        SELECT id
        FROM cashbook_entries
        WHERE order_id = $1 AND kind = 'income'
        ORDER BY id DESC
        LIMIT 1
        """,
        order_id,
    )
    if existing:
        tx = await conn.fetchrow(
            """
            UPDATE cashbook_entries
            SET method=$1,
                amount=$2,
                comment=$3,
                master_id=$4
            WHERE id=$5
            RETURNING id, happened_at
            """,
            norm,
            amount,
            comment,
            master_id,
            existing["id"],
        )
    else:
        tx = await conn.fetchrow(
            """
            INSERT INTO cashbook_entries(kind, method, amount, comment, order_id, master_id, happened_at)
            VALUES ('income', $1, $2, $3, $4, $5, now())
            RETURNING id, happened_at
            """,
            norm,
            amount,
            comment,
            order_id,
            master_id,
        )
    # notify money-flow chat
    try:
        if MONEY_FLOW_CHAT_ID:
            balance = await get_cash_balance_excluding_withdrawals(conn)
            if notify_label:
                display = f"{notify_label} / Заказ №{order_id}"
            else:
                display = comment
            line1 = f"✅-{format_money(Decimal(amount))}₽ {display}"
            line2 = f"Касса - {format_money(balance)}₽"
            await bot.send_message(MONEY_FLOW_CHAT_ID, line1 + "\n" + line2)
    except Exception as _e:
        logging.warning("money-flow order income notify failed: %s", _e)
    return tx


async def _record_withdrawal(
    conn: asyncpg.Connection,
    master_id: int,
    amount: Decimal,
    comment: str = "Изъятие",
    master_label: str | None = None,
):
    # Изъятие — внутреннее перемещение: уменьшает наличные у мастера, но не влияет на прибыль.
    # Храним в общей таблице cashbook_entries, помечаем [WDR], чтобы исключить из P&L-отчётов.
    note_parts = ["[WDR]"]
    if master_label:
        note_parts.append(master_label.strip())
    if comment:
        note_parts.append(comment.strip())
    final_comment = " — ".join(filter(None, note_parts))

    tx = await conn.fetchrow(
        """
        INSERT INTO cashbook_entries(kind, method, amount, comment, order_id, master_id, happened_at)
        VALUES ('expense', 'Наличные', $1, $2, NULL, $3, now())
        RETURNING id, happened_at
        """,
        amount,
        final_comment,
        master_id,
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
        BotCommand(command="daily_cash", description="Касса за сегодня"),
        BotCommand(command="daily_profit", description="Прибыль за сегодня"),
        BotCommand(command="daily_orders", description="Заказы за сегодня"),
        BotCommand(command="my_daily", description="Моя сводка за сегодня"),
        BotCommand(command="masters_all", description="Полный список мастеров"),
        BotCommand(command="import_amocrm", description="Импорт AmoCRM CSV"),
        BotCommand(command="bonus_backfill", description="Пересчитать бонусы"),
        BotCommand(command="tx_remove", description="Удалить транзакцию"),
        BotCommand(command="order_remove", description="Удалить заказ"),
    ]
    await bot.set_my_commands(cmds, scope=BotCommandScopeDefault())

# ===== Admin commands (must be defined after dp is created) =====
@dp.message(Command("list_masters"))
async def list_masters(msg: Message):
    if not await has_permission(msg.from_user.id, "add_master"):
        return await msg.answer("Только для администраторов.")
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT s.id,
                   s.tg_user_id,
                   COALESCE(s.first_name, '') AS fn,
                   COALESCE(s.last_name, '')  AS ln,
                   COALESCE(s.phone, '')      AS phone
            FROM staff s
            WHERE s.role = 'master'
              AND s.is_active = true
            ORDER BY fn, ln, id
            """
        )
    if not rows:
        return await msg.answer("Активных мастеров нет.")
    lines = [
        f"#{r['id']} {r['fn']} {r['ln']} | tg={r['tg_user_id']} | {r['phone'] or 'без телефона'}"
        for r in rows
    ]
    await msg.answer("Активные мастера:\n" + "\n".join(lines))


@dp.message(Command("masters_all"))
async def masters_all(msg: Message):
    if not await has_permission(msg.from_user.id, "add_master"):
        return await msg.answer("Только для администраторов.")
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, tg_user_id, is_active, COALESCE(first_name,'') AS fn, COALESCE(last_name,'') AS ln, COALESCE(phone,'') AS phone "
            "FROM staff WHERE role='master' ORDER BY is_active DESC, fn, ln, id"
        )
    if not rows:
        return await msg.answer("В базе мастеров не найдено.")
    active = [r for r in rows if r["is_active"]]
    inactive = [r for r in rows if not r["is_active"]]

    def fmt(r):
        return f"#{r['id']} {r['fn']} {r['ln']} | tg={r['tg_user_id']} | {r['phone'] or 'без телефона'}"

    parts: list[str] = []
    if active:
        parts.append("Активные:")
        parts.extend(fmt(r) for r in active)
    if inactive:
        if active:
            parts.append("")
        parts.append("Неактивные:")
        parts.extend(fmt(r) for r in inactive)
    await msg.answer("\n".join(parts))

@dp.message(Command("add_master"))
async def add_master(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "add_master"):
        return await msg.answer("Только для администраторов.")
    parts = msg.text.split(maxsplit=1)
    await state.clear()
    if len(parts) >= 2:
        try:
            tg_id = int(parts[1].lstrip("@"))
        except Exception:
            await state.set_state(AddMasterFSM.waiting_tg_id)
            return await msg.answer("Введите tg id мастера (число):", reply_markup=admin_cancel_kb())
        await state.update_data(tg_id=tg_id)
        await state.set_state(AddMasterFSM.waiting_phone)
        return await msg.answer("Введите телефон мастера (формат: +7XXXXXXXXXX или 8/9...):", reply_markup=admin_cancel_kb())

    await state.set_state(AddMasterFSM.waiting_tg_id)
    await msg.answer("Введите tg id мастера (число):", reply_markup=admin_cancel_kb())


@dp.message(AddMasterFSM.waiting_tg_id)
async def add_master_tg(msg: Message, state: FSMContext):
    raw = (msg.text or "").strip()
    if raw.lower() == "отмена":
        return await add_master_cancel(msg, state)

    candidate = raw.lstrip("@")
    if not candidate.isdigit():
        return await msg.answer("tg id должен быть числом. Введите ещё раз или нажмите «Отмена».", reply_markup=admin_cancel_kb())
    tg_id = int(candidate)
    if tg_id <= 0:
        return await msg.answer("tg id должен быть положительным числом.", reply_markup=admin_cancel_kb())

    await state.update_data(tg_id=tg_id)
    await state.set_state(AddMasterFSM.waiting_phone)
    await msg.answer("Введите телефон мастера (формат: +7XXXXXXXXXX или 8/9...):", reply_markup=admin_cancel_kb())


@dp.message(AddMasterFSM.waiting_phone)
async def add_master_phone(msg: Message, state: FSMContext):
    txt = (msg.text or "").strip()
    if txt.lower() == "отмена":
        return await add_master_cancel(msg, state)

    phone_norm = normalize_phone_for_db(txt)
    if not phone_norm or not phone_norm.startswith("+7"):
        return await msg.answer("Не распознал телефон. Пример: +7XXXXXXXXXX. Введите ещё раз.", reply_markup=admin_cancel_kb())

    await state.update_data(phone=phone_norm)
    await state.set_state(AddMasterFSM.waiting_name)
    await msg.answer("Введите имя мастера:", reply_markup=admin_cancel_kb())


@dp.message(AddMasterFSM.waiting_name)
async def add_master_name(msg: Message, state: FSMContext):
    name_raw = (msg.text or "").strip()
    if name_raw.lower() == "отмена":
        return await add_master_cancel(msg, state)
    if len(name_raw) < 2:
        return await msg.answer("Имя должно содержать минимум 2 символа. Введите ещё раз.", reply_markup=admin_cancel_kb())

    data = await state.get_data()
    tg_id = data.get("tg_id")
    phone = data.get("phone")
    if tg_id is None or phone is None:
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        return await msg.answer("Сессия сброшена. Начните заново.", reply_markup=admin_root_kb())

    parts = name_raw.split(maxsplit=1)
    first_name = parts[0]
    last_name = parts[1] if len(parts) > 1 else ""

    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO staff(tg_user_id, role, is_active, first_name, last_name, phone) "
            "VALUES ($1,'master',true,$2,$3,$4) "
            "ON CONFLICT (tg_user_id) DO UPDATE SET role='master', is_active=true, first_name=$2, last_name=$3, phone=$4",
            int(tg_id), first_name, last_name, phone,
        )

    lines = [
        "✅ Мастер добавлен",
        f"Имя: {name_raw}",
        f"Телефон: {phone}",
        f"tg id: {tg_id}",
        f"tg_user: tg://user?id={tg_id}",
    ]

    await state.clear()
    await state.set_state(AdminMenuFSM.root)
    await msg.answer("\n".join(lines), reply_markup=admin_root_kb())


async def add_master_cancel(msg: Message, state: FSMContext):
    await state.clear()
    await state.set_state(AdminMenuFSM.root)
    await msg.answer("Добавление мастера отменено.", reply_markup=admin_root_kb())


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
    async with pool.acquire() as conn:
        kb = await build_masters_kb(conn)
    if kb is None:
        await state.set_state(AdminMenuFSM.root)
        return await msg.answer(
            "Нет активных мастеров с наличными для изъятия.",
            reply_markup=admin_root_kb(),
        )
    await state.set_state(WithdrawFSM.waiting_master)
    await state.update_data(
        withdraw_master_id=None,
        withdraw_master_name=None,
        withdraw_amount=None,
        withdraw_available=None,
        withdraw_comment="",
    )
    return await msg.answer(
        "Выберите мастера, у которого нужно изъять наличные:",
        reply_markup=kb,
    )


@dp.message(AdminMenuFSM.root, F.text == "Клиенты")
async def admin_clients_root(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "edit_client"):
        return await msg.answer("Только для администраторов.")
    await state.clear()
    await state.set_state(AdminClientsFSM.find_wait_phone)
    await msg.answer("Введите номер телефона клиента (8/ +7/ 9...):", reply_markup=client_find_phone_kb())


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
            """
            SELECT s.id,
                   s.tg_user_id,
                   COALESCE(s.first_name,'') AS fn,
                   COALESCE(s.last_name,'')  AS ln,
                   COALESCE(s.phone,'')      AS phone
            FROM staff s
            WHERE s.role = 'master'
              AND s.is_active = true
            ORDER BY fn, ln, id
            """
        )
    if not rows:
        await msg.answer("Активных мастеров нет.", reply_markup=admin_masters_kb())
        return

    lines = [
        f"#{r['id']} {r['fn']} {r['ln']} | tg={r['tg_user_id']} | {r['phone'] or 'без телефона'}"
        for r in rows
    ]
    await msg.answer("Активные мастера:\n" + "\n".join(lines), reply_markup=admin_masters_kb())


@dp.message(AdminMenuFSM.masters, F.text == "Добавить мастера")
async def admin_masters_add(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "add_master"):
        return await msg.answer("Только для администраторов.")
    await state.clear()
    await state.set_state(AddMasterFSM.waiting_tg_id)
    await msg.answer("Введите tg id мастера (число):", reply_markup=admin_cancel_kb())


@dp.message(AdminMenuFSM.masters, F.text == "Деактивировать мастера")
async def admin_masters_remove_start(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "add_master"):
        return await msg.answer("Только для администраторов.")
    await state.set_state(AdminMastersFSM.remove_wait_phone)
    await msg.answer("Введите телефон мастера (8/+7/9...) или нажмите «Назад».", reply_markup=admin_masters_remove_kb())


@dp.message(AdminMastersFSM.remove_wait_phone)
async def admin_masters_remove_phone(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "add_master"):
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        return await msg.answer("Только для администраторов.", reply_markup=admin_root_kb())
    text = (msg.text or "").strip().lower()
    if text == "отмена":
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        return await msg.answer("Меню администратора:", reply_markup=admin_root_kb())
    if text == "назад":
        await state.set_state(AdminMenuFSM.masters)
        return await msg.answer("Раздел «Мастера»:", reply_markup=admin_masters_kb())
    phone = normalize_phone_for_db(msg.text)
    if not phone or not phone.startswith("+7"):
        return await msg.answer("Неверный телефон. Пример: +7XXXXXXXXXX. Введите ещё раз.", reply_markup=admin_masters_remove_kb())
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
    cash_on_orders = await get_master_cash_on_orders(conn, master_id)
    withdrawn = await conn.fetchval(
        """
        SELECT COALESCE(SUM(amount),0)
        FROM cashbook_entries
        WHERE kind='expense' AND method='Наличные'
          AND master_id=$1 AND order_id IS NULL
          AND COALESCE(is_deleted,false)=FALSE
          AND (comment ILIKE '[WDR]%' OR comment ILIKE 'изъят%')
        """,
        master_id,
    )

    return Decimal(cash_on_orders or 0), Decimal(withdrawn or 0)


def parse_amount_ru(text: str) -> tuple[Decimal | None, dict]:
    raw = (text or "").strip()
    dbg: dict[str, object] = {"raw": raw}

    normalized = raw.replace("\u00A0", " ")  # NBSP → space
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


@dp.message(WithdrawFSM.waiting_amount, F.text.lower() == "отмена")
async def withdraw_amount_cancel(msg: Message, state: FSMContext):
    logging.info(f"[withdraw] step=amount_cancel user={msg.from_user.id} text={msg.text}")
    await state.clear()
    await state.set_state(AdminMenuFSM.root)
    await msg.answer("Операция отменена.", reply_markup=admin_root_kb())


@dp.message(WithdrawFSM.waiting_amount, F.text.lower() == "назад")
async def withdraw_amount_back(msg: Message, state: FSMContext):
    logging.info(f"[withdraw] step=amount_back user={msg.from_user.id} text={msg.text}")
    async with pool.acquire() as conn:
        kb = await build_masters_kb(conn)
    if kb is None:
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        return await msg.answer("Нет активных мастеров для изъятия.", reply_markup=admin_root_kb())
    await state.update_data(
        withdraw_master_id=None,
        withdraw_master_name=None,
        withdraw_amount=None,
        withdraw_available=None,
        withdraw_comment="",
    )
    await state.set_state(WithdrawFSM.waiting_master)
    return await msg.answer("Выберите мастера, у которого нужно изъять наличные:", reply_markup=kb)


@dp.message(WithdrawFSM.waiting_amount, F.content_type == ContentType.TEXT)
async def withdraw_amount_got(msg: Message, state: FSMContext):
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

    async with pool.acquire() as conn:
        data = await state.get_data()
        master_id = data.get("withdraw_master_id")
        if not master_id:
            kb = await build_masters_kb(conn)
            await state.set_state(WithdrawFSM.waiting_master)
            if kb is None:
                await state.clear()
                await state.set_state(AdminMenuFSM.root)
                return await msg.answer("Нет активных мастеров для изъятия.", reply_markup=admin_root_kb())
            return await msg.answer("Сначала выберите мастера для изъятия.", reply_markup=kb)
        master_id = int(master_id)
        master_row = await conn.fetchrow(
            "SELECT COALESCE(first_name,'') AS fn, COALESCE(last_name,'') AS ln FROM staff WHERE id=$1",
            master_id,
        )
        if not master_row:
            kb = await build_masters_kb(conn)
            await state.update_data(
                withdraw_master_id=None,
                withdraw_master_name=None,
            )
            await state.set_state(WithdrawFSM.waiting_master)
            if kb is None:
                await state.clear()
                await state.set_state(AdminMenuFSM.root)
                return await msg.answer("Мастер не найден. Попробуйте снова из меню.", reply_markup=admin_root_kb())
            return await msg.answer("Мастер не найден. Выберите другого мастера.", reply_markup=kb)
        cash_on_orders, withdrawn_total = await get_master_wallet(conn, master_id)
        available = cash_on_orders - withdrawn_total
        if available < Decimal(0):
            available = Decimal(0)
    if amount > available:
        return await msg.answer(
            f"Можно изъять не больше {format_money(available)}₽. Введите сумму снова:",
            reply_markup=withdraw_nav_kb(),
        )

    await state.update_data(
        withdraw_amount=str(amount),
        withdraw_available=str(available),
        withdraw_comment="",
    )
    amount_str = format_money(amount)
    left_after = format_money(available - amount)

    await state.set_state(WithdrawFSM.waiting_confirm)
    return await msg.answer(
        "\n".join([
            f"Мастер: {(master_row['fn'] or '').strip()} {(master_row['ln'] or '').strip()}".strip() or f'ID {master_id}',
            f"Сумма изъятия: {amount_str}₽",
            f"Осталось на руках: {left_after}₽",
        ]),
        reply_markup=withdraw_confirm_kb(),
    )


@dp.message(WithdrawFSM.waiting_master, F.text.lower() == "отмена")
async def withdraw_master_cancel(msg: Message, state: FSMContext):
    logging.info(f"[withdraw] step=master_cancel user={msg.from_user.id} text={msg.text}")
    await state.clear()
    await state.set_state(AdminMenuFSM.root)
    await msg.answer("Операция отменена.", reply_markup=admin_root_kb())


@dp.message(WithdrawFSM.waiting_master)
async def withdraw_master_pick(msg: Message, state: FSMContext):
    logging.info(f"[withdraw] step=master_pick user={msg.from_user.id} text={msg.text}")
    txt = (msg.text or "").strip()

    master_id: int | None = None
    match = re.search(r"#(\d+)", txt)
    if match:
        master_id = int(match.group(1))
    else:
        match = re.search(r"id:(\d+)", txt, re.IGNORECASE)
        if match:
            master_id = int(match.group(1))
        elif txt.isdigit():
            master_id = int(txt)

    async with pool.acquire() as conn:
        master_row = None
        if master_id is not None:
            master_row = await conn.fetchrow(
                """
                SELECT id, COALESCE(first_name,'') AS fn, COALESCE(last_name,'') AS ln, is_active
                FROM staff
                WHERE id=$1
                """,
                master_id,
            )
        else:
            match = re.search(r"tg[:\s]*(\d+)", txt, re.IGNORECASE)
            if match:
                tg_id = int(match.group(1))
                master_row = await conn.fetchrow(
                    """
                    SELECT id, COALESCE(first_name,'') AS fn, COALESCE(last_name,'') AS ln, is_active
                    FROM staff
                    WHERE tg_user_id=$1 AND role='master'
                    """,
                    tg_id,
                )

        if not master_row or not master_row["is_active"]:
            kb = await build_masters_kb(conn)
            if kb is None:
                await state.clear()
                await state.set_state(AdminMenuFSM.root)
                return await msg.answer("Нет активных мастеров для изъятия.", reply_markup=admin_root_kb())
            return await msg.answer("Мастер недоступен или не выбран. Выберите другого мастера.", reply_markup=kb)

        cash_on_orders, withdrawn_total = await get_master_wallet(conn, master_row["id"])

    available = cash_on_orders - withdrawn_total
    if available <= 0:
        return await msg.answer("У этого мастера нет наличных для изъятия. Выберите другого мастера.")

    display_name = f"{(master_row['fn'] or '').strip()} {(master_row['ln'] or '').strip()}".strip() or f"Мастер {master_row['id']}"

    await state.update_data(
        withdraw_master_id=master_row["id"],
        withdraw_master_name=display_name,
        withdraw_available=str(available),
        withdraw_amount=None,
        withdraw_comment="",
    )
    await state.set_state(WithdrawFSM.waiting_amount)
    available_str = format_money(available)
    return await msg.answer(
        f"{display_name}: на руках {available_str}₽.\nВведите сумму изъятия:",
        reply_markup=withdraw_nav_kb(),
    )


@dp.callback_query(WithdrawFSM.waiting_confirm)
async def withdraw_confirm_handler(query: CallbackQuery, state: FSMContext):
    data = (query.data or "").strip()

    if data == "withdraw_confirm:cancel":
        await query.answer()
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        await query.message.answer("Операция отменена.", reply_markup=admin_root_kb())
        return

    if data == "withdraw_confirm:yes":
        await query.answer()

        s = await state.get_data()
        try:
            master_id = int(s.get("withdraw_master_id"))
            amount = Decimal(str(s.get("withdraw_amount") or "0"))
        except Exception:
            await state.clear()
            await state.set_state(AdminMenuFSM.root)
            await query.message.answer("Сессия изъятия потеряна. Попробуйте снова.", reply_markup=admin_root_kb())
            return

        comment = (s.get("withdraw_comment") or "").strip() or "Без комментария"
        master_name = s.get("withdraw_master_name") or "—"

        async with pool.acquire() as conn:
            cash_on_orders, withdrawn_total = await get_master_wallet(conn, master_id)
            current_available = cash_on_orders - withdrawn_total
            if current_available < Decimal(0):
                current_available = Decimal(0)

            if amount > current_available:
                await state.set_state(WithdrawFSM.waiting_amount)
                await query.message.answer(
                    f"Сейчас у мастера доступно только {format_money(current_available)}₽. Введите сумму снова:",
                    reply_markup=withdraw_nav_kb(),
                )
                return

            master_label = f"{master_name} (id:{master_id})"
            tx = await _record_withdrawal(conn, master_id, amount, comment, master_label)

            cash_on_orders, withdrawn_total = await get_master_wallet(conn, master_id)

        available_after = cash_on_orders - withdrawn_total
        if available_after < Decimal(0):
            available_after = Decimal(0)

        tx_id = tx["id"]
        dt_str = tx["happened_at"].strftime("%d.%m.%Y %H:%M")
        amount_str = format_money(amount)
        avail_str = format_money(available_after)

        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        await query.message.answer(
            "\n".join([
                f"Изъятие №{tx_id}",
                dt_str,
                f"Мастер: {master_name}",
                f"Изъято: {amount_str}₽",
                f"Осталось на руках: {avail_str}₽",
            ]),
            reply_markup=admin_root_kb(),
        )
        # notify orders-confirm chat (З/П = «Заказы подтверждения»)
        try:
            if ORDERS_CONFIRM_CHAT_ID:
                lines = [
                    "Изъятие наличных:",
                    f"{master_name}",
                    f"Сумма {amount_str}₽",
                    f"Осталось на руках {avail_str}₽",
                ]
                await bot.send_message(ORDERS_CONFIRM_CHAT_ID, "\n".join(lines))
        except Exception as _e:
            logging.warning("withdrawal notify failed: %s", _e)
        return

    else:
        await query.answer("Неизвестное действие", show_alert=True)
        return


@dp.message(
    StateFilter(
        AdminClientsFSM.find_wait_phone,
        AdminClientsFSM.view_client,
        AdminClientsFSM.edit_wait_phone,
        AdminClientsFSM.edit_pick_field,
        AdminClientsFSM.edit_wait_value,
    ),
    F.text == "Назад",
)
async def admin_clients_states_back(msg: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state == AdminClientsFSM.edit_wait_value.state:
        await state.set_state(AdminClientsFSM.edit_pick_field)
        await msg.answer("Что изменить?", reply_markup=client_edit_fields_kb())
        return

    if current_state == AdminClientsFSM.edit_pick_field.state:
        data = await state.get_data()
        client_id = data.get("client_id")
        if client_id:
            async with pool.acquire() as conn:
                rec = await conn.fetchrow(
                    "SELECT id, full_name, phone, birthday, bonus_balance, status FROM clients WHERE id=$1",
                    client_id,
                )
            if rec:
                await state.set_state(AdminClientsFSM.view_client)
                await msg.answer(_fmt_client_row(rec), reply_markup=client_view_kb())
                return
        await state.set_state(AdminClientsFSM.find_wait_phone)
        await msg.answer("Введите номер телефона клиента (8/ +7/ 9...):", reply_markup=client_find_phone_kb())
        return

    if current_state == AdminClientsFSM.view_client.state:
        await state.update_data(client_id=None, edit_field=None)
        await state.set_state(AdminClientsFSM.find_wait_phone)
        await msg.answer("Введите номер телефона клиента (8/ +7/ 9...):", reply_markup=client_find_phone_kb())
        return

    if current_state == AdminClientsFSM.edit_wait_phone.state:
        await state.set_state(AdminClientsFSM.find_wait_phone)
        await msg.answer("Введите номер телефона клиента (8/ +7/ 9...):", reply_markup=client_find_phone_kb())
        return

    # find_wait_phone or fallback — выходим в меню администратора
    await state.clear()
    await state.set_state(AdminMenuFSM.root)
    await msg.answer("\u2060", reply_markup=admin_root_kb())


@dp.message(
    StateFilter(
        AdminClientsFSM.find_wait_phone,
        AdminClientsFSM.view_client,
        AdminClientsFSM.edit_wait_phone,
        AdminClientsFSM.edit_pick_field,
        AdminClientsFSM.edit_wait_value,
    ),
    F.text == "Отмена",
)
async def admin_clients_states_cancel(msg: Message, state: FSMContext):
    await state.clear()
    await state.set_state(AdminMenuFSM.root)
    await msg.answer("\u2060", reply_markup=admin_root_kb())


@dp.message(AdminClientsFSM.find_wait_phone)
async def client_find_got_phone(msg: Message, state: FSMContext):
    async with pool.acquire() as conn:
        rec = await _find_client_by_phone(conn, msg.text)
    if not rec:
        return await msg.answer("Клиент не найден. Попробуйте ещё раз.", reply_markup=client_find_phone_kb())
    await state.update_data(client_id=rec["id"], edit_field=None)
    await state.set_state(AdminClientsFSM.view_client)
    await msg.answer(f"Клиент найден:\n{_fmt_client_row(rec)}", reply_markup=client_view_kb())


@dp.message(AdminClientsFSM.view_client, F.text.casefold() == "редактировать")
async def client_view_edit(msg: Message, state: FSMContext):
    data = await state.get_data()
    client_id = data.get("client_id")
    if not client_id:
        await state.set_state(AdminClientsFSM.find_wait_phone)
        return await msg.answer(
            "Сессия сброшена. Введите номер телефона клиента (8/ +7/ 9...):",
            reply_markup=client_find_phone_kb(),
        )
    await state.update_data(edit_field=None)
    await state.set_state(AdminClientsFSM.edit_pick_field)
    await msg.answer("Что изменить?", reply_markup=client_edit_fields_kb())


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
            bonus_row = await conn.fetchrow(
                "SELECT bonus_balance FROM clients WHERE id=$1",
                client_id,
            )
            current_bonus = int(bonus_row["bonus_balance"] or 0) if bonus_row else 0
            new_bonus = current_bonus + delta
            if new_bonus < 0:
                new_bonus = 0
            await conn.execute(
                "UPDATE clients SET bonus_balance=$1, last_updated=NOW() WHERE id=$2",
                new_bonus,
                client_id,
            )
        updated_rec = await conn.fetchrow(
            "SELECT id, full_name, phone, birthday, bonus_balance, status FROM clients WHERE id=$1",
            client_id,
        )
    if not updated_rec:
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        return await msg.answer("Клиент не найден.", reply_markup=admin_root_kb())

    await state.update_data(client_id=client_id, edit_field=None)
    await state.set_state(AdminClientsFSM.edit_pick_field)
    await msg.answer(f"Клиент обновлён:\n{_fmt_client_row(updated_rec)}")
    await msg.answer("Что изменить?", reply_markup=client_edit_fields_kb())


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
            "/daily_cash — сводка по кассе за сегодня\n"
            "\n"
            "/daily_profit — сводка по прибыли за сегодня и всё время\n"
            "\n"
            "/daily_orders — сводка по заказам мастеров за сегодня\n"
            "\n"
            "/import_amocrm — загрузить CSV выгрузку из AmoCRM\n"
            "\n"
            "/bonus_backfill — пересчитать историю бонусов (только суперадмин)\n"
            "\n"
            "/tx_remove — удалить приход/расход/изъятие (только суперадмин)\n"
            "\n"
            "/order_remove — удалить заказ (только суперадмин)\n"
            "\n"
            "/masters_all — полный список мастеров\n"
            "\n"
            "/order — открыть добавление заказа (клавиатура мастера)\n"
        )
    elif role == "master":
        text = (
            "Команды мастера:\n"
            "/whoami — кто я, мои права\n"
            "\n"
            "/mysalary [period] — моя зарплата (day/week/month/year)\n"
            "\n"
            "/myincome — мои оплаты за сегодня по типам\n"
            "\n"
            "/my_daily — ежедневная сводка (заказы, оплаты, ЗП, наличка)\n"
            "\n"
            "Для оформления заказа используйте кнопки внизу."
        )
    else:
        text = (
            "Доступные команды:\n"
            "/whoami — кто я, мои права\n"
            "\n"
            "Если вы мастер или администратор и не видите нужные команды — обратитесь к менеджеру для выдачи прав."
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
    # Исключаем изъятия из расходов компании, так как это внутреннее движение (наличные мастеров → касса)
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
        # Исключаем изъятия из расходов компании, так как это внутреннее движение (наличные мастеров → касса)
        exp = await conn.fetchval(
            f"""
            SELECT COALESCE(SUM(c.amount), 0)::numeric(12,2)
            FROM cashbook_entries c
            WHERE c.kind='expense'
              AND NOT ({_withdrawal_filter_sql("c")})
              AND c.happened_at >= {start_sql} AND c.happened_at < {end_sql}
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
              WHERE c.kind='expense'
                AND NOT ({_withdrawal_filter_sql("c")})
                AND c.happened_at >= {start_sql} AND c.happened_at < {end_sql}
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


def _normalize_report_kind(kind: str | None) -> str:
    mapping = {
        "master_orders": "Мастер/Заказы/Оплаты",
        "master_salary": "Мастер/Зарплата",
        "paytypes": "Типы оплат",
    }
    if not kind:
        return ""
    return mapping.get(kind, kind)


def _report_period_bounds(period: str) -> tuple[str, str, str] | None:
    period = (period or "").lower()
    mapping = {
        "day": ("date_trunc('day', NOW())", "date_trunc('day', NOW()) + interval '1 day'", "за сегодня"),
        "week": ("date_trunc('week', NOW())", "date_trunc('week', NOW()) + interval '1 week'", "за неделю"),
        "month": ("date_trunc('month', NOW())", "date_trunc('month', NOW()) + interval '1 month'", "за месяц"),
        "year": ("date_trunc('year', NOW())", "date_trunc('year', NOW()) + interval '1 year'", "за год"),
    }
    return mapping.get(period)


def _format_payment_summary(method_totals: dict[str, Decimal]) -> str:
    """
    Собрать строку с разбивкой по типам оплат. Показываем только ненулевые значения.
    """
    if not method_totals:
        return "нет данных"
    ordered = list(PAYMENT_METHODS) + [GIFT_CERT_LABEL]
    seen = set()
    parts: list[str] = []
    for label in ordered:
        value = method_totals.get(label)
        if value and value != Decimal(0):
            parts.append(f"{label}: {format_money(value)}₽")
            seen.add(label)
    for label in sorted(method_totals.keys()):
        if label in seen:
            continue
        value = method_totals[label]
        if value and value != Decimal(0):
            parts.append(f"{label}: {format_money(Decimal(value))}₽")
    return "; ".join(parts) if parts else "нет данных"


async def build_daily_cash_summary_text() -> str:
    start_sql = "date_trunc('day', NOW())"
    end_sql = "date_trunc('day', NOW()) + interval '1 day'"
    async with pool.acquire() as conn:
        totals = await conn.fetchrow(
            f"""
            SELECT
              COALESCE(SUM(CASE WHEN c.kind='income' THEN c.amount ELSE 0 END),0)::numeric(12,2) AS income,
              COALESCE(SUM(CASE WHEN c.kind='expense' AND NOT ({_withdrawal_filter_sql("c")}) THEN c.amount ELSE 0 END),0)::numeric(12,2) AS expense
            FROM cashbook_entries c
            WHERE c.happened_at >= {start_sql} AND c.happened_at < {end_sql}
            """
        )
        pay_rows = await conn.fetch(
            f"""
            SELECT COALESCE(method,'прочее') AS method,
                   COALESCE(SUM(amount),0)::numeric(12,2) AS total
            FROM cashbook_entries
            WHERE kind='income'
              AND happened_at >= {start_sql} AND happened_at < {end_sql}
            GROUP BY 1
            """
        )
        balance = await get_cash_balance_excluding_withdrawals(conn)

    income = Decimal(totals["income"] or 0)
    expense = Decimal(totals["expense"] or 0)
    method_totals: dict[str, Decimal] = {}
    for row in pay_rows:
        method = row["method"] or "прочее"
        method_totals[method] = Decimal(row["total"] or 0)
    lines = [
        "📊 Касса — за сегодня",
        f"Приход: {format_money(income)}₽",
        f"Расход: {format_money(expense)}₽",
        f"Остаток: {format_money(balance)}₽",
    ]
    lines.append("Типы оплат: " + _format_payment_summary(method_totals))
    return "\n".join(lines)


async def build_profit_summary_text() -> str:
    start_sql = "date_trunc('day', NOW())"
    end_sql = "date_trunc('day', NOW()) + interval '1 day'"
    async with pool.acquire() as conn:
        revenue_day = await conn.fetchval(
            f"""
            SELECT COALESCE(SUM(o.amount_cash), 0)::numeric(12,2)
            FROM orders o
            WHERE o.created_at >= {start_sql} AND o.created_at < {end_sql}
            """
        )
        expense_day = await conn.fetchval(
            f"""
            SELECT COALESCE(SUM(c.amount), 0)::numeric(12,2)
            FROM cashbook_entries c
            WHERE c.kind='expense'
              AND NOT ({_withdrawal_filter_sql("c")})
              AND c.happened_at >= {start_sql} AND c.happened_at < {end_sql}
            """
        )
        revenue_total = await conn.fetchval(
            "SELECT COALESCE(SUM(amount_cash), 0)::numeric(12,2) FROM orders"
        )
        expense_total = await conn.fetchval(
            f"""
            SELECT COALESCE(SUM(amount), 0)::numeric(12,2)
            FROM cashbook_entries c
            WHERE c.kind='expense'
              AND NOT ({_withdrawal_filter_sql("c")})
            """
        )

    revenue_day = Decimal(revenue_day or 0)
    expense_day = Decimal(expense_day or 0)
    revenue_total = Decimal(revenue_total or 0)
    expense_total = Decimal(expense_total or 0)
    profit_day = revenue_day - expense_day
    profit_total = revenue_total - expense_total
    return "\n".join([
        "📈 Прибыль",
        f"За сегодня: {format_money(profit_day)}₽ (выручка {format_money(revenue_day)}₽, расходы {format_money(expense_day)}₽)",
        f"За всё время: {format_money(profit_total)}₽ (выручка {format_money(revenue_total)}₽, расходы {format_money(expense_total)}₽)",
    ])


async def build_daily_orders_admin_summary_text() -> str:
    start_sql = "date_trunc('day', NOW())"
    end_sql = "date_trunc('day', NOW()) + interval '1 day'"
    async with pool.acquire() as conn:
        masters = await conn.fetch(
            "SELECT id, COALESCE(first_name,'') AS fn, COALESCE(last_name,'') AS ln "
            "FROM staff WHERE role='master' AND is_active=true ORDER BY fn, ln, id"
        )
        if not masters:
            return "Мастеров в активном статусе нет."

        total_orders = 0
        total_method_totals: dict[str, Decimal] = {}
        total_on_hand = Decimal(0)
        lines = ["📋 Заказы по мастерам — за сегодня"]

        for m in masters:
            stats = await conn.fetchrow(
                f"""
                SELECT
                  COUNT(*) AS cnt,
                  COALESCE(SUM(CASE WHEN payment_method='Наличные'              THEN amount_cash  ELSE 0 END),0)::numeric(12,2) AS s_cash,
                  COALESCE(SUM(CASE WHEN payment_method='Карта Женя'            THEN amount_cash  ELSE 0 END),0)::numeric(12,2) AS s_card_jenya,
                  COALESCE(SUM(CASE WHEN payment_method='Карта Дима'            THEN amount_cash  ELSE 0 END),0)::numeric(12,2) AS s_card_dima,
                  COALESCE(SUM(CASE WHEN payment_method='р/с'                   THEN amount_cash  ELSE 0 END),0)::numeric(12,2) AS s_rs,
                  COALESCE(SUM(CASE WHEN payment_method='Подарочный сертификат' THEN amount_total ELSE 0 END),0)::numeric(12,2) AS s_gift_total
                FROM orders o
                WHERE o.master_id = $1
                  AND o.created_at >= {start_sql}
                  AND o.created_at <  {end_sql}
                """,
                m["id"],
            )
            method_totals = {
                "Наличные": Decimal(stats["s_cash"] or 0),
                "Карта Женя": Decimal(stats["s_card_jenya"] or 0),
                "Карта Дима": Decimal(stats["s_card_dima"] or 0),
                "р/с": Decimal(stats["s_rs"] or 0),
                GIFT_CERT_LABEL: Decimal(stats["s_gift_total"] or 0),
            }
            master_orders = int(stats["cnt"] or 0)
            total_orders += master_orders
            for key, value in method_totals.items():
                total_method_totals[key] = total_method_totals.get(key, Decimal(0)) + value

            cash_on_orders, withdrawn_total = await get_master_wallet(conn, m["id"])
            on_hand = cash_on_orders - withdrawn_total
            if on_hand < Decimal(0):
                on_hand = Decimal(0)
            total_on_hand += on_hand

            name = f"{m['fn']} {m['ln']}".strip() or f"Мастер #{m['id']}"
            if master_orders > 0:
                payments_text = _format_payment_summary(method_totals)
                lines.append(f"- {name}: {master_orders} заказ(ов); оплаты — {payments_text}; на руках {format_money(on_hand)}₽")
            else:
                lines.append(f"- {name}: на руках {format_money(on_hand)}₽")

        lines.append("")
        lines.append(f"Всего заказов за день: {total_orders}")
        lines.append("Оплаты всего: " + _format_payment_summary(total_method_totals))
        lines.append(f"Наличными у мастеров: {format_money(total_on_hand)}₽")
    return "\n".join(lines)


async def build_master_daily_summary_text(user_id: int) -> str:
    start_sql = "date_trunc('day', NOW())"
    end_sql = "date_trunc('day', NOW()) + interval '1 day'"
    async with pool.acquire() as conn:
        master_row = await conn.fetchrow(
            "SELECT id, COALESCE(first_name,'') AS fn, COALESCE(last_name,'') AS ln "
            "FROM staff WHERE tg_user_id=$1 AND is_active LIMIT 1",
            user_id,
        )
        if not master_row:
            return "Вы не зарегистрированы как активный мастер."

        master_id = master_row["id"]
        stats = await conn.fetchrow(
            f"""
            SELECT
              COUNT(*) AS cnt,
              COALESCE(SUM(CASE WHEN payment_method='Наличные'              THEN amount_cash  ELSE 0 END),0)::numeric(12,2) AS s_cash,
              COALESCE(SUM(CASE WHEN payment_method='Карта Женя'            THEN amount_cash  ELSE 0 END),0)::numeric(12,2) AS s_card_jenya,
              COALESCE(SUM(CASE WHEN payment_method='Карта Дима'            THEN amount_cash  ELSE 0 END),0)::numeric(12,2) AS s_card_dima,
              COALESCE(SUM(CASE WHEN payment_method='р/с'                   THEN amount_cash  ELSE 0 END),0)::numeric(12,2) AS s_rs,
              COALESCE(SUM(CASE WHEN payment_method='Подарочный сертификат' THEN amount_total ELSE 0 END),0)::numeric(12,2) AS s_gift_total,
              COALESCE(SUM(o.amount_total),0)::numeric(12,2) AS total_amount
            FROM orders o
            WHERE o.master_id = $1
              AND o.created_at >= {start_sql}
              AND o.created_at <  {end_sql}
            """,
            master_id,
        )
        payroll = await conn.fetchrow(
            f"""
            SELECT
              COALESCE(SUM(pi.base_pay),0)::numeric(12,2) AS base_pay,
              COALESCE(SUM(pi.fuel_pay),0)::numeric(12,2) AS fuel_pay,
              COALESCE(SUM(pi.upsell_pay),0)::numeric(12,2) AS upsell_pay,
              COALESCE(SUM(pi.total_pay),0)::numeric(12,2) AS total_pay
            FROM payroll_items pi
            JOIN orders o ON o.id = pi.order_id
            WHERE pi.master_id = $1
              AND o.created_at >= {start_sql}
              AND o.created_at <  {end_sql}
            """,
            master_id,
        )
        payroll_month = await conn.fetchrow(
            """
            SELECT
              COALESCE(SUM(pi.total_pay),0)::numeric(12,2) AS total_pay
            FROM payroll_items pi
            JOIN orders o ON o.id = pi.order_id
            WHERE pi.master_id = $1
              AND o.created_at >= date_trunc('month', NOW())
              AND o.created_at <  date_trunc('month', NOW()) + interval '1 month'
            """,
            master_id,
        )
        cash_on_orders, withdrawn_total = await get_master_wallet(conn, master_id)
        on_hand = cash_on_orders - withdrawn_total
        if on_hand < Decimal(0):
            on_hand = Decimal(0)

    method_totals = {
        "Наличные": Decimal(stats["s_cash"] or 0),
        "Карта Женя": Decimal(stats["s_card_jenya"] or 0),
        "Карта Дима": Decimal(stats["s_card_dima"] or 0),
        "р/с": Decimal(stats["s_rs"] or 0),
        GIFT_CERT_LABEL: Decimal(stats["s_gift_total"] or 0),
    }
    total_pay = Decimal(payroll["total_pay"] or 0)
    base_pay = Decimal(payroll["base_pay"] or 0)
    fuel_pay = Decimal(payroll["fuel_pay"] or 0)
    upsell_pay = Decimal(payroll["upsell_pay"] or 0)
    total_pay_month = Decimal(payroll_month["total_pay"] or 0)
    name = f"{master_row['fn']} {master_row['ln']}".strip() or f"Мастер #{master_id}"

    lines = [
        f"🧾 Сводка за сегодня — {name}",
        f"Заказов: {int(stats['cnt'] or 0)}",
        f"Счёт на сумму: {format_money(Decimal(stats['total_amount'] or 0))}₽",
        "Типы оплат: " + _format_payment_summary(method_totals),
        f"ЗП за сегодня: база {format_money(base_pay)}₽ + бензин {format_money(fuel_pay)}₽ + доп {format_money(upsell_pay)}₽ = {format_money(total_pay)}₽",
        f"ЗП за месяц: {format_money(total_pay_month)}₽",
        f"Наличных на руках: {format_money(on_hand)}₽",
    ]
    return "\n".join(lines)


async def _resolve_master_id_from_state(data: dict) -> int | None:
    tg_val = data.get("report_master_tg") or data.get("master_tg")
    if tg_val is None:
        return None
    try:
        tg_id = int(tg_val)
    except (TypeError, ValueError):
        return None
    async with pool.acquire() as conn:
        master_id = await conn.fetchval(
            "SELECT id FROM staff WHERE tg_user_id=$1",
            tg_id,
        )
    return master_id


async def send_daily_reports():
    try:
        cash_text = await build_daily_cash_summary_text()
        if MONEY_FLOW_CHAT_ID:
            await bot.send_message(MONEY_FLOW_CHAT_ID, cash_text)
        profit_text = await build_profit_summary_text()
        if MONEY_FLOW_CHAT_ID:
            await bot.send_message(MONEY_FLOW_CHAT_ID, profit_text)
        orders_text = await build_daily_orders_admin_summary_text()
        if ORDERS_CONFIRM_CHAT_ID:
            await bot.send_message(ORDERS_CONFIRM_CHAT_ID, orders_text)
    except Exception as exc:
        logging.exception("Failed to send admin daily reports: %s", exc)

    async with pool.acquire() as conn:
        master_rows = await conn.fetch(
            "SELECT tg_user_id FROM staff WHERE role='master' AND is_active AND tg_user_id IS NOT NULL"
        )
    for row in master_rows:
        tg_id = row["tg_user_id"]
        if not tg_id:
            continue
        try:
            text = await build_master_daily_summary_text(int(tg_id))
            await bot.send_message(tg_id, text)
        except Exception as exc:
            logging.exception("Failed to send master daily report to %s: %s", tg_id, exc)


async def daily_reports_scheduler():
    while True:
        now = datetime.now()
        target = now.replace(hour=22, minute=0, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        wait_seconds = (target - now).total_seconds()
        logging.info("Next daily reports dispatch scheduled in %.0f seconds", wait_seconds)
        try:
            await asyncio.sleep(wait_seconds)
            await send_daily_reports()
        except Exception as exc:
            logging.exception("Daily reports scheduler iteration failed: %s", exc)
            await asyncio.sleep(60)


async def get_master_payroll_report_text(master_id: int, period: str) -> str:
    bounds = _report_period_bounds(period)
    if not bounds:
        return "Неизвестный период отчёта."

    start_sql, end_sql, label = bounds
    async with pool.acquire() as conn:
        master_row = await conn.fetchrow(
            "SELECT id, tg_user_id, COALESCE(first_name,'') AS fn, COALESCE(last_name,'') AS ln "
            "FROM staff WHERE id=$1",
            master_id,
        )
        if not master_row:
            return "Мастер не найден."

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
            master_id,
        )

    orders = rec["orders"] if rec else 0
    base_pay = rec["base_pay"] if rec else 0
    fuel_pay = rec["fuel_pay"] if rec else 0
    upsell_pay = rec["upsell_pay"] if rec else 0
    total_pay = rec["total_pay"] if rec else 0

    fio = f"{master_row['fn']} {master_row['ln']}".strip()
    tg_id = master_row["tg_user_id"]

    lines = [
        f"Зарплата мастера: {fio or '—'} (tg:{tg_id}) — {label}",
        f"Заказов: {orders or 0}",
        f"База: {base_pay or 0}₽",
        f"Бензин: {fuel_pay or 0}₽",
    ]
    if (upsell_pay or 0) > 0:
        lines.append(f"Доп. услуги: {upsell_pay}₽")
    lines.append(f"Итого к выплате: {total_pay or 0}₽")
    return "\n".join(lines)


async def get_master_orders_payments_report_text(master_id: int, period: str) -> str:
    bounds = _report_period_bounds(period)
    if not bounds:
        return "Неизвестный период отчёта."

    start_sql, end_sql, label = bounds
    async with pool.acquire() as conn:
        master_row = await conn.fetchrow(
            "SELECT id, tg_user_id, COALESCE(first_name,'') AS fn, COALESCE(last_name,'') AS ln "
            "FROM staff WHERE id=$1",
            master_id,
        )
        if not master_row:
            return "Мастер не найден."

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
              WHERE c.kind='withdrawal' AND c.method='cash' AND c.master_id=$1
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
            master_id,
        )

        cash_on_orders, withdrawn_total = await get_master_wallet(conn, master_id)
        on_hand_now = cash_on_orders - withdrawn_total
        if on_hand_now < Decimal(0):
            on_hand_now = Decimal(0)

        withdrawn_period = await conn.fetchval(
            f"""
            SELECT COALESCE(SUM(amount),0)::numeric(12,2)
            FROM cashbook_entries
            WHERE kind='expense' AND method='Наличные'
              AND master_id=$1 AND order_id IS NULL
              AND (comment ILIKE '[WDR]%' OR comment ILIKE 'изъят%')
              AND happened_at >= {start_sql} AND happened_at < {end_sql}
            """,
            master_id,
        )

    fio = f"{master_row['fn']} {master_row['ln']}".strip()
    tg_id = master_row["tg_user_id"]

    lines = [
        f"Мастер: {fio or '—'} (tg:{tg_id}) — {label}",
        f"Заказов выполнено: {rec['cnt'] if rec else 0}",
    ]
    if rec:
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
        withdrawn_value = rec["withdrawn"] or Decimal(0)
        if withdrawn_value > 0:
            lines.append(f"Изъято у мастера: {withdrawn_value}₽")

    lines.append(f"Изъято у мастера за период: {format_money(Decimal(withdrawn_period or 0))}₽")
    lines.append(f"Итого на руках наличных: {format_money(on_hand_now)}₽")
    return "\n".join(lines)


async def _build_report_text(kind_raw: str | None, data: dict, period: str, state: FSMContext) -> str:
    kind = _normalize_report_kind(kind_raw)
    text = "Неизвестный тип отчёта."

    if kind == "Касса":
        text = await get_cash_report_text(period)
    elif kind == "Прибыль":
        text = await get_profit_report_text(period)
    elif kind == "Типы оплат":
        text = await get_payments_by_method_report_text(period)
    elif kind == "Мастер/Заказы/Оплаты":
        master_id = data.get("report_master_id")
        if master_id is None:
            master_id = await _resolve_master_id_from_state(data)
            if master_id is not None:
                await state.update_data(report_master_id=master_id)
        if master_id:
            text = await get_master_orders_payments_report_text(int(master_id), period)
        else:
            text = "Сначала выберите мастера."
    elif kind == "Мастер/Зарплата":
        master_id = data.get("report_master_id")
        if master_id is None:
            master_id = await _resolve_master_id_from_state(data)
            if master_id is not None:
                await state.update_data(report_master_id=master_id)
        if master_id:
            text = await get_master_payroll_report_text(int(master_id), period)
        else:
            text = "Сначала выберите мастера."

    return text


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


@dp.message(Command("daily_cash"))
async def daily_cash_report(msg: Message):
    if not await has_permission(msg.from_user.id, "view_cash_reports"):
        return await msg.answer("Только для администраторов.")
    text = await build_daily_cash_summary_text()
    await msg.answer(text)


@dp.message(Command("daily_profit"))
async def daily_profit_report(msg: Message):
    if not await has_permission(msg.from_user.id, "view_profit_reports"):
        return await msg.answer("Только для администраторов.")
    text = await build_profit_summary_text()
    await msg.answer(text)


@dp.message(Command("daily_orders"))
async def daily_orders_report(msg: Message):
    if not await has_permission(msg.from_user.id, "view_orders_reports"):
        return await msg.answer("Только для администраторов.")
    text = await build_daily_orders_admin_summary_text()
    await msg.answer(text)


@dp.message(Command("orders"))
async def orders_report(msg: Message):
    if not await has_permission(msg.from_user.id, "view_orders_reports"):
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
        prompt, kb = await build_report_masters_kb(conn)
    await state.clear()
    await state.set_state(ReportsFSM.waiting_pick_master)
    await state.update_data(
        report_kind="Мастер/Заказы/Оплаты",
        report_master_id=None,
        report_master_tg=None,
        report_master_name=None,
    )
    await msg.answer(prompt, reply_markup=kb)


@dp.message(ReportsFSM.waiting_root, F.text.casefold() == "мастер/зарплата")
async def rep_master_salary_entry(msg: Message, state: FSMContext):
    async with pool.acquire() as conn:
        prompt, kb = await build_report_masters_kb(conn)
    await state.clear()
    await state.set_state(ReportsFSM.waiting_pick_master)
    await state.update_data(
        report_kind="Мастер/Зарплата",
        report_master_id=None,
        report_master_tg=None,
        report_master_name=None,
    )
    await msg.answer(prompt, reply_markup=kb)


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
    data = await state.get_data()
    report_kind = data.get("report_kind")
    if report_kind in {
        "Мастер/Заказы/Оплаты",
        "master_orders",
        "Мастер/Зарплата",
        "master_salary",
    }:
        async with pool.acquire() as conn:
            prompt, kb = await build_report_masters_kb(conn)
        await state.set_state(ReportsFSM.waiting_pick_master)
        return await msg.answer(prompt, reply_markup=kb)

    await state.set_state(ReportsFSM.waiting_root)
    await msg.answer("Отчёты: выбери раздел.", reply_markup=reports_root_kb())


@dp.message(ReportsFSM.waiting_pick_period, F.text == "Выйти")
async def reports_exit_to_admin(msg: Message, state: FSMContext):
    await state.clear()
    await state.set_state(AdminMenuFSM.root)
    await msg.answer("Меню администратора:", reply_markup=admin_root_kb())


@dp.message(ReportsFSM.waiting_pick_master, F.text.in_({"Назад", "Отмена"}))
async def reports_pick_master_cancel_or_back(msg: Message, state: FSMContext):
    await state.set_state(ReportsFSM.waiting_root)
    await msg.answer("Отчёты: выбери раздел.", reply_markup=reports_root_kb())
    return


@dp.message(ReportsFSM.waiting_pick_master, F.text.casefold() == "назад")
async def rep_master_back(msg: Message, state: FSMContext):
    await state.set_state(ReportsFSM.waiting_root)
    return await msg.answer("Выберите отчёт:", reply_markup=reports_root_kb())


@dp.message(ReportsFSM.waiting_root, F.text == "Назад")
async def reports_root_back(msg: Message, state: FSMContext):
    await state.clear()
    await state.set_state(AdminMenuFSM.root)
    await msg.answer("Меню администратора:", reply_markup=admin_root_kb())


@dp.message(ReportsFSM.waiting_root, F.text == "Отмена")
@dp.message(ReportsFSM.waiting_pick_period, F.text == "Отмена")
async def reports_cancel(msg: Message, state: FSMContext):
    await state.clear()
    await state.set_state(AdminMenuFSM.root)
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


@dp.message(AdminMenuFSM.root, F.text == "Приход")
async def income_wizard_start(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "manage_income"):
        return await msg.answer("Только для администраторов.")
    await state.set_state(IncomeFSM.waiting_method)
    await msg.answer("Выберите способ оплаты:", reply_markup=admin_payment_method_kb())


@dp.message(IncomeFSM.waiting_method, F.text.casefold() == "отмена")
@dp.message(IncomeFSM.waiting_amount, F.text.casefold() == "отмена")
@dp.message(IncomeFSM.waiting_comment, F.text.casefold() == "отмена")
async def income_cancel_any(msg: Message, state: FSMContext):
    await state.clear()
    await state.set_state(AdminMenuFSM.root)
    await msg.answer("Операция отменена.", reply_markup=admin_root_kb())


@dp.message(IncomeFSM.waiting_amount, F.text.casefold() == "назад")
async def income_back_to_method(msg: Message, state: FSMContext):
    await state.set_state(IncomeFSM.waiting_method)
    await msg.answer("Выберите способ оплаты:", reply_markup=admin_payment_method_kb())


@dp.message(IncomeFSM.waiting_comment, F.text.casefold() == "назад")
async def income_back_to_amount(msg: Message, state: FSMContext):
    await state.set_state(IncomeFSM.waiting_amount)
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Назад"), KeyboardButton(text="Отмена")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await msg.answer("Введите сумму прихода (например 2500 или 2 500,5):", reply_markup=kb)


@dp.message(IncomeFSM.waiting_method)
async def income_wizard_pick_method(msg: Message, state: FSMContext):
    method = norm_pay_method_py(msg.text)
    if method not in PAYMENT_METHODS + [GIFT_CERT_LABEL]:
        kb = admin_payment_method_kb()
        return await msg.answer("Используйте кнопки для выбора способа оплаты.", reply_markup=kb)
    await state.update_data(method=method)
    await state.set_state(IncomeFSM.waiting_amount)
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Назад"), KeyboardButton(text="Отмена")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await msg.answer("Введите сумму прихода:", reply_markup=kb)


@dp.message(IncomeFSM.waiting_amount)
async def income_wizard_amount(msg: Message, state: FSMContext):
    txt = (msg.text or "").strip().replace(" ", "").replace(",", ".")
    try:
        amount = Decimal(txt)
    except Exception:
        return await msg.answer("Сумма должна быть числом. Повторите ввод или «Отмена».")
    if amount <= 0:
        return await msg.answer("Сумма должна быть > 0. Повторите ввод или «Отмена».")
    await state.update_data(amount=str(amount))
    await state.set_state(IncomeFSM.waiting_comment)
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Без комментария")],
            [KeyboardButton(text="Назад"), KeyboardButton(text="Отмена")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await msg.answer("Комментарий? (введите текст или нажмите «Без комментария»)", reply_markup=kb)


@dp.message(IncomeFSM.waiting_comment)
async def income_wizard_comment(msg: Message, state: FSMContext):
    txt = (msg.text or "").strip()
    if txt.casefold() == "без комментария" or not txt:
        txt = "поступление денег в кассу"
    data = await state.get_data()
    method = data.get("method")
    amount = Decimal(data.get("amount"))
    await state.update_data(comment=txt)
    await state.set_state(IncomeFSM.waiting_confirm)
    lines = [
        "Подтвердите приход:",
        f"Сумма: {format_money(amount)}₽",
        f"Метод: {method}",
        f"Комментарий: {txt}",
    ]
    await msg.answer("\n".join(lines), reply_markup=confirm_inline_kb("income_confirm"))


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
        await state.set_state(AdminMenuFSM.root)
        return await msg.answer("Операция отменена.", reply_markup=admin_root_kb())
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
        await state.set_state(AdminMenuFSM.root)
        return await msg.answer("Операция отменена.", reply_markup=admin_root_kb())
    if txt.casefold() == "без комментария":
        txt = "Расход"
    data = await state.get_data()
    amount = Decimal(data.get("amount"))
    await state.update_data(comment=txt)
    await state.set_state(ExpenseFSM.waiting_confirm)
    lines = [
        "Подтвердите расход:",
        f"Сумма: {format_money(amount)}₽",
        f"Комментарий: {txt}",
    ]
    await msg.answer("\n".join(lines), reply_markup=confirm_inline_kb("expense_confirm"))


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
    async with pool.acquire() as conn:
        master_row = await conn.fetchrow(
            "SELECT id, tg_user_id, COALESCE(first_name,'') AS fn, COALESCE(last_name,'') AS ln "
            "FROM staff WHERE tg_user_id=$1 AND role IN ('master','admin') AND is_active",
            tg_id,
        )
    if not master_row:
        return await msg.answer("Мастер с таким tg id не найден.")

    master_name = f"{master_row['fn']} {master_row['ln']}".strip() or f"Мастер #{master_row['id']}"
    await state.update_data(
        master_tg=tg_id,
        report_master_tg=tg_id,
        report_master_id=master_row["id"],
        report_master_name=master_name,
    )
    await state.set_state(ReportsFSM.waiting_pick_period)
    await msg.answer(
        f"Мастер выбран: {master_name} (tg:{tg_id}). Выберите период:",
        reply_markup=reports_period_kb(),
    )


@dp.message(ReportsFSM.waiting_pick_period, F.text.in_({"день", "неделя", "месяц", "год"}))
async def rep_master_period(msg: Message, state: FSMContext):
    period_map = {
        "день": "day",
        "неделя": "week",
        "месяц": "month",
        "год": "year",
    }
    normalized = period_map.get((msg.text or "").strip().lower())
    if not normalized:
        return await msg.answer("Выберите один из вариантов: день / неделя / месяц / год")

    data = await state.get_data()
    text = await _build_report_text(data.get("report_kind"), data, normalized, state)
    await msg.answer(text, reply_markup=reports_period_kb())

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


class UploadFSM(StatesGroup):
    waiting_csv = State()

class AmoImportFSM(StatesGroup):
    waiting_file = State()
    waiting_confirm = State()

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


@dp.message(Command("import_amocrm"))
async def import_amocrm_start(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "import_leads"):
        return await msg.answer("Только для администраторов.")
    await state.set_state(AmoImportFSM.waiting_file)
    await msg.answer(
        "Отправьте CSV-файл выгрузки AmoCRM (UTF-8, разделитель ';').\n"
        "Файл должен содержать столбцы из шаблона (телефоны, услуга, адрес и т.д.).",
        reply_markup=admin_cancel_kb(),
    )


@dp.message(AmoImportFSM.waiting_file, F.text.casefold() == "отмена")
async def import_amocrm_cancel(msg: Message, state: FSMContext):
    await state.clear()
    await state.set_state(AdminMenuFSM.root)
    await msg.answer("Импорт отменён.", reply_markup=admin_root_kb())


@dp.message(AmoImportFSM.waiting_file, F.document)
async def import_amocrm_file(msg: Message, state: FSMContext):
    if not await has_permission(msg.from_user.id, "import_leads"):
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        return await msg.answer("Только для администраторов.", reply_markup=admin_root_kb())

    document = msg.document
    if not document.file_name.lower().endswith(".csv"):
        return await msg.answer("Нужен CSV-файл (расширение .csv). Попробуйте ещё раз или нажмите Отмена.")

    try:
        file = await bot.get_file(document.file_id)
        file_bytes = await bot.download_file(file.file_path)
        data = file_bytes.read()
    except Exception as exc:  # noqa: BLE001
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        return await msg.answer(f"Не удалось получить файл: {exc}", reply_markup=admin_root_kb())

    for encoding in ("utf-8-sig", "utf-8"):
        try:
            csv_text = data.decode(encoding)
            break
        except UnicodeDecodeError:
            csv_text = None
    if csv_text is None:
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        return await msg.answer("Файл должен быть в кодировке UTF-8.", reply_markup=admin_root_kb())

    await state.update_data(import_csv=csv_text)

    async with pool.acquire() as conn:
        try:
            preview_counters, preview_errors = await process_amocrm_csv(conn, csv_text, dry_run=True)
        except Exception as exc:  # noqa: BLE001
            logging.exception("AmoCRM preview failed")
            await state.clear()
            await state.set_state(AdminMenuFSM.root)
            return await msg.answer(f"Ошибка при анализе файла: {exc}", reply_markup=admin_root_kb())

    await state.update_data(import_preview=(preview_counters, preview_errors))
    await state.set_state(AmoImportFSM.waiting_confirm)

    lines = ["Подтвердить импорт?"] + _format_amocrm_counters(preview_counters)
    if preview_errors:
        lines.append("\nОшибки (первые 10):")
        for err in preview_errors[:10]:
            lines.append(f"- {err}")
        if len(preview_errors) > 10:
            lines.append(f"… ещё {len(preview_errors) - 10} строк с ошибками")

    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Да")],
            [KeyboardButton(text="Нет")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await msg.answer("\n".join(lines), reply_markup=kb)


@dp.message(AmoImportFSM.waiting_file)
async def import_amocrm_waiting(msg: Message, state: FSMContext):
    await msg.answer("Нужен CSV-файл. Отправьте документ или нажмите Отмена.")


@dp.message(AmoImportFSM.waiting_confirm, F.text.casefold() == "да")
async def import_amocrm_confirm_yes(msg: Message, state: FSMContext):
    data = await state.get_data()
    csv_text = data.get("import_csv")
    if not csv_text:
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        return await msg.answer("Не найден файл для импорта. Повторите загрузку.", reply_markup=admin_root_kb())

    await msg.answer("Выполняю импорт…", reply_markup=admin_cancel_kb())

    async with pool.acquire() as conn:
        try:
            counters, errors = await process_amocrm_csv(conn, csv_text, dry_run=False)
        except Exception as exc:  # noqa: BLE001
            logging.exception("AmoCRM import failed")
            await state.clear()
            await state.set_state(AdminMenuFSM.root)
            return await msg.answer(f"Ошибка во время импорта: {exc}", reply_markup=admin_root_kb())

    await state.clear()
    await state.set_state(AdminMenuFSM.root)

    lines = ["Импорт AmoCRM завершён:"] + _format_amocrm_counters(counters)
    if errors:
        lines.append("\nОшибки:")
        for err in errors[:10]:
            lines.append(f"- {err}")
        if len(errors) > 10:
            lines.append(f"… ещё {len(errors) - 10} строк с ошибками")

    await msg.answer("\n".join(lines), reply_markup=admin_root_kb())


@dp.message(AmoImportFSM.waiting_confirm, F.text.casefold().in_({"нет", "отмена"}))
async def import_amocrm_confirm_no(msg: Message, state: FSMContext):
    await state.clear()
    await state.set_state(AdminMenuFSM.root)
    await msg.answer("Импорт отменён.", reply_markup=admin_root_kb())


@dp.message(AmoImportFSM.waiting_confirm)
async def import_amocrm_confirm_wait(msg: Message, state: FSMContext):
    await msg.answer("Ответьте «Да», чтобы подтвердить, или «Нет», чтобы отменить.")


@dp.message(Command("tx_remove"))
async def tx_remove_start(msg: Message, state: FSMContext):
    async with pool.acquire() as conn:
        role = await get_user_role(conn, msg.from_user.id)
    if role != "superadmin":
        return await msg.answer("Команда доступна только суперадмину.")
    await state.set_state(TxDeleteFSM.waiting_date)
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Отмена")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await msg.answer("Введите дату транзакций (ДД.ММ.ГГГГ):", reply_markup=kb)


@dp.message(TxDeleteFSM.waiting_date)
async def tx_remove_pick_date(msg: Message, state: FSMContext):
    txt = (msg.text or "").strip()
    if txt.lower() == "отмена":
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        return await msg.answer("Операция отменена.", reply_markup=admin_root_kb())

    dt = parse_birthday_str(txt)
    if not dt:
        return await msg.answer("Дата должна быть в формате ДД.ММ.ГГГГ или ГГГГ-ММ-ДД. Попробуйте снова или нажмите Отмена.")

    start_local = datetime.combine(dt, time.min, tzinfo=MOSCOW_TZ)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, happened_at, kind, method, amount, comment, master_id, order_id
            FROM cashbook_entries
            WHERE happened_at >= $1 AND happened_at < $2
              AND COALESCE(is_deleted,false)=FALSE
            ORDER BY happened_at, id
            """,
            start_utc,
            end_utc,
        )

    if not rows:
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        return await msg.answer("За указанную дату активных транзакций не найдено.", reply_markup=admin_root_kb())

    candidates = []
    lines = [f"Транзакции за {dt:%d.%m.%Y}:"]
    for row in rows:
        tx_type = _tx_type_label(row)
        dt_local = row["happened_at"].astimezone(MOSCOW_TZ)
        amount_str = format_money(Decimal(row["amount"] or 0))
        comment = (row["comment"] or "").strip()
        if len(comment) > 80:
            comment = comment[:77] + "…"
        lines.append(
            f"#{row['id']} {dt_local:%H:%M} {tx_type} {amount_str}₽ — {row['method']}" + (f" — {comment}" if comment else "")
        )
        candidates.append(row["id"])

    await state.update_data(
        tx_period={"start": start_utc.isoformat(), "end": end_utc.isoformat()},
        tx_candidates=candidates,
    )
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Отмена")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    lines.append("\nВведите ID транзакции для удаления или нажмите Отмена.")
    await state.set_state(TxDeleteFSM.waiting_pick)
    await msg.answer("\n".join(lines), reply_markup=kb)


@dp.message(TxDeleteFSM.waiting_pick)
async def tx_remove_choose(msg: Message, state: FSMContext):
    txt = (msg.text or "").strip()
    if txt.lower() == "отмена":
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        return await msg.answer("Операция отменена.", reply_markup=admin_root_kb())

    if not txt.isdigit():
        return await msg.answer("Введите числовой ID из списка или нажмите Отмена.")
    target_id = int(txt)
    data = await state.get_data()
    candidates = set(data.get("tx_candidates") or [])
    if target_id not in candidates:
        return await msg.answer("Этот ID не в списке. Укажите ID из перечня или Отмена.")

    period = data.get("tx_period") or {}
    start = datetime.fromisoformat(period.get("start"))
    end = datetime.fromisoformat(period.get("end"))

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, happened_at, kind, method, amount, comment, master_id, order_id
            FROM cashbook_entries
            WHERE id=$1 AND COALESCE(is_deleted,false)=FALSE
              AND happened_at >= $2 AND happened_at < $3
            """,
            target_id,
            start,
            end,
        )

    if not row:
        return await msg.answer("Транзакция уже удалена или не принадлежит выбранной дате.")

    tx_type = _tx_type_label(row)
    dt_local = row["happened_at"].astimezone(MOSCOW_TZ)
    amount_str = format_money(Decimal(row["amount"] or 0))
    comment = (row["comment"] or "").strip() or "—"

    await state.update_data(tx_target_id=target_id)
    await state.set_state(TxDeleteFSM.waiting_confirm)

    lines = [
        "Удалить транзакцию?",
        f"ID: {target_id}",
        f"Дата: {dt_local:%d.%m.%Y %H:%M}",
        f"Тип: {tx_type}",
        f"Метод: {row['method']}",
        f"Сумма: {amount_str}₽",
        f"Комментарий: {comment}",
    ]
    await msg.answer("\n".join(lines), reply_markup=confirm_inline_kb("tx_remove"))


@dp.callback_query(TxDeleteFSM.waiting_confirm)
async def tx_remove_confirm(query: CallbackQuery, state: FSMContext):
    data = (query.data or "").strip()
    if data not in {"tx_remove:yes", "tx_remove:cancel"}:
        await query.answer()
        return

    await query.answer()
    await query.message.edit_reply_markup(None)

    if data.endswith("cancel"):
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        await query.message.answer("Удаление отменено.", reply_markup=admin_root_kb())
        return

    payload = await state.get_data()
    target_id = payload.get("tx_target_id")
    if not target_id:
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        await query.message.answer("Не удалось получить ID транзакции. Попробуйте снова.", reply_markup=admin_root_kb())
        return

    async with pool.acquire() as conn:
        res = await conn.execute(
            "UPDATE cashbook_entries SET is_deleted=TRUE, deleted_at=NOW() WHERE id=$1 AND COALESCE(is_deleted,false)=FALSE",
            target_id,
        )
    if res.split()[-1] == "0":
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        await query.message.answer("Транзакция уже была удалена ранее.", reply_markup=admin_root_kb())
        return

    await state.clear()
    await state.set_state(AdminMenuFSM.root)
    await query.message.answer(f"Транзакция #{target_id} удалена.", reply_markup=admin_root_kb())


@dp.message(Command("order_remove"))
async def order_remove_start(msg: Message, state: FSMContext):
    async with pool.acquire() as conn:
        role = await get_user_role(conn, msg.from_user.id)
    if role != "superadmin":
        return await msg.answer("Команда доступна только суперадмину.")
    await state.set_state(OrderDeleteFSM.waiting_date)
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Отмена")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await msg.answer("Введите дату заказов (ДД.ММ.ГГГГ):", reply_markup=kb)


@dp.message(OrderDeleteFSM.waiting_date)
async def order_remove_pick_date(msg: Message, state: FSMContext):
    txt = (msg.text or "").strip()
    if txt.lower() == "отмена":
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        return await msg.answer("Операция отменена.", reply_markup=admin_root_kb())

    dt = parse_birthday_str(txt)
    if not dt:
        return await msg.answer("Дата должна быть в формате ДД.ММ.ГГГГ или ГГГГ-ММ-ДД. Попробуйте снова или нажмите Отмена.")

    start_local = datetime.combine(dt, time.min, tzinfo=MOSCOW_TZ)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT o.id,
                   o.created_at,
                   o.amount_total,
                   o.amount_cash,
                   o.payment_method,
                   o.bonus_spent,
                   o.bonus_earned,
                   COALESCE(c.full_name, '') AS client_name,
                   c.phone AS client_phone
            FROM orders o
            LEFT JOIN clients c ON c.id = o.client_id
            WHERE o.created_at >= $1 AND o.created_at < $2
            ORDER BY o.created_at, o.id
            """,
            start_utc,
            end_utc,
        )

    if not rows:
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        return await msg.answer("За указанную дату заказы не найдены.", reply_markup=admin_root_kb())

    candidates: list[int] = []
    lines = [f"Заказы за {dt:%d.%m.%Y}:"]
    for row in rows:
        created_at = row["created_at"]
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        created_local = created_at.astimezone(MOSCOW_TZ)
        client_label = (row["client_name"] or "Без имени").strip() or "Без имени"
        phone_mask = mask_phone_last4(row["client_phone"])
        method = row["payment_method"] or "—"
        cash_amount = format_money(Decimal(row["amount_cash"] or 0))
        total_amount = format_money(Decimal(row["amount_total"] or 0))
        lines.append(
            f"#{row['id']} {created_local:%H:%M} {client_label} {phone_mask} — "
            f"{method} {cash_amount}₽ (итого {total_amount}₽)"
        )
        candidates.append(row["id"])

    await state.update_data(
        order_period={"start": start_utc.isoformat(), "end": end_utc.isoformat()},
        order_candidates=candidates,
    )
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Отмена")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    lines.append("\nВведите ID заказа для удаления или нажмите Отмена.")
    await state.set_state(OrderDeleteFSM.waiting_pick)
    await msg.answer("\n".join(lines), reply_markup=kb)


@dp.message(OrderDeleteFSM.waiting_pick)
async def order_remove_choose(msg: Message, state: FSMContext):
    txt = (msg.text or "").strip()
    if txt.lower() == "отмена":
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        return await msg.answer("Операция отменена.", reply_markup=admin_root_kb())

    if not txt.isdigit():
        return await msg.answer("Введите числовой ID из списка или нажмите Отмена.")
    target_id = int(txt)
    data = await state.get_data()
    candidates = set(data.get("order_candidates") or [])
    if target_id not in candidates:
        return await msg.answer("Этот ID не в списке. Укажите ID из перечня или Отмена.")

    period = data.get("order_period") or {}
    start_raw = period.get("start")
    end_raw = period.get("end")
    if not start_raw or not end_raw:
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        return await msg.answer("Не удалось определить период. Попробуйте снова.", reply_markup=admin_root_kb())
    start = datetime.fromisoformat(start_raw)
    end = datetime.fromisoformat(end_raw)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT o.id,
                   o.created_at,
                   o.amount_total,
                   o.amount_cash,
                   o.payment_method,
                   o.bonus_spent,
                   o.bonus_earned,
                   o.client_id,
                   COALESCE(c.full_name, '') AS client_name,
                   c.phone AS client_phone,
                   COALESCE(c.address, '') AS client_address,
                   COALESCE(s.first_name, '') AS master_fn,
                   COALESCE(s.last_name, '')  AS master_ln
            FROM orders o
            LEFT JOIN clients c ON c.id = o.client_id
            LEFT JOIN staff s ON s.id = o.master_id
            WHERE o.id = $1
              AND o.created_at >= $2
              AND o.created_at < $3
            """,
            target_id,
            start,
            end,
        )

    if not row:
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        return await msg.answer("Заказ не найден (возможно, уже удалён).", reply_markup=admin_root_kb())

    created_at = row["created_at"]
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    created_local = created_at.astimezone(MOSCOW_TZ)
    client_label = (row["client_name"] or "Без имени").strip() or "Без имени"
    phone_mask = mask_phone_last4(row["client_phone"])
    address = (row["client_address"] or "").strip()
    master_name = f"{row['master_fn']} {row['master_ln']}".strip() or "—"
    payment_method = row["payment_method"] or "—"
    cash_amount = format_money(Decimal(row["amount_cash"] or 0))
    total_amount = format_money(Decimal(row["amount_total"] or 0))
    bonus_spent = int(row["bonus_spent"] or 0)
    bonus_earned = int(row["bonus_earned"] or 0)

    await state.update_data(order_target_id=target_id)
    await state.set_state(OrderDeleteFSM.waiting_confirm)

    lines = [
        "Удалить заказ?",
        f"ID: {target_id}",
        f"Дата: {created_local:%d.%m.%Y %H:%M}",
        f"Клиент: {client_label} {phone_mask}",
        f"Адрес: {address or '—'}",
        f"Мастер: {master_name}",
        f"Оплата: {payment_method}",
        f"Наличными в кассе: {cash_amount}₽",
        f"Итого чек: {total_amount}₽",
        f"Списано бонусов: {bonus_spent}",
        f"Начислено бонусов: {bonus_earned}",
        "",
        "Подтвердите удаление — касса и бонусы будут пересчитаны.",
    ]
    await msg.answer("\n".join(lines), reply_markup=confirm_inline_kb("order_remove"))


@dp.callback_query(OrderDeleteFSM.waiting_confirm)
async def order_remove_confirm(query: CallbackQuery, state: FSMContext):
    data = (query.data or "").strip()
    if data not in {"order_remove:yes", "order_remove:cancel"}:
        await query.answer()
        return

    await query.answer()
    await query.message.edit_reply_markup(None)

    if data.endswith("cancel"):
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        await query.message.answer("Удаление заказа отменено.", reply_markup=admin_root_kb())
        return

    payload = await state.get_data()
    target_id = payload.get("order_target_id")
    if not target_id:
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        await query.message.answer("Не удалось получить ID заказа. Попробуйте снова.", reply_markup=admin_root_kb())
        return

    order_info: dict | None = None
    status = "ok"

    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                SELECT o.id,
                       o.created_at,
                       o.amount_total,
                       o.amount_cash,
                       o.payment_method,
                       o.bonus_spent,
                       o.bonus_earned,
                       o.client_id,
                       COALESCE(c.full_name, '') AS client_name,
                       c.phone AS client_phone
                FROM orders o
                LEFT JOIN clients c ON c.id = o.client_id
                WHERE o.id = $1
                FOR UPDATE
                """,
                target_id,
            )

            if not row:
                status = "missing"
            else:
                client_id = row["client_id"]
                client_name = (row["client_name"] or "Без имени").strip() or "Без имени"
                phone_mask = mask_phone_last4(row["client_phone"])
                payment_method = row["payment_method"] or "—"
                amount_cash = Decimal(row["amount_cash"] or 0)
                amount_total = Decimal(row["amount_total"] or 0)
                bonus_spent = Decimal(row["bonus_spent"] or 0)
                bonus_earned = Decimal(row["bonus_earned"] or 0)
                bonus_delta = bonus_earned - bonus_spent

                cash_rows = await conn.fetch(
                    """
                    UPDATE cashbook_entries
                    SET is_deleted = TRUE, deleted_at = NOW()
                    WHERE order_id = $1 AND COALESCE(is_deleted, FALSE) = FALSE
                    RETURNING id, amount, method, comment
                    """,
                    target_id,
                )
                cash_removed = sum(Decimal(r["amount"] or 0) for r in cash_rows) if cash_rows else Decimal(0)
                cash_methods = sorted({r["method"] for r in cash_rows if r["method"]})

                payroll_delete_res = await conn.execute(
                    "DELETE FROM payroll_items WHERE order_id = $1",
                    target_id,
                )
                payroll_deleted = int(payroll_delete_res.split()[-1])

                bonus_delete_res = await conn.execute(
                    "DELETE FROM bonus_transactions WHERE order_id = $1",
                    target_id,
                )
                bonus_deleted = int(bonus_delete_res.split()[-1])

                bonus_adjusted = False
                if client_id and bonus_delta != 0:
                    await conn.execute(
                        """
                        UPDATE clients
                        SET bonus_balance = GREATEST(COALESCE(bonus_balance,0) - $1, 0),
                            last_updated = NOW()
                        WHERE id = $2
                        """,
                        bonus_delta,
                        client_id,
                    )
                    bonus_adjusted = True

                await conn.execute(
                    "DELETE FROM orders WHERE id = $1",
                    target_id,
                )

                balance = await get_cash_balance_excluding_withdrawals(conn)

                order_info = {
                    "order_id": target_id,
                    "client_name": client_name,
                    "phone_mask": phone_mask,
                    "payment_method": payment_method,
                    "cash_removed": cash_removed,
                    "cash_methods": cash_methods,
                    "amount_total": amount_total,
                    "bonus_delta": bonus_delta,
                    "bonus_adjusted": bonus_adjusted,
                    "bonus_deleted": bonus_deleted,
                    "payroll_deleted": payroll_deleted,
                    "cash_entry_ids": [r["id"] for r in cash_rows],
                    "balance": balance,
                }

    await state.clear()
    await state.set_state(AdminMenuFSM.root)

    if status == "missing":
        await query.message.answer("Заказ уже был удалён ранее.", reply_markup=admin_root_kb())
        return

    if not order_info:
        await query.message.answer("Не удалось удалить заказ. Проверьте журналы.", reply_markup=admin_root_kb())
        return

    cash_methods = order_info["cash_methods"]
    method_display = ", ".join(cash_methods) if cash_methods else order_info["payment_method"]
    cash_removed = order_info["cash_removed"]
    client_label = f"{order_info['client_name']} {order_info['phone_mask']}".strip()
    bonus_delta = order_info["bonus_delta"]
    bonus_adjustment = -bonus_delta

    lines = [
        f"Заказ #{order_info['order_id']} удалён.",
        f"Клиент: {client_label}",
        f"Оплата: {order_info['payment_method']} (касса: {method_display})",
        f"Наличные скорректированы на {format_money(cash_removed)}₽",
    ]

    if order_info["cash_entry_ids"]:
        ids_str = ", ".join(f"#{cid}" for cid in order_info["cash_entry_ids"])
        lines.append(f"Помечены кассовые записи: {ids_str}")
    else:
        lines.append("Кассовых записей для заказа не найдено.")

    if order_info["payroll_deleted"]:
        lines.append(f"Удалено записей payroll: {order_info['payroll_deleted']}")
    if order_info["bonus_deleted"]:
        lines.append(f"Удалено бонусных транзакций: {order_info['bonus_deleted']}")
    if order_info["bonus_adjusted"]:
        adj_str = f"{int(bonus_adjustment)}"
        lines.append(f"Бонусы клиента скорректированы на {adj_str}")

    lines.append(f"Остаток кассы: {format_money(order_info['balance'])}₽")

    await query.message.answer("\n".join(lines), reply_markup=admin_root_kb())

    if MONEY_FLOW_CHAT_ID:
        try:
            cash_line = format_money(cash_removed)
            balance_line = format_money(order_info["balance"])
            msg_lines = [
                "Транзакция удалена",
                f"Заказ №{order_info['order_id']} — {method_display} {cash_line}₽",
                f"Касса - {balance_line}₽",
            ]
            await bot.send_message(MONEY_FLOW_CHAT_ID, "\n".join(msg_lines))
        except Exception as exc:  # noqa: BLE001
            logging.warning("order_remove notify failed for order_id=%s: %s", order_info["order_id"], exc)


@dp.message(Command("bonus_backfill"))
async def bonus_backfill(msg: Message):
    async with pool.acquire() as conn:
        role = await get_user_role(conn, msg.from_user.id)
    if role != "superadmin":
        return await msg.answer("Команда доступна только суперадмину.")

    await msg.answer("Пересчитываю историю бонусов…")

    today_local = datetime.now(MOSCOW_TZ).date()

    async with pool.acquire() as conn:
        client_rows = await conn.fetch(
            """
            SELECT id, COALESCE(bonus_balance,0) AS balance, birthday
            FROM clients
            WHERE COALESCE(bonus_balance,0) > 0
            ORDER BY id
            """
        )
        existing = await conn.fetch(
            "SELECT DISTINCT client_id FROM bonus_transactions WHERE delta > 0"
        )
        existing_ids = {row["client_id"] for row in existing if row["client_id"] is not None}

        processed = 0
        skipped_existing = 0
        birthday_used = 0
        records_created = 0
        errors: list[str] = []

        async with conn.transaction():
            for row in client_rows:
                client_id = row["id"]
                balance = int(row["balance"] or 0)
                if balance <= 0:
                    continue
                if client_id in existing_ids:
                    skipped_existing += 1
                    continue

                remaining = balance
                birthday = row["birthday"]

                try:
                    if birthday and remaining > 0:
                        last_bd = _last_birthday_date(birthday, today_local)
                        amount_bd = min(int(BONUS_BIRTHDAY_VALUE), remaining)
                        if amount_bd > 0:
                            bd_local = datetime.combine(last_bd, time(hour=12, minute=0), tzinfo=MOSCOW_TZ)
                            bd_utc = bd_local.astimezone(timezone.utc)
                            expires_bd = (bd_local + timedelta(days=365)).astimezone(timezone.utc)
                            await conn.execute(
                                """
                                INSERT INTO bonus_transactions (client_id, delta, reason, created_at, happened_at, expires_at, meta)
                                VALUES ($1, $2, 'birthday', $3, $3, $4::timestamptz, jsonb_build_object('backfill', true))
                                """,
                                client_id,
                                amount_bd,
                                bd_utc,
                                expires_bd,
                            )
                            remaining -= amount_bd
                            birthday_used += 1
                            records_created += 1

                    if remaining > 0:
                        now_local = datetime.now(MOSCOW_TZ)
                        now_utc = now_local.astimezone(timezone.utc)
                        expires = (now_local + timedelta(days=365)).astimezone(timezone.utc)
                        await conn.execute(
                            """
                            INSERT INTO bonus_transactions (client_id, delta, reason, created_at, happened_at, expires_at, meta)
                            VALUES ($1, $2, 'accrual', $3, $3, $4::timestamptz, jsonb_build_object('backfill', true))
                            """,
                            client_id,
                            remaining,
                            now_utc,
                            expires,
                        )
                        records_created += 1

                    processed += 1
                except Exception as exc:  # noqa: BLE001
                    logging.exception("bonus backfill failed for client %s: %s", client_id, exc)
                    errors.append(f"client {client_id}: {exc}")

    lines = [
        "Бонусы перерасчитаны:",
        f"Клиентов обработано: {processed}",
        f"Пропущено (уже есть история): {skipped_existing}",
        f"Создано записей: {records_created}",
        f"Использован день рождения: {birthday_used}",
    ]
    if errors:
        lines.append("\nОшибки:")
        for err in errors[:10]:
            lines.append(f"- {err}")
        if len(errors) > 10:
            lines.append(f"… ещё {len(errors) - 10} строк")
    await msg.answer("\n".join(lines), reply_markup=admin_root_kb())
@dp.callback_query(IncomeFSM.waiting_confirm)
async def income_confirm_handler(query: CallbackQuery, state: FSMContext):
    data = (query.data or "").strip()
    if data not in {"income_confirm:yes", "income_confirm:cancel"}:
        await query.answer()
        return

    await query.answer()
    await query.message.edit_reply_markup(None)

    if data.endswith("cancel"):
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        await query.message.answer("Приход отменён.", reply_markup=admin_root_kb())
        return

    payload = await state.get_data()
    try:
        method = payload.get("method") or "прочее"
        amount = Decimal(payload.get("amount") or "0")
        comment = payload.get("comment") or "поступление денег в кассу"
    except Exception as exc:  # noqa: BLE001
        logging.exception("income confirm payload error: %s", exc)
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        await query.message.answer("Не удалось прочитать данные прихода. Попробуйте оформить заново.", reply_markup=admin_root_kb())
        return

    async with pool.acquire() as conn:
        try:
            tx = await _record_income(conn, method, amount, comment)
        except Exception as exc:  # noqa: BLE001
            logging.exception("income confirm failed: %s", exc)
            await state.clear()
            await state.set_state(AdminMenuFSM.root)
            await query.message.answer(f"Ошибка при проведении прихода: {exc}", reply_markup=admin_root_kb())
            return

    when = tx["happened_at"].strftime("%Y-%m-%d %H:%M")
    await query.message.answer(
        f"Приход №{tx['id']}: {format_money(amount)}₽ | {method} — {when}\nКомментарий: {comment}",
        reply_markup=admin_root_kb(),
    )
    await state.clear()
    await state.set_state(AdminMenuFSM.root)


@dp.callback_query(ExpenseFSM.waiting_confirm)
async def expense_confirm_handler(query: CallbackQuery, state: FSMContext):
    data = (query.data or "").strip()
    if data not in {"expense_confirm:yes", "expense_confirm:cancel"}:
        await query.answer()
        return

    await query.answer()
    await query.message.edit_reply_markup(None)

    if data.endswith("cancel"):
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        await query.message.answer("Расход отменён.", reply_markup=admin_root_kb())
        return

    payload = await state.get_data()
    try:
        amount = Decimal(payload.get("amount") or "0")
        comment = payload.get("comment") or "Расход"
    except Exception as exc:  # noqa: BLE001
        logging.exception("expense confirm payload error: %s", exc)
        await state.clear()
        await state.set_state(AdminMenuFSM.root)
        await query.message.answer("Не удалось прочитать данные расхода. Попробуйте оформить заново.", reply_markup=admin_root_kb())
        return

    async with pool.acquire() as conn:
        try:
            tx = await _record_expense(conn, amount, comment, method="прочее")
        except Exception as exc:  # noqa: BLE001
            logging.exception("expense confirm failed: %s", exc)
            await state.clear()
            await state.set_state(AdminMenuFSM.root)
            await query.message.answer(f"Ошибка при проведении расхода: {exc}", reply_markup=admin_root_kb())
            return

    when = tx["happened_at"].strftime("%Y-%m-%d %H:%M")
    await query.message.answer(
        f"Расход №{tx['id']}: {format_money(amount)}₽ — {when}\nКомментарий: {comment}",
        reply_markup=admin_root_kb(),
    )
    await state.clear()
    await state.set_state(AdminMenuFSM.root)

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
async def tx_last_cmd(msg: Message, command: CommandObject | None = None):
    limit = 30
    try:
        if command and command.args:
            n = int((command.args or "30").strip())
            if 1 <= n <= 200:
                limit = n
    except Exception:
        pass
    await _send_tx_last(msg, limit)


@dp.message(F.text.in_({"/tx_last 10", "/tx_last 30", "/tx_last 50"}))
async def tx_last_presets(msg: Message):
    try:
        limit = int(msg.text.split()[1])
    except Exception:
        limit = 30
    await _send_tx_last(msg, limit)

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
    if not await ensure_master(msg.from_user.id):
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
    if not await ensure_master(msg.from_user.id):
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


@dp.message(Command("my_daily"))
async def my_daily_report(msg: Message):
    if not await ensure_master(msg.from_user.id):
        return await msg.answer("Доступно только мастерам.")
    text = await build_master_daily_summary_text(msg.from_user.id)
    await msg.answer(text)


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
    current_state = await state.get_state()
    await state.clear()

    admin_prefixes = {
        "AdminMenuFSM",
        "AdminClientsFSM",
        "AdminMastersFSM",
        "AddMasterFSM",
        "WithdrawFSM",
        "IncomeFSM",
        "ExpenseFSM",
        "UploadFSM",
        "ReportsFSM",
    }
    prefix = current_state.split(":")[0] if current_state else ""
    if prefix in admin_prefixes or await has_permission(msg.from_user.id, "view_orders_reports"):
        return await msg.answer("Отменено.", reply_markup=admin_root_kb())

    if await ensure_master(msg.from_user.id):
        return await msg.answer("Отменено.", reply_markup=master_kb)

    return await msg.answer("Отменено.", reply_markup=main_kb)


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
    async with pool.acquire() as conn:
        rec = await _find_client_by_phone(conn, user_input)
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
    kb = master_kb if await ensure_master(msg.from_user.id) else main_kb
    await msg.answer(text, reply_markup=kb)

# ===== FSM: Я ВЫПОЛНИЛ ЗАКАЗ =====
class OrderFSM(StatesGroup):
    phone = State()
    name = State()
    amount = State()
    upsell_flag = State()
    upsell_amount = State()
    bonus_spend = State()
    bonus_custom = State()
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
        return await msg.answer(
            "Формат номера: 9XXXXXXXXX, 8XXXXXXXXXX или +7XXXXXXXXXX",
            reply_markup=cancel_kb
        )
    # если всё хорошо — нормализуем номер
    phone_in = normalize_phone_for_db(user_input)
    async with pool.acquire() as conn:
        client = await _find_client_by_phone(conn, user_input)
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
                "Введите правильное имя клиента (или нажмите «Отмена»):",
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
        return await msg.answer("Имя не может быть пустым. Введите имя или нажмите «Отмена».", reply_markup=cancel_kb)
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
        await state.set_state(OrderFSM.bonus_custom)
        return await msg.answer(
            "Введите целую сумму бонусов для списания (в рублях), например 300.\n"
            f"Максимум доступно: {bonus_max}.",
            reply_markup=cancel_kb
        )
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


@dp.message(OrderFSM.bonus_custom, F.text)
async def bonus_custom_amount(msg: Message, state: FSMContext):
    raw = (msg.text or "").strip()
    digits = re.sub(r"[^\d]", "", raw)
    if not digits:
        return await msg.answer("Введите целую сумму бонусов (например 300) или нажмите «Отмена».", reply_markup=cancel_kb)
    try:
        spend = Decimal(digits)
    except Exception:
        return await msg.answer("Не удалось распознать сумму. Введите число, например 300.", reply_markup=cancel_kb)

    data = await state.get_data()
    amount = Decimal(str(data["amount_total"]))
    bonus_max = Decimal(str(data["bonus_max"]))
    if spend > bonus_max:
        return await msg.answer(f"Нельзя списать больше {bonus_max}. Введите сумму не превышающую лимит.", reply_markup=cancel_kb)
    cash_payment = amount - spend
    if cash_payment < MIN_CASH:
        return await msg.answer(f"Минимальная оплата деньгами {MIN_CASH}. Уменьшите списание бонусов.", reply_markup=cancel_kb)

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
    client_birthday_val: date | None = data.get("birthday")
    if isinstance(client_birthday_val, str):
        client_birthday_val = parse_birthday_str(client_birthday_val)

    order_id: int | None = None
    master_display_name: str | None = None
    master_db_id: int | None = None
    client_full_name_val: str | None = None
    client_phone_val: str | None = phone_in
    client_address_val: str | None = None
    client_display_masked: str | None = None
    notify_label: str | None = None
    street_label: str | None = None

    async with pool.acquire() as conn:
        async with conn.transaction():
            client = await conn.fetchrow(
                "INSERT INTO clients (full_name, phone, bonus_balance, birthday, status) "
                "VALUES ($1, $2, 0, $3, 'client') "
                "ON CONFLICT (phone) DO UPDATE SET "
                "  full_name = COALESCE(EXCLUDED.full_name, clients.full_name), "
                "  birthday  = COALESCE(EXCLUDED.birthday, clients.birthday), "
                "  status='client' "
                "RETURNING id, bonus_balance, full_name, phone, address, birthday",
                name, phone_in, new_bday
            )
            client_id = client["id"]
            client_full_name_val = (client["full_name"] or name or "").strip() or None
            client_phone_val = client["phone"] or phone_in
            client_address_val = client.get("address")
            client_birthday_val = client.get("birthday") or client_birthday_val or new_bday

            order = await conn.fetchrow(
                "INSERT INTO orders (client_id, master_id, phone_digits, amount_total, amount_cash, amount_upsell, "
                " bonus_spent, bonus_earned, payment_method) "
                "VALUES ($1, "
                "       (SELECT id FROM staff WHERE tg_user_id=$2 AND is_active LIMIT 1), "
                "       regexp_replace($3,'[^0-9]+','','g'), $4, $5, $6, $7, $8, $9) "
                "RETURNING id, master_id",
                client_id, msg.from_user.id, phone_in, amount_total, cash_payment, upsell,
                bonus_spent, bonus_earned, payment_method
            )
            order_id = order["id"]
            master_db_id = order["master_id"]

            await conn.execute(
                "INSERT INTO staff(tg_user_id, role, is_active) "
                "VALUES ($1,'master',true) ON CONFLICT (tg_user_id) DO UPDATE SET is_active=true",
                msg.from_user.id
            )

            if master_db_id is None:
                master_db_id = await conn.fetchval(
                    "SELECT id FROM staff WHERE tg_user_id=$1 AND is_active LIMIT 1",
                    msg.from_user.id,
                )
                if master_db_id is not None:
                    await conn.execute(
                        "UPDATE orders SET master_id=$1 WHERE id=$2",
                        master_db_id,
                        order_id,
                    )

            if master_db_id is not None:
                master_row = await conn.fetchrow(
                    "SELECT COALESCE(first_name,'') AS first_name, COALESCE(last_name,'') AS last_name "
                    "FROM staff WHERE id=$1",
                    master_db_id,
                )
                if master_row:
                    master_display_name = f"{master_row['first_name']} {master_row['last_name']}".strip() or None

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

            await conn.execute(
                "INSERT INTO payroll_items (order_id, master_id, base_pay, fuel_pay, upsell_pay, total_pay, calc_info) "
                "VALUES ($1, (SELECT id FROM staff WHERE tg_user_id=$2), $3, $4, $5, $6, "
                "        jsonb_build_object('cash_payment', to_jsonb(($7)::numeric), 'rules', '1000/3000 + 150 + 500/3000'))",
                order_id, msg.from_user.id, base_pay, fuel_pay, upsell_pay, total_pay, cash_payment
            )

            street_label = extract_street(client_address_val)
            base_name_for_label = (client_full_name_val or name or "Клиент").strip() or "Клиент"
            masked_phone = mask_phone_last4(client_phone_val)
            client_display_masked = f"{base_name_for_label} {masked_phone}".strip()
            if street_label:
                notify_label = street_label
            else:
                notify_label = client_display_masked

            if master_db_id is None:
                raise RuntimeError("Не удалось определить master_id для записи кассы.")
            await _record_order_income(conn, payment_method, cash_payment, order_id, int(master_db_id), notify_label)
        try:
            await post_order_bonus_delta(conn, order_id)
        except Exception as e:  # noqa: BLE001
            logging.warning("post_order_bonus_delta failed for order_id=%s: %s", order_id, e)

    master_display_name = master_display_name or (msg.from_user.full_name or msg.from_user.username or f"tg:{msg.from_user.id}")
    client_display_masked = client_display_masked or f"{(name or 'Клиент').strip() or 'Клиент'} {mask_phone_last4(client_phone_val)}".strip()
    birthday_display = "—"
    if isinstance(client_birthday_val, date):
        birthday_display = client_birthday_val.strftime("%d.%m")

    if ORDERS_CONFIRM_CHAT_ID:
        try:
            lines = [
                f"🧾 Заказ №{order_id}",
                f"Клиент: {client_display_masked}",
            ]
            if client_address_val:
                lines.append(f"Адрес: {client_address_val}")
            lines.append(f"ДР: {birthday_display}")
            pay_line = (
                f"Оплата: {payment_method} {format_money(cash_payment)}₽ | "
                f"Бонусами {bonus_spent} | Итого: {format_money(amount_total)}₽"
            )
            lines.append(pay_line)
            lines.append(f"Доп. продажа: {format_money(upsell)}₽")
            lines.append(f"Бонусов начислено {bonus_earned}")
            lines.append(f"Мастер: {master_display_name}")
            await bot.send_message(ORDERS_CONFIRM_CHAT_ID, "\n".join(lines))
        except Exception as e:  # noqa: BLE001
            logging.warning("order confirm notify failed for order_id=%s: %s", order_id, e)

    await state.clear()
    await msg.answer("Готово ✅ Заказ сохранён.\nСпасибо!", reply_markup=master_kb)

# ---- Master menu handlers ----

# 🔍 Клиент — поиск клиента по номеру
@dp.message(F.text == "🔍 Клиент")
async def master_find_start(msg: Message, state: FSMContext):
    if not await ensure_master(msg.from_user.id):
        return await msg.answer("Доступно только мастерам.")
    await state.set_state(MasterFSM.waiting_phone)
    await msg.answer("Введите номер телефона клиента:", reply_markup=cancel_kb)

@dp.message(MasterFSM.waiting_phone, F.text)
async def master_find_phone(msg: Message, state: FSMContext):
    user_input = msg.text.strip()
    # если формат неправильный — вернуть сообщение об ошибке
    if not is_valid_phone_format(user_input):
        return await msg.answer(
            "Формат номера: 9XXXXXXXXX, 8XXXXXXXXXX или +7XXXXXXXXXX",
            reply_markup=cancel_kb
        )

    async with pool.acquire() as conn:
        rec = await _find_client_by_phone(conn, user_input)
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
    if not await ensure_master(msg.from_user.id):
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
    if not await ensure_master(msg.from_user.id):
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
    if await has_permission(msg.from_user.id, "view_orders_reports"):
        kb = admin_root_kb()
    elif await ensure_master(msg.from_user.id):
        kb = master_kb
    else:
        kb = main_kb
    await msg.answer("Команда не распознана. Нажми «🧾 Я ВЫПОЛНИЛ ЗАКАЗ» или /help", reply_markup=kb)

async def main():
    global pool, daily_reports_task, birthday_task
    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=1, max_size=5)
    async with pool.acquire() as _conn:
        await init_permissions(_conn)
        await _ensure_bonus_posted_column(_conn)
    await set_commands()
    if daily_reports_task is None:
        daily_reports_task = asyncio.create_task(
            schedule_daily_job(22, 0, send_daily_reports, "reports")
        )
    if birthday_task is None:
        birthday_task = asyncio.create_task(
            schedule_daily_job(12, 0, run_birthday_jobs, "birthday_bonuses")
        )
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
