import asyncio, os, re, logging
import csv, io
from decimal import Decimal, ROUND_DOWN
from datetime import date, datetime, timezone
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, BotCommand, BotCommandScopeDefault, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from aiogram.filters import CommandStart, Command, CommandObject
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
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
def parse_birthday_str(s: str | None) -> str | None:
    """
    Accepts 'DD.MM.YYYY' or 'YYYY-MM-DD' (or empty/None) and returns ISO 'YYYY-MM-DD' or None.
    Any other format returns None.
    """
    if not s:
        return None
    s = s.strip()
    if not s:
        return None
    # try DD.MM.YYYY
    m = re.fullmatch(r"(\d{2})\.(\d{2})\.(\d{4})", s)
    if m:
        dd, mm, yyyy = m.groups()
        return f"{yyyy}-{mm}-{dd}"
    # try YYYY-MM-DD
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return s
    return None

# ===== Client edit helpers =====
async def _find_client_by_phone(conn: asyncpg.Connection, phone_input: str):
    """Lookup client by any phone format using phone_digits unique index."""
    digits = re.sub(r"[^0-9]", "", phone_input or "")
    if not digits:
        return None
    # phone_digits is unique for non-empty values
    rec = await conn.fetchrow(
        "SELECT id, full_name, phone, birthday, bonus_balance, status FROM clients WHERE phone_digits = $1",
        digits,
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
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=True)

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
        # если хотите оставить командный поиск клиента — добавьте /find здесь
    ]
    await bot.set_my_commands(cmds, scope=BotCommandScopeDefault())

# ===== Admin commands (must be defined after dp is created) =====
@dp.message(Command("list_masters"))
async def list_masters(msg: Message):
    if not await has_permission(msg.from_user.id, "add_master"):
        return await msg.answer("Только для администраторов.")
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT s.id, s.tg_user_id, s.role, s.is_active FROM staff s WHERE role IN ('master','admin') ORDER BY role DESC, id")
    if not rows:
        return await msg.answer("Список пуст.")
    lines = [f"#{r['id']} {r['role']} tg={r['tg_user_id']} {'✅' if r['is_active'] else '⛔️'}" for r in rows]
    await msg.answer("Мастера/админы:\n" + "\n".join(lines))

@dp.message(Command("add_master"))
async def add_master(msg: Message):
    if not await has_permission(msg.from_user.id, "add_master"):
        return await msg.answer("Только для администраторов.")
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        return await msg.answer("Формат: /add_master <tg_user_id>")
    try:
        target_id = int(parts[1].lstrip("@"))
    except Exception:
        return await msg.answer("Нужно указать числовой tg_user_id.")
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO staff(tg_user_id, role, is_active) VALUES ($1,'master',true) "
            "ON CONFLICT (tg_user_id) DO UPDATE SET role='master', is_active=true",
            target_id
        )
    await msg.answer(f"Пользователь {target_id} назначен мастером и активирован.")


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


@dp.message(Command("whoami"))
async def whoami(msg: Message):
    global pool
    async with pool.acquire() as conn:
        rec = await conn.fetchrow("SELECT role, is_active FROM staff WHERE tg_user_id=$1 LIMIT 1", msg.from_user.id)
        role = rec["role"] if rec else None
        is_active = bool(rec["is_active"]) if rec else False
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
    parts = msg.text.split(maxsplit=2)
    if len(parts) < 3:
        return await msg.answer("Формат: /client_set_birthday <телефон> <ДР: DD.MM.YYYY или YYYY-MM-DD>")
    phone_q = parts[1].strip()
    bday_raw = parts[2].strip()
    bday_iso = parse_birthday_str(bday_raw)
    if not bday_iso:
        return await msg.answer("Не распознал дату. Форматы: DD.MM.YYYY или YYYY-MM-DD.")
    async with pool.acquire() as conn:
        rec = await _find_client_by_phone(conn, phone_q)
        if not rec:
            return await msg.answer("Клиент не найден по этому номеру.")
        await conn.execute("UPDATE clients SET birthday=$1::date, last_updated=NOW() WHERE id=$2", bday_iso, rec["id"])
        rec2 = await conn.fetchrow("SELECT id, full_name, phone, birthday, bonus_balance, status FROM clients WHERE id=$1", rec["id"])
    return await msg.answer("ДР обновлён:\n" + _fmt_client_row(rec2))

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
        except asyncpg.UniqueViolationError:
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

# ===== /cash admin command =====
@dp.message(Command("cash"))
async def cash_report(msg: Message):
    # доступ только у админа
    if not await has_permission(msg.from_user.id, "view_cash_reports"):
        return await msg.answer("Только для администраторов.")

    # Форматы:
    # /cash                -> за сегодня
    # /cash day|month|year -> агрегат за текущий период
    # /cash 2025-10-03     -> конкретный день
    # /cash 2025-10        -> конкретный месяц
    parts = msg.text.split(maxsplit=1)
    args = parts[1].strip().lower() if len(parts) > 1 else "day"

    # хелпер: строим SQL-границы периода [start, end)
    def trunc(unit: str) -> str:
        # оставляем расчёт на стороне БД
        return f"date_trunc('{unit}', NOW())"

    if args in ("day", "month", "year"):
        period_label = {"day": "сегодня", "month": "текущий месяц", "year": "текущий год"}[args]
        unit = args
        start_sql = trunc(unit)
        end_sql = f"{trunc(unit)} + interval '1 {unit}'"
    else:
        mday = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", args)
        mmon = re.fullmatch(r"(\d{4})-(\d{2})", args)
        if mday:
            y, m, d = map(int, mday.groups())
            period_label = f"{y:04d}-{m:02d}-{d:02d}"
            start_sql = f"TIMESTAMP WITH TIME ZONE '{y:04d}-{m:02d}-{d:02d} 00:00:00+00'"
            end_sql   = f"{start_sql} + interval '1 day'"
        elif mmon:
            y, m = map(int, mmon.groups())
            period_label = f"{y:04d}-{m:02d}"
            start_sql = f"TIMESTAMP WITH TIME ZONE '{y:04d}-{m:02d}-01 00:00:00+00'"
            end_sql   = f"{start_sql} + interval '1 month'"
        else:
            return await msg.answer("Формат: /cash [day|month|year|YYYY-MM|YYYY-MM-DD]")

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
        rows = await conn.fetch(
            f"""
            SELECT day::date AS d,
                   COALESCE(income,0)::numeric(12,2)  AS income,
                   COALESCE(expense,0)::numeric(12,2) AS expense,
                   COALESCE(delta,0)::numeric(12,2)   AS delta
            FROM v_cash_summary
            WHERE day >= {start_sql} AND day < {end_sql}
            ORDER BY day DESC
            LIMIT 31;
            """
        )

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
        lines.append("\nДетализация по дням (последние):")
        for r in rows:
            lines.append(f"{r['d']}: +{r['income']} / -{r['expense']} = {r['delta']}₽")

    await msg.answer("\n".join(lines))

# ===== /profit admin command =====
@dp.message(Command("profit"))
async def profit_report(msg: Message):
    # доступ только у админа
    if not await has_permission(msg.from_user.id, "view_profit_reports"):
        return await msg.answer("Только для администраторов.")

    # Форматы:
    # /profit                -> за сегодня
    # /profit day|month|year -> текущий период
    # /profit 2025-10-03     -> конкретный день
    # /profit 2025-10        -> конкретный месяц
    parts = msg.text.split(maxsplit=1)
    args = parts[1].strip().lower() if len(parts) > 1 else "day"

    def trunc(unit: str) -> str:
        return f"date_trunc('{unit}', NOW())"

    if args in ("day", "month", "year"):
        period_label = {"day": "сегодня", "month": "текущий месяц", "year": "текущий год"}[args]
        unit = args
        start_sql = trunc(unit)
        end_sql = f"{trunc(unit)} + interval '1 {unit}'"
    else:
        mday = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", args)
        mmon = re.fullmatch(r"(\d{4})-(\d{2})", args)
        if mday:
            y, m, d = map(int, mday.groups())
            period_label = f"{y:04d}-{m:02d}-{d:02d}"
            start_sql = f"TIMESTAMP WITH TIME ZONE '{y:04d}-{m:02d}-{d:02d} 00:00:00+00'"
            end_sql   = f"{start_sql} + interval '1 day'"
        elif mmon:
            y, m = map(int, mmon.groups())
            period_label = f"{y:04d}-{m:02d}"
            start_sql = f"TIMESTAMP WITH TIME ZONE '{y:04d}-{m:02d}-01 00:00:00+00'"
            end_sql   = f"{start_sql} + interval '1 month'"
        else:
            return await msg.answer("Формат: /profit [day|month|year|YYYY-MM|YYYY-MM-DD]")

    async with pool.acquire() as conn:
        # Итого по периоду
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

        # Детализация по дням (до 31 строки)
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
            )
            SELECT COALESCE(r.day, e.day) AS day,
                   COALESCE(r.revenue, 0)::numeric(12,2) AS revenue,
                   COALESCE(e.expense, 0)::numeric(12,2) AS expense,
                   (COALESCE(r.revenue, 0) - COALESCE(e.expense, 0))::numeric(12,2) AS profit
            FROM r FULL OUTER JOIN e ON r.day = e.day
            ORDER BY day DESC
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
        lines.append("\nПо дням (последние):")
        for r in rows:
            lines.append(f"{r['day']:%Y-%m-%d}: выручка {r['revenue']} / расходы {r['expense']} → прибыль {r['profit']}₽")

    await msg.answer("\n".join(lines))

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
    CREATE OR REPLACE FUNCTION trg_order_to_cashbook() RETURNS trigger AS $$
    DECLARE tx_id integer;
    BEGIN
      IF
        NEW.income_tx_id IS NULL
        AND (
          (NEW.amount_cash IS NOT NULL AND NEW.amount_cash > 0)
          OR (NEW.payment_method = 'Подарочный сертификат')
        )
      THEN
        INSERT INTO cashbook_entries(kind, method, amount, comment, order_id, happened_at)
        VALUES (
          'income',
          COALESCE(NEW.payment_method, 'прочее'),
          COALESCE(NEW.amount_cash, 0),
          CONCAT('Поступление по заказу #', NEW.id),
          NEW.id,
          now()
        )
        RETURNING id INTO tx_id;

        UPDATE orders SET income_tx_id = tx_id WHERE id = NEW.id;
      END IF;

      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    DROP TRIGGER IF EXISTS orders_to_cashbook_ai ON orders;

    CREATE TRIGGER orders_to_cashbook_ai
    AFTER INSERT ON orders
    FOR EACH ROW
    EXECUTE FUNCTION trg_order_to_cashbook();
    """
    async with pool.acquire() as conn:
        await conn.execute(sql)
    await msg.answer("✅ Функция и триггер `orders_to_cashbook_ai` обновлены успешно.")
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
                VALUES ($1, $2, $3, NULLIF($4,'')::date, $5)
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
    # попытка извлечь id заказа из комментария: понимаем "#123" или "order:123"
    order_id = None
    m = re.search(r"(?:^|\s)(?:order[:#]|#)(\d+)", comment, re.IGNORECASE)
    if m:
        try:
            order_id = int(m.group(1))
        except Exception:
            order_id = None

    async with pool.acquire() as conn:
        async with conn.transaction():
            rec = await conn.fetchrow(
                "INSERT INTO cashbook_entries (kind, method, amount, comment, order_id) "
                "VALUES ('income', $1, $2, $3, $4) RETURNING id, happened_at",
                method, amount, comment, order_id
            )
            if order_id is not None:
                await conn.execute(
                    "UPDATE orders SET income_tx_id = $1 WHERE id = $2",
                    rec['id'], order_id
                )

    lines = [
        f"✅ Приход №{rec['id']}",
        f"Сумма: {amount}₽",
        f"Тип оплаты: {method}",
        f"Когда: {rec['happened_at']:%Y-%m-%d %H:%M}",
        f"Комментарий: {comment}",
    ]
    if order_id:
        lines.insert(1, f"Заказ: #{order_id}")
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
        rec = await conn.fetchrow(
            "INSERT INTO cashbook_entries (kind, method, amount, comment) "
            "VALUES ('expense', 'прочее', $1, $2) RETURNING id, happened_at",
            amount, comment
        )
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
async def tx_last(msg: Message):
    if not await has_permission(msg.from_user.id, "view_cash_reports"):
        return await msg.answer("Только для администраторов.")
    parts = msg.text.split(maxsplit=1)
    limit = 10
    if len(parts) > 1 and parts[1].isdigit():
        limit = max(1, min(50, int(parts[1])))
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, kind, method, amount, comment, happened_at, COALESCE(is_deleted,false) AS del, order_id "
            "FROM cashbook_entries ORDER BY id DESC LIMIT $1",
            limit
        )
    if not rows:
        return await msg.answer("Нет транзакций.")
    lines = [
        f"№{r['id']} | {'❌' if r['del'] else '✅'} | {r['kind']} | {r['amount']}₽ | {r['method'] or '-'} | "
        f"{r['happened_at']:%Y-%m-%d %H:%M} | order=#{r['order_id']} | {r['comment'] or ''}"
        for r in rows
    ]
    await msg.answer("Последние транзакции:\n" + "\n".join(lines))

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

# Legacy env-based admin check kept for backward compatibility
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_TG_IDS

async def ensure_master(user_id: int) -> bool:
    # Master access is defined by permission to create orders/clients
    return await has_permission(user_id, "create_orders_clients")

@dp.message(CommandStart())
async def on_start(msg: Message):
    # автоматическая регистрация админа остаётся без изменений…
    # выбираем клавиатуру
    if await has_permission(msg.from_user.id, "add_master"):
        kb = main_kb  # админы видят стандартную клавиатуру
    elif await has_permission(msg.from_user.id, "view_own_salary"):
        kb = master_kb  # мастера видят мастер‑меню
    else:
        kb = main_kb  # на всякий случай
    await msg.answer(
        "Привет! Это внутренний бот.\nНажми нужную кнопку.",
        reply_markup=kb
    )
    # авто-регистрация админа как активного сотрудника
    if is_admin(msg.from_user.id):
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO staff(tg_user_id, role, is_active) VALUES ($1,'admin',true) "
                "ON CONFLICT (tg_user_id) DO UPDATE SET is_active=true",
                msg.from_user.id
            )

@dp.message(Command("help"))
async def help_cmd(msg: Message):
    if await has_permission(msg.from_user.id, "add_master"):
        # справка для админа
        await msg.answer(
            "Доступные команды:\n"
            "• /add_master <tg_id> — добавить мастера\n"
            "• /remove_master <tg_id> — отключить мастера\n"
            "• /list_masters — список мастеров\n"
            "• /payroll YYYY-MM — отчёт по зарплате за месяц\n\n"
            "Доступные кнопки:\n"
            "• 🧾 Заказ — добавить заказ\n"
            "• 🔍 Клиент — найти клиента\n"
            f"• {MASTER_SALARY_LABEL} — отчёт по зарплате\n"
            f"• {MASTER_INCOME_LABEL} — отчёт по выручке",
            reply_markup=main_kb
        )
    elif await has_permission(msg.from_user.id, "view_own_salary"):
        # справка для мастера
        await msg.answer(
            "Описание кнопок:\n"
            "• 🧾 Заказ — добавить заказ и клиента\n"
            "• 🔍 Клиент — найти клиента по номеру\n"
            f"• {MASTER_SALARY_LABEL} — отчёт по вашей зарплате (день, неделя, месяц, год)\n"
            f"• {MASTER_INCOME_LABEL} — выручка за сегодня по типам оплаты",
            reply_markup=master_kb
        )
    else:
        await msg.answer("Для работы используйте кнопки внизу.", reply_markup=main_kb)

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

@dp.message(F.text)
async def unknown(msg: Message, state: FSMContext):
    # Если пользователь находится в процессе любого сценария — не вмешиваемся
    cur = await state.get_state()
    if cur is not None:
        return
    # Не перехватываем команды вида /something
    if msg.text and msg.text.startswith("/"):
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
