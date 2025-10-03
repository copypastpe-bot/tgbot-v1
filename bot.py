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

# Payment method normalizer (Python side to mirror SQL norm_pay_method)
def norm_pay_method_py(p: str | None) -> str:
    if not p:
        return 'прочее'
    x = (p or '').strip().lower()
    # collapse inner spaces
    while '  ' in x:
        x = x.replace('  ', ' ')
    if 'нал' in x:
        return 'наличные'
    if x.startswith('карта дима') or x.startswith('дима'):
        return 'карта дима'
    if x.startswith('карта женя') or x.startswith('женя'):
        return 'карта женя'
    if 'р/с' in x or 'р\с' in x or 'расчет' in x or 'счет' in x:
        return 'р/с'
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
        await conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_clients_phone_digits ON clients (regexp_replace(COALESCE(phone,''),'[^0-9]+','','g'))"
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
            await conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_clients_phone_digits ON clients (regexp_replace(COALESCE(phone,''),'[^0-9]+','','g'))"
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

    async with pool.acquire() as conn:
        rec = await conn.fetchrow(
            "INSERT INTO cashbook_entries (kind, method, amount, comment) VALUES ('income', $1, $2, $3) RETURNING id, happened_at",
            method, amount, comment
        )

    await msg.answer(
        "\n".join([
            f"✅ Приход №{rec['id']}",
            f"Сумма: {amount}₽",
            f"Тип оплаты: {method}",
            f"Когда: {rec['happened_at']:%Y-%m-%d %H:%M}",
            f"Комментарий: {comment}",
        ])
    )

# ===== /expense admin command =====
@dp.message(Command("expense"))
async def add_expense(msg: Message, command: CommandObject):
    if not await has_permission(msg.from_user.id, "record_cashflows"):
        return await msg.answer("Только для администраторов.")

    # Разбор аргументов из текста: /expense <сумма> <комментарий>
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        return await msg.answer("Формат: /expense <сумма> <комментарий>")
    tail = parts[1].strip()
    tokens = tail.split()
    if len(tokens) < 2:
        return await msg.answer("Нужно указать сумму и комментарий. Формат: /expense <сумма> <комментарий>")
    amount_str = tokens[0]
    comment = ' '.join(tokens[1:]) if len(tokens) > 1 else ''
    try:
        amount = Decimal(amount_str)
        if amount <= 0:
            return await msg.answer("Сумма должна быть положительным числом.")
    except Exception:
        return await msg.answer(f"Ошибка: '{amount_str}' не является корректной суммой.")

    async with pool.acquire() as conn:
        rec = await conn.fetchrow(
            "INSERT INTO cashbook_entries (kind, method, amount, comment) VALUES ('expense', 'прочее', $1, $2) RETURNING id, happened_at",
            amount, comment
        )

    await msg.answer(
        "\n".join([
            f"✅ Расход №{rec['id']}",
            f"Сумма: {amount}₽",
            f"Комментарий: {comment}",
            f"Когда: {rec['happened_at']:%Y-%m-%d %H:%M}",
        ])
    )

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
            "UPDATE cashbook_entries SET is_deleted = TRUE, deleted_at = NOW() WHERE id = $1 AND COALESCE(is_deleted, FALSE) = FALSE RETURNING id",
            tx_id
        )
    if not rec:
        return await msg.answer("Транзакция не найдена или уже удалена.")
    await msg.answer(f"🗑️ Транзакция №{tx_id} помечена как удалённая.")

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
            "SELECT id, kind, method, amount, comment, happened_at, COALESCE(is_deleted,false) AS del FROM cashbook_entries ORDER BY id DESC LIMIT $1",
            limit
        )
    if not rows:
        return await msg.answer("Нет транзакций.")
    lines = [
        f"№{r['id']} | {'❌' if r['del'] else '✅'} | {r['kind']} | {r['amount']}₽ | {r['method'] or '-'} | {r['happened_at']:%Y-%m-%d %H:%M} | {r['comment'] or ''}"
        for r in rows
    ]
    await msg.answer("Последние транзакции:\n" + "\n".join(lines))