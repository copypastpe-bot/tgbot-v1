import asyncio, os, re, logging
from decimal import Decimal, ROUND_DOWN
from datetime import date
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, BotCommand, BotCommandScopeDefault, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from aiogram.filters import CommandStart, Command, CommandObject
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from dotenv import load_dotenv
import asyncpg

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
    d = only_digits(s)
    if len(d) == 10 and d.startswith("9"):
        return "+7" + d
    if len(d) == 11 and d.startswith("8"):
        return "+7" + d[1:]
    if len(d) == 11 and d.startswith("7"):
        return "+" + d
    if d and not s.startswith("+"):
        return "+" + d
    return s

def qround_ruble(x: Decimal) -> Decimal:
    # округление вниз до рубля
    return x.quantize(Decimal("1."), rounding=ROUND_DOWN)

async def set_commands():
    cmds = [
        BotCommand(command="start", description="Старт"),
        BotCommand(command="help",  description="Справка"),
        BotCommand(command="find",  description="Поиск клиента по телефону"),
        BotCommand(command="payroll",  description="Отчёт по ЗП за месяц (админ)"),
        BotCommand(command="add_master",  description="Добавить мастера (админ)"),
        BotCommand(command="remove_master",  description="Отключить мастера (админ)"),
        BotCommand(command="list_masters",  description="Список мастеров (админ)"),
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
        await conn.execute(
            "INSERT INTO cashbook_entries (kind, method, amount, comment) "
            "VALUES ('expense', 'прочее', $1, $2)",
            amount, comment
        )

    await msg.answer(f"✅ Расход {amount}₽ добавлен: {comment}")

@dp.message(Command("mysalary"))
async def my_salary(msg: Message):
    # доступ только для мастеров (разрешение view_own_salary)
    if not await has_permission(msg.from_user.id, "view_own_salary"):
        return await msg.answer("Доступно только мастерам.")
    parts = msg.text.split(maxsplit=1)
    period = "month"
    if len(parts) > 1:
        period = parts[1].strip().lower()
    period_map = {
        "day": "day",
        "week": "week",
        "month": "month",
        "year": "year",
    }
    if period not in period_map:
        return await msg.answer("Формат: /mysalary [day|week|month|year]")
    # формируем начало периода для выборки (текущий день, неделя, месяц, год)
    start_expr = f"DATE_TRUNC('{period_map[period]}', NOW())"
    async with pool.acquire() as conn:
        rec = await conn.fetchrow(
            f\"\"\"
            SELECT
                COALESCE(SUM(pi.base_pay), 0) AS base_pay,
                COALESCE(SUM(pi.fuel_pay), 0) AS fuel_pay,
                COALESCE(SUM(pi.upsell_pay), 0) AS upsell_pay,
                COALESCE(SUM(pi.total_pay), 0) AS total_pay
            FROM payroll_items pi
            JOIN orders o ON o.id = pi.order_id
            WHERE pi.master_id = (SELECT id FROM staff WHERE tg_user_id=$1 AND is_active LIMIT 1)
              AND o.created_at >= {start_expr}
            \"\"\",\n            msg.from_user.id\n        )\n    if not rec:\n        return await msg.answer(\"Нет данных для указанного периода.\")\n    base_pay, fuel_pay, upsell_pay, total_pay = rec[\"base_pay\"], rec[\"fuel_pay\"], rec[\"upsell_pay\"], rec[\"total_pay\"]\n    await msg.answer(\n        f\"Зарплата за {period} (с {period_map[period]}):\\n\"\n        f\"Базовая оплата: {base_pay}₽\\n\"\n        f\"Оплата за бензин: {fuel_pay}₽\\n\"\n        f\"Оплата за доп. продажи: {upsell_pay}₽\\n\"\n        f\"Итого: {total_pay}₽\"\n    )\n```

### 2. Добавить обработчик `/myincome` (дневная выручка по типу оплаты)

```python
@dp.message(Command("myincome"))
async def my_income(msg: Message):
    # доступ только для мастеров (разрешение view_own_income)
    if not await has_permission(msg.from_user.id, "view_own_income"):
        return await msg.answer("Доступно только мастерам.")
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            \"\"\"\n            SELECT o.payment_method AS method,\n                   SUM(o.amount_cash) AS total\n            FROM orders o\n            WHERE o.master_id = (\n                SELECT id FROM staff WHERE tg_user_id=$1 AND is_active LIMIT 1\n            )\n              AND DATE_TRUNC('day', o.created_at) = DATE_TRUNC('day', NOW())\n            GROUP BY o.payment_method\n            \"\"\",\n            msg.from_user.id\n        )\n    if not rows:\n        return await msg.answer(\"Нет данных за сегодня.\")\n    lines = [f\"{row['method']}: {row['total']}₽\" for row in rows]\n    await msg.answer(\"Сегодняшний приход по типам оплаты:\\n\" + \"\\n\".join(lines))\n```

main_kb = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="🧾 Я ВЫПОЛНИЛ ЗАКАЗ")]],
    resize_keyboard=True
)

# Legacy env-based admin check kept for backward compatibility
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_TG_IDS

async def ensure_master(user_id: int) -> bool:
    # Master access is defined by permission to create orders/clients
    return await has_permission(user_id, "create_orders_clients")

@dp.message(CommandStart())
async def on_start(msg: Message):
    await msg.answer("Привет! Это внутренний бот.\nНажми «🧾 Я ВЫПОЛНИЛ ЗАКАЗ» или используй /find +7XXXXXXXXXX", reply_markup=main_kb)
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
    await msg.answer(
        "Доступно:\n"
        "• /find +7XXXXXXXXXX — карточка клиента\n"
        "• Кнопка «🧾 Я ВЫПОЛНИЛ ЗАКАЗ» — пошаговый ввод заказа\n"
        "• /payroll YYYY-MM — отчёт за месяц (админ)\n"
        "• /add_master @username|id — выдать роль мастера (админ)\n"
        "• /remove_master @username|id — отключить мастера (админ)\n"
        "• /list_masters — список мастеров (админ)"
        "• /mysalary [day|week|month|year] — ваш отчёт по зарплате\\n"
        "• /myincome — ваша выручка за сегодня по типам оплаты\\n",
        reply_markup=main_kb
    )

# ---- /find ----
@dp.message(Command("find"))
async def find_cmd(msg: Message):
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        return await msg.answer("Формат: /find +7XXXXXXXXXX или 9XXXXXXXXX")
    phone_in = normalize_phone_for_db(parts[1])
    async with pool.acquire() as conn:
        rec = await conn.fetchrow(
            "SELECT full_name, phone, bonus_balance, birthday, status "
            "FROM clients WHERE regexp_replace(phone,'[^0-9]+','','g')=regexp_replace($1,'[^0-9]+','','g')",
            phone_in
        )
    if not rec:
        return await msg.answer("Не найдено.")
    bd = rec["birthday"].isoformat() if rec["birthday"] else "—"
    await msg.answer(
        f"👤 {rec['full_name'] or 'Без имени'}\n"
        f"📞 {rec['phone']}\n"
        f"💳 {rec['bonus_balance']}\n"
        f"🎂 {bd}\n"
        f"🏷️ {rec['status'] or '—'}",
        reply_markup=main_kb
    )

# ===== FSM: Я ВЫПОЛНИЛ ЗАКАЗ =====
class OrderFSM(StatesGroup):
    phone = State()
    name = State()
    amount = State()
    upsell_flag = State()
    upsell_amount = State()
    bonus_spend = State()
    payment_method = State()
    maybe_bday = State()
    confirm = State()

@dp.message(F.text == "🧾 Я ВЫПОЛНИЛ ЗАКАЗ")
async def start_order(msg: Message, state: FSMContext):
    if not await ensure_master(msg.from_user.id):
        return await msg.answer("У вас нет прав мастера. Обратитесь к администратору.")
    await state.clear()
    await state.set_state(OrderFSM.phone)
    await msg.answer("Введите номер клиента (9XXXXXXXXX, 8XXXXXXXXXX или +7XXXXXXXXXX):", reply_markup=ReplyKeyboardRemove())

@dp.message(OrderFSM.phone, F.text)
async def got_phone(msg: Message, state: FSMContext):
    phone_in = normalize_phone_for_db(msg.text.strip())
    async with pool.acquire() as conn:
        client = await conn.fetchrow(
            "SELECT id, full_name, phone, bonus_balance, birthday "
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
        await state.set_state(OrderFSM.amount)
        return await msg.answer(f"Клиент найден: {client['full_name'] or 'Без имени'}\nБонусов: {data['bonus_balance']}\nВведите сумму чека (руб):")
    else:
        data["client_id"] = None
        data["bonus_balance"] = 0
        await state.update_data(**data)
        await state.set_state(OrderFSM.name)
        return await msg.answer("Клиент не найден. Введите имя клиента:")

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
    await msg.answer("Введите сумму чека (руб):")

@dp.message(OrderFSM.amount, F.text)
async def got_amount(msg: Message, state: FSMContext):
    amount = parse_money(msg.text)
    if amount is None:
        return await msg.answer("Нужно число ≥ 0. Введите сумму чека ещё раз:")
    await state.update_data(amount_total=amount)
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Да"), KeyboardButton(text="Нет")]], resize_keyboard=True)
    await state.set_state(OrderFSM.upsell_flag)
    await msg.answer("Была доп. продажа? (Да/Нет)", reply_markup=kb)

@dp.message(OrderFSM.upsell_flag, F.text.lower().in_(["да","нет"]))
async def got_upsell_flag(msg: Message, state: FSMContext):
    if msg.text.lower() == "да":
        await state.set_state(OrderFSM.upsell_amount)
        return await msg.answer("Введите сумму доп. продажи (руб):", reply_markup=ReplyKeyboardRemove())
    else:
        await state.update_data(upsell_amount=Decimal("0"))
        return await ask_bonus(msg, state)

@dp.message(OrderFSM.upsell_amount, F.text)
async def got_upsell_amount(msg: Message, state: FSMContext):
    v = parse_money(msg.text)
    if v is None:
        return await msg.answer("Нужно число ≥ 0. Введите сумму доп. продажи ещё раз:")
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
        await state.set_state(OrderFSM.payment_method)
        kb = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="нал"), KeyboardButton(text="карта"), KeyboardButton(text="перевод")]],
            resize_keyboard=True
        )
        return await msg.answer(
            "Бонусов нет — пропускаем списание.\n"
            f"Оплата деньгами: {amount}\nВыберите способ оплаты:",
            reply_markup=kb
        )

    # иначе — задаём выбор списания
    await state.update_data(bonus_max=bonus_max)
    await state.set_state(OrderFSM.bonus_spend)
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="Списать 0"), KeyboardButton(text="Списать 50%"), KeyboardButton(text="Списать MAX")],
        [KeyboardButton(text="Другая сумма")]
    ], resize_keyboard=True)
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
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="нал"), KeyboardButton(text="карта"), KeyboardButton(text="перевод")]], resize_keyboard=True)
    await state.set_state(OrderFSM.payment_method)
    await msg.answer(f"Оплата деньгами: {cash_payment}\nВыберите способ оплаты:", reply_markup=kb)

@dp.message(OrderFSM.payment_method, F.text.lower().in_(["нал","карта","перевод"]))
async def got_method(msg: Message, state: FSMContext):
    await state.update_data(payment_method=msg.text.lower())
    data = await state.get_data()
    if data.get("birthday"):
        await state.set_state(OrderFSM.confirm)
        return await show_confirm(msg, state)
    else:
        await state.set_state(OrderFSM.maybe_bday)
        return await msg.answer("Если знаете ДР клиента, введите ДД.ММ (или '-' чтобы пропустить):", reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="-")]], resize_keyboard=True))

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
    bonus_earned = qround_ruble(cash_payment * BONUS_RATE)
    base_pay = qround_ruble(cash_payment * (MASTER_PER_3000 / Decimal(3000)))
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
    await msg.answer("Отменено.", reply_markup=main_kb)

@dp.message(OrderFSM.confirm, F.text.lower() == "подтвердить")
async def commit_order(msg: Message, state: FSMContext):
    data = await state.get_data()
    phone_in = data["phone_in"]
    amount = Decimal(str(data["amount_total"]))
    upsell = Decimal(str(data.get("upsell_amount", 0)))
    bonus_spent = int(Decimal(str(data.get("bonus_spent", 0))))
    cash_payment = Decimal(str(data["amount_cash"]))
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
                client_id, msg.from_user.id, phone_in, amount, cash_payment, upsell,
                bonus_spent, bonus_earned, data["payment_method"]
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
                "INSERT INTO cashbook_entries (kind, method, amount, comment, order_id) "
                "VALUES ('income', $1, $2, 'Оплата за заказ', $3)",
                data["payment_method"], cash_payment, order_id
            )

            await conn.execute(
                "INSERT INTO payroll_items (order_id, master_id, base_pay, fuel_pay, upsell_pay, total_pay, calc_info) "
                "VALUES ($1, (SELECT id FROM staff WHERE tg_user_id=$2), $3, $4, $5, $6, "
                "        jsonb_build_object('cash_payment', to_jsonb(($7)::numeric), 'rules', '1000/3000 + 150 + 500/3000'))",
                order_id, msg.from_user.id, base_pay, fuel_pay, upsell_pay, total_pay, cash_payment
            )

    await state.clear()
    await msg.answer("Готово ✅ Заказ сохранён.\nСпасибо!", reply_markup=main_kb)

# fallback
@dp.message(F.text)
async def unknown(msg: Message):
    # Не перехватываем команды вида /something
    if msg.text and msg.text.startswith("/"):
        return
    await msg.answer("Команда не распознана. Нажми «🧾 Я ВЫПОЛНИЛ ЗАКАЗ» или /help", reply_markup=main_kb)

async def main():
    global pool
    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=1, max_size=5)
    await set_commands()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
