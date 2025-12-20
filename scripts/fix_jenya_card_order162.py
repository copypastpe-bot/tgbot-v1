#!/usr/bin/env python3
"""
Скрипт для исправления записи в реестре "Карта Жени" для заказа №162.
Уменьшает запись на 5000₽ (корректировка разделения оплат).
Выполняет все действия: создает корректирующую запись и отправляет сообщение в чат.
"""

import asyncio
import os
import sys
from decimal import Decimal

import asyncpg
from aiogram import Bot
from dotenv import load_dotenv

# Добавляем корневую директорию проекта в путь для импорта функций
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

DB_DSN = os.getenv("DB_DSN")
BOT_TOKEN = os.getenv("BOT_TOKEN")
JENYA_CARD_CHAT_ID = int(os.getenv("JENYA_CARD_CHAT_ID", "0") or "0")

if not DB_DSN:
    raise RuntimeError("DB_DSN is not set")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")


def format_money(amount: Decimal) -> str:
    """Форматирует сумму для отображения."""
    return f"{amount:,.2f}".replace(",", " ").replace(".", ",")


async def get_jenya_card_balance(conn: asyncpg.Connection) -> Decimal:
    """Получает текущий баланс карты Жени."""
    row = await conn.fetchrow(
        """
        SELECT
          COALESCE(SUM(CASE WHEN kind IN ('income','opening_balance') THEN amount ELSE 0 END),0) AS income_sum,
          COALESCE(SUM(CASE WHEN kind='expense' THEN amount ELSE 0 END),0) AS expense_sum
        FROM jenya_card_entries
        WHERE COALESCE(is_deleted,false)=FALSE
        """
    )
    inc = Decimal(row["income_sum"] or 0)
    exp = Decimal(row["expense_sum"] or 0)
    return inc - exp


async def fix_order_162():
    """Исправляет запись в реестре 'Карта Жени' для заказа №162."""
    print("🔧 Начинаю корректировку заказа №162...")
    
    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=1, max_size=2)
    bot = Bot(token=BOT_TOKEN)
    
    try:
        async with pool.acquire() as conn:
            # Создаем корректирующую запись (expense) на 5000₽
            correction_amount = Decimal("5000.00")
            comment = "Корректировка заказа №162 (разделение оплат)"
            
            print(f"📝 Создаю корректирующую запись на {format_money(correction_amount)}₽...")
            
            correction_entry = await conn.fetchrow(
                """
                INSERT INTO jenya_card_entries(kind, amount, comment, happened_at, created_at)
                VALUES ('expense', $1, $2, NOW(), NOW())
                RETURNING id, happened_at, kind, amount, comment
                """,
                correction_amount,
                comment
            )
            
            print(f"✅ Создана корректирующая запись: ID={correction_entry['id']}")
            
            # Получаем новый баланс
            new_balance = await get_jenya_card_balance(conn)
            print(f"✅ Новый остаток карты Жени: {format_money(new_balance)}₽")
            
            # Отправляем сообщение в чат
            if JENYA_CARD_CHAT_ID:
                try:
                    message = (
                        f"➖{format_money(correction_amount)}₽ Карта Жени — {comment}\n"
                        f"Остаток: {format_money(new_balance)}₽"
                    )
                    await bot.send_message(JENYA_CARD_CHAT_ID, message)
                    print(f"✅ Сообщение отправлено в чат {JENYA_CARD_CHAT_ID}")
                    print(f"\n📨 Сообщение:\n{message}")
                except Exception as exc:
                    print(f"❌ Не удалось отправить сообщение в чат: {exc}")
                    sys.exit(1)
            else:
                print("⚠️ JENYA_CARD_CHAT_ID не установлен, сообщение не отправлено")
            
            print("\n✅ Корректировка выполнена успешно!")
            
    except Exception as exc:
        print(f"❌ Ошибка при выполнении корректировки: {exc}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        await bot.session.close()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(fix_order_162())

