#!/usr/bin/env python3
"""
Скрипт для отправки сообщения о корректировке в чат "Карта Жени".
Использование: python3 send_jenya_card_message.py <баланс>
"""

import asyncio
import os
import sys
from decimal import Decimal

from aiogram import Bot
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
JENYA_CARD_CHAT_ID = int(os.getenv("JENYA_CARD_CHAT_ID", "0") or "0")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")
if not JENYA_CARD_CHAT_ID:
    raise RuntimeError("JENYA_CARD_CHAT_ID is not set")


def format_money(amount: Decimal | float | str) -> str:
    """Форматирует сумму для отображения."""
    if isinstance(amount, str):
        amount = Decimal(amount)
    elif isinstance(amount, float):
        amount = Decimal(str(amount))
    return f"{amount:,.2f}".replace(",", " ").replace(".", ",")


async def send_message():
    """Отправляет сообщение о корректировке."""
    if len(sys.argv) < 2:
        print("Использование: python3 send_jenya_card_message.py <баланс>")
        print("Пример: python3 send_jenya_card_message.py 33253.00")
        sys.exit(1)
    
    try:
        balance = Decimal(sys.argv[1])
    except Exception as e:
        print(f"Ошибка: неверный формат баланса: {e}")
        sys.exit(1)
    
    bot = Bot(token=BOT_TOKEN)
    
    try:
        message = (
            f"➖{format_money(Decimal('5000.00'))}₽ Карта Жени — Корректировка заказа №162 (разделение оплат)\n"
            f"Остаток: {format_money(balance)}₽"
        )
        await bot.send_message(JENYA_CARD_CHAT_ID, message)
        print(f"✅ Сообщение отправлено в чат {JENYA_CARD_CHAT_ID}")
        print(f"Сообщение:\n{message}")
    except Exception as exc:
        print(f"❌ Ошибка при отправке сообщения: {exc}")
        sys.exit(1)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(send_message())

