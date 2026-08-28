"""Правила разговора с клиентом до работы.

Чистые функции: ни сети, ни базы. Здесь решается, когда спрашивать, как понять
ответ и когда звать владельца, — и всё это проверяется тестами. Запись в базу,
отправка писем и походы в CRM живут отдельным тонким слоем в `bot.py`.

Главное правило: **непонятое не гадаем.** Робот различает ровно «да» и «нет»,
потому что сам об этом и просит («Ответьте одним словом»). Всё остальное —
«перенесите на среду», «не знаю», «а можно позже» — уходит владельцу целиком:
попытка угадать смысл здесь стоит дороже, чем лишнее сообщение владельцу.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

ASK_BEFORE = timedelta(days=1)        # за сутки от времени заказа (решение владельца)
SILENCE_LIMIT = timedelta(hours=3)    # столько ждём ответа, потом зовём владельца

# Только однозначные слова. «Не» сюда не входит намеренно: «не знаю», «не уверен»,
# «не смогу сказать» — это разговор, а не отказ, и вести его должен владелец.
_YES = {"да", "ага", "верно", "подтверждаю", "подтверждаем"}
_NO = {"нет", "отмена", "отменяю", "отменяем"}

_PUNCTUATION = "!.,;:?)("


@dataclass(frozen=True)
class ConfirmationPlan:
    """Что робот сделает с только что оформленным заказом."""

    send_confirmation: bool           # слать ли письмо «заказ принят»
    ask_at: Optional[datetime]        # когда задать вопрос; None — не спрашиваем
    reason: str = ""                  # человеческая причина: она идёт в журнал


def parse_answer(text: Optional[str]) -> str:
    """`yes`, `no` или `unclear`. Смотрим первое слово — его и просили прислать."""
    cleaned = (text or "").strip().lower()
    if not cleaned:
        return "unclear"
    first = cleaned.split()[0].strip(_PUNCTUATION)
    if first in _YES:
        return "yes"
    if first in _NO:
        return "no"
    return "unclear"


def plan_confirmation(*, order_at: Optional[datetime],
                      now: datetime) -> ConfirmationPlan:
    """Что делать с только что оформленным заказом."""
    if order_at is None:
        return ConfirmationPlan(False, None, "в сделке нет даты заказа")
    if order_at <= now:
        return ConfirmationPlan(False, None, "работа уже прошла")

    ask_at = order_at - ASK_BEFORE
    if ask_at <= now:
        # Решение владельца: до работы меньше суток — только подтверждение,
        # вопроса нет. Такие заказы он и так ведёт вручную.
        return ConfirmationPlan(True, None, "до работы меньше суток")
    return ConfirmationPlan(True, ask_at, "вопрос за сутки до работы")


def should_call_owner(*, asked_at: Optional[datetime],
                      notified_at: Optional[datetime], now: datetime) -> bool:
    """Пора ли звать владельца к молчащему клиенту. Зовём ровно один раз.

    `asked_at` пуст, пока вопрос не ушёл: молчания ещё нет, звать не о чем.
    `notified_at` заполнен — владельца уже позвали, второй раз не тревожим.
    """
    if asked_at is None or notified_at is not None:
        return False
    return now - asked_at >= SILENCE_LIMIT


__all__ = [
    "ASK_BEFORE",
    "SILENCE_LIMIT",
    "ConfirmationPlan",
    "parse_answer",
    "plan_confirmation",
    "should_call_owner",
]
