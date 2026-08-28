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
from datetime import datetime, timedelta, tzinfo
from typing import Any, Mapping, Optional

from .amo_exchange import AMO_FIELD_ADDRESS, field_values

ASK_BEFORE = timedelta(days=1)        # за сутки от времени заказа (решение владельца)
SILENCE_LIMIT = timedelta(hours=3)    # столько ждём ответа, потом зовём владельца

# Сколько ожидание живёт после времени работы. Ответ «да», пришедший через
# неделю, подтверждать уже нечего: мастер либо съездил, либо нет.
PENDING_TTL_AFTER_ORDER = timedelta(days=1)

# Воронка реализации и её этапы. Номера этапов в разных воронках свои, поэтому
# воронку проверяем всегда: без этого робот написал бы клиентам чужих сделок.
AMO_PIPELINE_REALIZATION = 4482787      # Воронка для реализации
AMO_STAGE_ORDER_CREATED = 41463832      # «Заказ оформлен» — с него начинается разговор
AMO_STAGE_CONFIRMED = 41463838          # «Заказ подтвержден, Мастер назначен»

AMO_FIELD_ORDER_DATETIME = 18701        # Дата и время заказа (unix)

# Ответ клиента владельцу показывается целиком, а не пересказом. Предел стоит
# только против случайной простыни на сотни строк: телеграм её всё равно
# не примет, а реальные ответы короче одной строки.
ANSWER_LIMIT = 1500

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

    `asked_at` пуст, когда вопроса не было (заказ меньше чем за сутки): молчать
    там не о чем. `notified_at` заполнен — владельца уже позвали, второй раз
    не тревожим.
    """
    if asked_at is None or notified_at is not None:
        return False
    return now - asked_at >= SILENCE_LIMIT


def child_deal_id(notes: list[Mapping[str, Any]]) -> Optional[int]:
    """Дочерняя сделка, заведённая роботом владельца, — по примечанию лида.

    Когда робот переносит заказ из календаря в CRM, amoCRM оставляет в лиде
    примечание `lead_auto_created` со ссылкой на созданную сделку реализации.
    Это прямая связь родитель-подчинённая: она точнее любого подбора по дате
    и не путается, когда у клиента два заказа подряд.

    Примечаний может быть несколько — берём самое свежее.
    """
    best: Optional[int] = None
    best_at = -1
    for note in notes:
        if str(note.get("note_type") or "") != "lead_auto_created":
            continue
        params = note.get("params") or {}
        deal_id = params.get("lead_id") or (params.get("link") or {}).get("id")
        if not deal_id:
            continue
        created_at = int(note.get("created_at") or 0)
        if created_at >= best_at:
            best, best_at = int(deal_id), created_at
    return best


def pick_realization_deal(deals: list[Mapping[str, Any]], *,
                          order_at_raw: Optional[str],
                          contact_ids: Optional[set[int]] = None) -> Optional[int]:
    """Найти сделку реализации, парную только что оформленному заказу.

    Робот владельца заводит её той же минутой, что и лид, с той же датой работы
    (проверено на живых данных 2026-08-28). Её и двигают по ответу «Да».

    Сходство по дате работы — главный признак: у постоянного клиента в этой
    воронке десятки сделок, и «самая свежая» на второй заказ подряд указала бы
    не на ту. Совпадения нет — не гадаем: пусть лучше сделку подвинет человек.
    """
    if not order_at_raw:
        return None
    for deal in deals:
        if int(deal.get("pipeline_id") or 0) != AMO_PIPELINE_REALIZATION:
            continue
        if int(deal.get("status_id") or 0) != AMO_STAGE_ORDER_CREATED:
            continue
        if str(next(iter(field_values(dict(deal), AMO_FIELD_ORDER_DATETIME)), "")) != str(order_at_raw):
            continue
        if contact_ids:
            linked = {int(contact.get("id") or 0)
                      for contact in ((deal.get("_embedded") or {}).get("contacts") or [])}
            if linked and not (linked & contact_ids):
                continue
        return int(deal["id"])
    return None


@dataclass(frozen=True)
class OrderDetails:
    """Что известно о работе из карточки лида."""

    order_at: Optional[datetime]
    address: Optional[str]
    order_at_raw: Optional[str] = None    # как лежит в CRM — по нему ищем парную сделку


def order_from_lead(lead: Mapping[str, Any], *, tz: tzinfo) -> OrderDetails:
    """Дата работы и адрес из лида.

    Оба поля заполняет робот владельца, когда переносит заказ из календаря
    в CRM, — поэтому вторая сделка для письма клиенту не нужна.

    Дата приходит меткой времени; сырое значение сохраняется, чтобы по нему
    потом найти парную сделку реализации. Испорченное значение (поле правили
    руками) не роняет проход: письма без даты не будет, остальные разберутся.
    """
    order_at: Optional[datetime] = None
    raw = next(iter(field_values(dict(lead), AMO_FIELD_ORDER_DATETIME)), None)
    if raw is not None:
        try:
            order_at = datetime.fromtimestamp(int(raw), tz=tz)
        except (TypeError, ValueError, OSError, OverflowError):
            order_at = None
    address = next(iter(field_values(dict(lead), AMO_FIELD_ADDRESS)), None)
    return OrderDetails(order_at=order_at, address=address, order_at_raw=raw)


def letter_payload(*, order_at: Optional[datetime],
                   address: Optional[str]) -> dict[str, str]:
    """Переменные писем клиенту: дата, время, адрес."""
    return {
        "date": order_at.strftime("%d.%m.%Y") if order_at else "",
        "time": order_at.strftime("%H:%M") if order_at else "",
        "address": address or "",
    }


def decide_on_answer(*, answer: str, status: str) -> str:
    """Что делать с ответом клиента: `confirm`, `call_owner` или `ignore`.

    `owner_notified` означает, что владелец уже в разговоре: второй раз его
    об одном заказе не тревожим. Но «да» принимаем и после сигнала — решение
    владельца 7: клиент подтвердил, значит сделке место в подтверждённых.
    """
    if status not in ("planned", "owner_notified"):
        return "ignore"                       # с этим заказом всё уже решено
    if answer == "yes":
        return "confirm"
    if status == "owner_notified":
        return "ignore"
    return "call_owner"                       # «нет» и всё непонятое — владельцу


def should_move_deal(status_id: Optional[int]) -> bool:
    """Двигать ли сделку в «Заказ подтвержден».

    Только если она всё ещё в «Заказ оформлен». Ушла дальше — значит ею занялся
    человек, и робот его работу не откатывает (решение владельца 7).
    """
    return status_id is not None and int(status_id) == AMO_STAGE_ORDER_CREATED


_ALERT_HEADS = {
    "refused": "❗️ Клиент отказался от заказа",
    "unclear": "❓ Клиент ответил не «да» и не «нет»",
    "silence": "🔕 Клиент не ответил на подтверждение",
}


def owner_alert_text(*, kind: str, name: Optional[str], phone: Optional[str],
                     order_at: Optional[datetime], answer_text: Optional[str],
                     lead_link: Optional[str]) -> str:
    """Сообщение владельцу: что случилось, с кем и куда звонить.

    Телефон целиком и дата заказа — решение владельца 2026-08-26: бот приватный,
    получатель у него один, и ему нужно позвонить клиенту, не открывая CRM.
    Ответ клиента приводится дословно: пересказ непонятого и есть то самое
    угадывание, которого робот избегает.
    """
    lines = [_ALERT_HEADS.get(kind, "❗️ Заказ требует внимания")]
    lines.append(f"Клиент: {name or 'без имени'}")
    lines.append(f"Телефон: {phone or 'неизвестен'}")
    if order_at is not None:
        lines.append(f"Работа: {order_at.strftime('%d.%m.%Y')} в "
                     f"{order_at.strftime('%H:%M')}")
    if answer_text:
        lines.append(f"Ответ клиента: {answer_text.strip()[:ANSWER_LIMIT]}")
    if kind == "silence":
        lines.append(f"Молчит {int(SILENCE_LIMIT.total_seconds() // 3600)} часа "
                     f"после вопроса.")
    if lead_link:
        lines.append(f"Сделка: {lead_link}")
    return "\n".join(lines)


__all__ = [
    "AMO_FIELD_ORDER_DATETIME",
    "AMO_PIPELINE_REALIZATION",
    "AMO_STAGE_CONFIRMED",
    "AMO_STAGE_ORDER_CREATED",
    "ASK_BEFORE",
    "PENDING_TTL_AFTER_ORDER",
    "SILENCE_LIMIT",
    "ConfirmationPlan",
    "OrderDetails",
    "child_deal_id",
    "decide_on_answer",
    "letter_payload",
    "pick_realization_deal",
    "order_from_lead",
    "parse_answer",
    "plan_confirmation",
    "should_call_owner",
    "should_move_deal",
]
