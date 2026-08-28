# Разговор с клиентом до работы: план реализации

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Клиент получает подтверждение сразу после оформления заказа и вопрос «подтверждаете?» за сутки до работы; ответ «Да» двигает сделку в CRM, «Нет» и молчание зовут владельца.

**Architecture:** Правила (когда слать, как понять ответ, что делать дальше) — чистые функции в `notifications/client_messaging.py`, покрытые тестами. Тонкий слой в `bot.py`: опрос событий amoCRM (тот же приём, что в автообмене), своя таблица ожиданий, постановка писем в существующую очередь `notification_outbox`, разбор ответа внутри `handle_wahelp_inbound`.

**Tech Stack:** Python 3.11, aiohttp, asyncpg, unittest. Тесты: `.venv-wahelp/bin/python -m unittest tests.<module>`.

**Дизайн:** `docs/plans/2026-08-28-client-messaging-design.md`. Решения владельца от 2026-08-28 — там же и ниже.

---

## Что уже готово и переиспользуется

| Что | Где | Роль |
|---|---|---|
| Опрос событий amoCRM | `bot.py:_amocrm_poll_exchange_once` | образец: курсор, дедуп, отсев по `value_after` |
| `outcome_of_event` | `notifications/amo_exchange.py` | образец разбора события смены этапа |
| Очередь с отложенной отправкой | `notifications/outbox.py:enqueue_notification` | письмо «за сутки» через `scheduled_at` |
| Выбор канала и повтор | `crm/wahelp_dispatcher.py` | WhatsApp → Telegram → MAX, ретрай при недоставке |
| Приём ответов клиента | `bot.py:handle_wahelp_inbound` | сюда добавляется ветка «Да/Нет» |
| Тексты писем | `docs/notification_rules.json` | `order_created` уже есть, нужен `order_confirm_request` |

## Как письмо доходит до клиента (разобрано с владельцем 2026-08-28)

1. Получателем может быть **только запись из `clients`**: в очередь кладётся
   `client_id`, а не телефон. Нет записи — письма не будет. Отсюда зависимость
   от автообмена.
2. Письмо кладётся в `notification_outbox` с `scheduled_at` — так и работает
   «за сутки до работы».
3. `NotificationWorker` отсеивает: нет клиента, уведомления отключены, нет
   телефона, стоит `wahelp_requires_connection` — запись отменяется с причиной.
4. Канал выбирается по истории клиента (`client_channels`); истории нет —
   перебор по телефону: **WhatsApp → Telegram → MAX**.
5. Отправка через Wahelp: ему отдаётся номер, он сам находит человека
   в мессенджере. Заранее неизвестно, есть ли у клиента WhatsApp.
6. Статусы: отправлено → доставлено → прочитано. Если через 10 минут только
   «отправлено», канал считается глухим и пробуется следующий.

Важное: **клиентский бот компании для служебных писем отключён** (пометка
в `send_with_rules`), всё идёт через Wahelp, включая телеграм.

Ограничения, которые надо держать в голове:

- Дневной лимит отправок — 60 на всю компанию (`DAILY_SEND_LIMIT`), и в него
  уже входят оценки, бонусы, поздравления, реактивация. Две новые буквы на
  заказ при пяти заказах в день — это плюс десять.
- Клиент без мессенджеров вообще не получит письма. **Решение владельца:
  порядок для всех один** — молчит три часа, значит зовём владельца; причина
  молчания (не ответил или не дошло) роли не играет. Отдельной ветки для
  недоставки не делаем.

## Константы amoCRM

```python
AMO_PIPELINE_REALIZATION = 4482787   # Воронка для реализации
AMO_STAGE_ORDER_CREATED = 41463832   # «Заказ оформлен» — триггер письма
AMO_STAGE_CONFIRMED = 41463838       # «Заказ подтвержден, Мастер назначен»
AMO_FIELD_ORDER_DATETIME = 18701     # Дата и время заказа (unix)
AMO_FIELD_ADDRESS = 18639            # Адрес
```

## Решения владельца, которые план обязан соблюсти

1. Письмо при оформлении — **без мастера** (на этом этапе он не назначен). Текущий шаблон `order_created` упоминает мастера: его надо поправить.
2. Вопрос уходит **за сутки от времени заказа**, без ограничения по часам.
3. «Да» → сделка в «Заказ подтвержден», владельцу молчание.
4. «Нет» и всё непонятое → сигнал владельцу, сделку не трогаем.
5. Молчание 3 часа → сигнал владельцу, ровно один раз.
6. Заказ оформлен меньше чем за сутки → только подтверждение, вопроса и сигналов нет.
7. Поздний ответ «Да» (после сигнала) → двигаем сделку, если она ещё в «Заказ оформлен»; владельцу не пишем.
8. Задачи в amoCRM робот не трогает.

---

## Задача 1: Таблица ожиданий

**Files:**
- Modify: `bot.py` (рядом с `ensure_amocrm_api_schema`, около строки 990)

**Step 1: Схема**

```python
async def ensure_client_messaging_schema(conn: asyncpg.Connection) -> None:
    """Ожидания подтверждения заказа: по одной строке на сделку.

    Ключ — сделка amoCRM, а не клиент: у клиента бывает два заказа подряд,
    и ответ «Да» должен подтвердить тот, о котором спросили последним.
    """
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS order_confirmations (
            lead_id      bigint PRIMARY KEY,      -- сделка воронки реализации
            client_id    bigint,                  -- clients.id, если клиент найден
            phone_digits text NOT NULL,           -- по нему ищем ответ клиента
            order_at     timestamptz,             -- когда работа
            status       text NOT NULL,           -- planned|asked|confirmed|refused|owner_notified
            asked_at     timestamptz,             -- когда ушёл вопрос
            answered_at  timestamptz,
            answer_text  text,
            notified_at  timestamptz,             -- когда позвали владельца
            created_at   timestamptz NOT NULL DEFAULT NOW(),
            updated_at   timestamptz NOT NULL DEFAULT NOW()
        );
        """
    )
    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_order_confirmations_phone
        ON order_confirmations(phone_digits, status);
        """
    )
```

Вызвать из того же места, где вызываются остальные `ensure_*_schema` при старте.

**Step 2: Коммит**

```bash
git add bot.py
git commit -m "feat(messaging): таблица ожиданий подтверждения заказа"
```

---

## Задача 2: Правила — когда спрашивать и как понимать ответ

**Files:**
- Create: `notifications/client_messaging.py`
- Test: `tests/test_client_messaging.py`

**Step 1: Написать падающий тест**

```python
import unittest
from datetime import datetime, timedelta, timezone

from notifications.client_messaging import (
    ASK_BEFORE,
    SILENCE_LIMIT,
    parse_answer,
    plan_confirmation,
    should_call_owner,
)

MSK = timezone(timedelta(hours=3))


class ParseAnswerTests(unittest.TestCase):
    def test_yes_variants(self):
        for text in ("Да", "да", "ДА", "да ", "да!", "Да, жду"):
            self.assertEqual(parse_answer(text), "yes", text)

    def test_no_variants(self):
        for text in ("Нет", "нет", "НЕТ", "нет!", "Нет, отмена"):
            self.assertEqual(parse_answer(text), "no", text)

    def test_anything_else_is_unclear(self):
        """Непонятое не гадаем — оно уходит владельцу."""
        for text in ("перенесите на среду", "5", "", "ok", "давайте"):
            self.assertEqual(parse_answer(text), "unclear", text)

    def test_stop_is_not_an_answer(self):
        """STOP — отписка, ей занимается другая ветка."""
        self.assertEqual(parse_answer("STOP"), "unclear")


class PlanConfirmationTests(unittest.TestCase):
    NOW = datetime(2026, 8, 28, 12, 0, tzinfo=MSK)

    def test_question_is_scheduled_a_day_before(self):
        order_at = self.NOW + timedelta(days=3)
        plan = plan_confirmation(order_at=order_at, now=self.NOW)
        self.assertTrue(plan.send_confirmation)
        self.assertEqual(plan.ask_at, order_at - ASK_BEFORE)

    def test_urgent_order_gets_no_question(self):
        """Заказ меньше чем за сутки: только подтверждение, вопроса нет."""
        plan = plan_confirmation(order_at=self.NOW + timedelta(hours=5), now=self.NOW)
        self.assertTrue(plan.send_confirmation)
        self.assertIsNone(plan.ask_at)

    def test_order_without_date_gets_nothing(self):
        plan = plan_confirmation(order_at=None, now=self.NOW)
        self.assertFalse(plan.send_confirmation)
        self.assertIsNone(plan.ask_at)

    def test_past_order_gets_nothing(self):
        """Сделку могли оформить задним числом — писать про вчера незачем."""
        plan = plan_confirmation(order_at=self.NOW - timedelta(hours=1), now=self.NOW)
        self.assertFalse(plan.send_confirmation)


class SilenceTests(unittest.TestCase):
    NOW = datetime(2026, 8, 28, 12, 0, tzinfo=MSK)

    def test_owner_is_called_after_three_hours(self):
        asked_at = self.NOW - SILENCE_LIMIT - timedelta(minutes=1)
        self.assertTrue(should_call_owner(asked_at=asked_at, notified_at=None,
                                          now=self.NOW))

    def test_owner_is_not_called_too_early(self):
        asked_at = self.NOW - timedelta(hours=1)
        self.assertFalse(should_call_owner(asked_at=asked_at, notified_at=None,
                                           now=self.NOW))

    def test_owner_is_called_only_once(self):
        asked_at = self.NOW - timedelta(hours=5)
        self.assertFalse(should_call_owner(asked_at=asked_at,
                                           notified_at=self.NOW - timedelta(hours=1),
                                           now=self.NOW))


if __name__ == "__main__":
    unittest.main()
```

**Step 2: Запустить и убедиться, что падает**

Run: `.venv-wahelp/bin/python -m unittest tests.test_client_messaging`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Реализация**

```python
"""Правила разговора с клиентом до работы.

Чистые функции: ни сети, ни базы. Здесь решается, когда спрашивать, как понять
ответ и когда звать владельца, — и всё это проверяется тестами.

Главное правило: **непонятое не гадаем.** Робот различает ровно «да» и «нет»,
потому что сам об этом и просит («Ответьте одним словом»). Всё остальное —
«перенесите на среду», «а можно позже» — уходит владельцу целиком: попытка
угадать смысл здесь стоит дороже, чем лишнее сообщение.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

ASK_BEFORE = timedelta(days=1)        # за сутки от времени заказа (решение владельца)
SILENCE_LIMIT = timedelta(hours=3)    # столько ждём ответа, потом зовём владельца

_YES = {"да", "да!", "да.", "ага", "верно", "подтверждаю"}
_NO = {"нет", "нет!", "нет.", "не", "отмена", "отменяю"}


@dataclass(frozen=True)
class ConfirmationPlan:
    send_confirmation: bool           # слать ли письмо «заказ принят»
    ask_at: Optional[datetime]        # когда задать вопрос; None — не спрашиваем
    reason: str = ""


def parse_answer(text: Optional[str]) -> str:
    """`yes`, `no` или `unclear`. Первое слово — его и просили прислать."""
    cleaned = (text or "").strip().lower().rstrip("!.,;")
    if not cleaned:
        return "unclear"
    first = cleaned.split()[0].rstrip("!.,;")
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
    """Пора ли звать владельца к молчащему клиенту. Зовём ровно один раз."""
    if asked_at is None or notified_at is not None:
        return False
    return now - asked_at >= SILENCE_LIMIT
```

**Step 4: Тесты зелёные**

Run: `.venv-wahelp/bin/python -m unittest tests.test_client_messaging`
Expected: PASS, 12 тестов

**Step 5: Коммит**

```bash
git add notifications/client_messaging.py tests/test_client_messaging.py
git commit -m "feat(messaging): правила подтверждения заказа"
```

---

## Задача 3: Тексты писем

**Files:**
- Modify: `docs/notification_rules.json`

**Step 1: Поправить `order_created`** — убрать мастера, которого на этапе оформления нет:

```
Здравствуйте!
Ваш заказ принят ✅

📅 Дата: {{date}}
🕒 Время: {{time}}
📍 Адрес: {{address}}

Мастер свяжется с вами перед приездом.
```

Переменные: `date`, `time`, `address` (было ещё `master_name`, `master_phone` — убрать).

**Step 2: Добавить `order_confirm_request`:**

```
Здравствуйте! Напоминаем о заказе:

📅 {{date}}, {{time}}
📍 {{address}}

Вы подтверждаете заказ? Да/Нет
Ответьте, пожалуйста, одним словом.
```

Получатель `client`, задержка 0 (время задаётся через `scheduled_at`).

**Step 3: Проверить, что файл читается**

Run: `.venv-wahelp/bin/python -c "from notifications.rules import load_notification_rules as l; r = l('docs/notification_rules.json'); print(r.get_event('order_confirm_request').template[:60])"`

**ВНИМАНИЕ:** на сервере лежит `docs/notification_rules.json.local` — проверить, какой файл читает прод (`NOTIFICATION_RULES_PATH`), и поправить тот же.

**Step 4: Коммит**

---

## Задача 4: Ловим оформленные заказы

**Files:**
- Modify: `bot.py` (рядом с `_amocrm_poll_exchange_once`)

**Step 1: Реализация**

Поллер `_amocrm_poll_confirmations_once(client, *, dry_run)` — устройство ровно как у обмена:

- свой курсор (поток `confirmation_events`), в репетиции — **в памяти процесса**
  (см. `_exchange_rehearsal_cursor`, ту же ошибку не повторять);
- события `lead_status_changed`, отсев по `value_after`: воронка
  `AMO_PIPELINE_REALIZATION`, статус `AMO_STAGE_ORDER_CREATED`;
- для отобранных: `fetch_lead` → дата заказа из поля 18701, адрес из 18639,
  контакт и телефон;
- ищем клиента в `clients` по цифрам телефона; **нет клиента — пропускаем
  и считаем отдельно**: писать некому, это работа автообмена;
- `plan_confirmation(...)` → если `send_confirmation`, ставим `order_created`
  в очередь немедленно; если есть `ask_at` — ставим `order_confirm_request`
  с `scheduled_at=ask_at` и пишем строку в `order_confirmations`.

**Step 2: Проверка на живом API (до включения)**

Убедиться, что события перехода в «Заказ оформлен» действительно приходят:
прогнать разовый скрипт, как это делалось для обмена (события за сутки →
сколько попадает в воронку реализации со статусом 41463832).

**Step 3: Коммит**

---

## Задача 5: Разбор ответа клиента

**Files:**
- Modify: `bot.py:handle_wahelp_inbound` (около строки 2817)

**Step 1: Реализация**

В начале разбора текста, до веток рейтинга и STOP, добавить проверку ожидания:

```python
    # Ждём ли мы от этого клиента подтверждения заказа. Проверяем раньше
    # рейтинга: «да» и «нет» с оценками не пересекаются, а вот порядок веток
    # определяет, кто первым заберёт сообщение.
    pending = await _confirmation_pending_for(digits)
    if pending is not None:
        answer = parse_answer(normalized_text)
        if answer == "yes":
            await _confirm_order(pending)      # двигаем сделку, владельцу молчим
            return True
        if answer in ("no", "unclear"):
            await _confirmation_call_owner(pending, answer, normalized_text)
            return True
```

`_confirm_order`: если сделка ещё в `AMO_STAGE_ORDER_CREATED` — перевести в
`AMO_STAGE_CONFIRMED`; если уже дальше — не трогать (решение владельца 7).
Запись помечается `confirmed`.

`_confirmation_call_owner`: сообщение владельцу с именем, полным телефоном,
датой заказа, ответом клиента и ссылкой на сделку. Запись помечается
`refused` или `owner_notified`.

**ВНИМАНИЕ:** телефон владельцу показывается целиком — его решение 2026-08-26.

**Step 2: Коммит**

---

## Задача 6: Сторож молчунов

**Files:**
- Modify: `bot.py`

Раз в 10 минут: строки со `status='asked'`, у которых `should_call_owner(...)`
истинно, → сообщение владельцу, `notified_at = NOW()`. Ровно один раз на запись.

Встроить в существующий цикл фоновых задач рядом с `retry_pending_sent_messages`.

---

## Задача 7: Выключатель, репетиция, отчёт

**Files:**
- Modify: `bot.py`, `.env.example`

```python
CLIENT_MESSAGING_ENABLED = _env_int("CLIENT_MESSAGING_ENABLED", 0) == 1
CLIENT_MESSAGING_DRY_RUN = _env_int("CLIENT_MESSAGING_DRY_RUN", 1) == 1
```

В репетиции: письма клиенту **не отправляются**, в журнал пишется, что ушло бы;
курсор — в памяти; сделки в CRM не двигаются; владельцу можно писать (это
и есть проверка). Строка в ежедневный отчёт: сколько подтверждений отправлено,
сколько вопросов, сколько «да», «нет», молчунов.

---

## Порядок включения

1. Автообмен переводится в боевой режим — **без него писать почти некому**
   (проверено 2026-08-28: трое из четырёх заказчиков дня в базе отсутствовали).
2. `CLIENT_MESSAGING_ENABLED=1`, `DRY_RUN=1` — сутки смотрим журнал.
3. Владелец проверяет: кому робот написал бы, что именно и когда.
4. `DRY_RUN=0` — первые сообщения уходят клиентам. Первый день смотреть каждое.

## Чего план намеренно НЕ делает

- Не трогает задачи в amoCRM.
- Не пишет клиентам, которых нет в базе бота (это работа автообмена).
- Не пытается понять ответ сложнее «да» / «нет».
- Не шлёт напоминание повторно и не пишет владельцу дважды об одном заказе.
