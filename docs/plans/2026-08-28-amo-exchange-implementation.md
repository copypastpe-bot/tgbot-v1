# Автообмен amoCRM → база бота: план реализации

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Робот сам, без ручной выгрузки CSV, пополняет базу лидов и дописывает клиентам адрес, услугу и имя, реагируя на закрытие сделок первичной воронки amoCRM.

**Architecture:** Правила обмена и разбор данных амо — чистые функции в новом модуле `notifications/amo_exchange.py` (без сети и без базы, покрыты тестами). Тонкий слой — опрос событий амо и запись в `clients` — живёт в `bot.py` по образцу существующего `_amocrm_poll_new_leads_once`: закладка в `amocrm_api_state`, дедупликация событий через `amocrm_api_events`.

**Tech Stack:** Python 3.11, aiohttp, asyncpg, unittest. Тесты: `python -m unittest tests.<module>` из `.venv-wahelp`.

**Дизайн:** `docs/plans/2026-08-28-amo-exchange-design.md` — решения владельца от 2026-08-28.

---

## Что уже есть и переиспользуется

| Что | Где | Зачем |
|---|---|---|
| `AmoCRMAPIClient.fetch_events` | `notifications/amocrm_api.py:110` | опрос событий амо по типу и времени |
| `AmoCRMAPIClient.fetch_lead` / `fetch_contact` | `notifications/amocrm_api.py:128` | детали сделки и контакта |
| `extract_contact_phone` | `notifications/amocrm_api.py` | телефон из custom_fields контакта |
| `_amocrm_get_cursor` / `_amocrm_set_cursor` | `bot.py:1049` / `bot.py:1068` | закладка по потоку событий |
| `_amocrm_poll_new_leads_once` | `bot.py:1541` | образец поллера: курсор → события → дедуп → обработка → сдвиг курсора |
| `_amocrm_fetch_first_contact` | `bot.py:1510` | первый контакт сделки |
| `_amo_normalize_phone` | `bot.py:4182` | нормализация телефона к `+7XXXXXXXXXX` + цифры |
| `_clients_name_column` | `bot.py` | в проде колонка имени бывает `full_name` или `name` |

## Жёсткие ограничения базы (`docs/db_production_contract.md`)

- `clients.phone_digits` — GENERATED, **писать нельзя** (`GeneratedAlwaysError`).
- `clients.status` — CHECK, только `'lead'` или `'client'`.
- `clients.phone` — UNIQUE, формат `+7XXXXXXXXXX`.
- Имя пишется в колонку из `_clients_name_column(conn)`, не хардкодом.
- При любом изменении обновлять `last_updated`.

## Константы amoCRM

```python
AMO_PIPELINE_PRIMARY = 4482751   # Воронка первичной обработки
AMO_STATUS_WON = 142             # «Передано в работу» — успех первичной воронки
AMO_STATUS_LOST = 143            # «Закрыто и не реализовано»
AMO_FIELD_ADDRESS = 18639        # Адрес (сделка)
AMO_FIELD_SERVICE = 271915       # Услуга (сделка, multiselect)
```

---

## Задача 1: Правила обмена — чистые функции

**Files:**
- Create: `notifications/amo_exchange.py`
- Test: `tests/test_amo_exchange.py`

**Step 1: Написать падающий тест**

```python
import unittest

from notifications.amo_exchange import (
    ExistingClient,
    IncomingClient,
    decide_exchange,
)


class DecideExchangeTests(unittest.TestCase):
    def _incoming(self, **kwargs):
        base = dict(phone="+79001234567", digits="79001234567",
                    name="Дарья", address="Панина 7к2", service="Чистка мебели")
        base.update(kwargs)
        return IncomingClient(**base)

    def test_lost_deal_creates_lead_when_unknown(self):
        decision = decide_exchange(outcome="lost", incoming=self._incoming(),
                                   existing=None)
        self.assertEqual(decision.action, "create_lead")
        self.assertEqual(decision.fields["status"], "lead")

    def test_lost_deal_never_touches_existing_client(self):
        """Постоянный клиент узнал цену и не заказал — карточку не трогаем."""
        existing = ExistingClient(client_id=7, status="client", name="Дарья",
                                  address=None, service=None)
        decision = decide_exchange(outcome="lost", incoming=self._incoming(),
                                   existing=existing)
        self.assertEqual(decision.action, "skip")

    def test_lost_deal_never_touches_existing_lead(self):
        existing = ExistingClient(client_id=7, status="lead", name=None,
                                  address=None, service=None)
        decision = decide_exchange(outcome="lost", incoming=self._incoming(),
                                   existing=existing)
        self.assertEqual(decision.action, "skip")

    def test_won_deal_creates_client_when_unknown(self):
        decision = decide_exchange(outcome="won", incoming=self._incoming(),
                                   existing=None)
        self.assertEqual(decision.action, "create_client")
        self.assertEqual(decision.fields["status"], "client")
        self.assertEqual(decision.fields["last_order_addr"], "Панина 7к2")
        self.assertEqual(decision.fields["last_service"], "Чистка мебели")

    def test_won_deal_promotes_lead_to_client(self):
        existing = ExistingClient(client_id=7, status="lead", name=None,
                                  address=None, service=None)
        decision = decide_exchange(outcome="won", incoming=self._incoming(),
                                   existing=existing)
        self.assertEqual(decision.action, "update")
        self.assertEqual(decision.fields["status"], "client")

    def test_client_is_never_demoted(self):
        """Понижения статуса не бывает — статус в правках не появляется."""
        existing = ExistingClient(client_id=7, status="client", name="Дарья",
                                  address="Старый адрес", service="Уборка")
        decision = decide_exchange(outcome="won", incoming=self._incoming(),
                                   existing=existing)
        self.assertNotIn("status", decision.fields)

    def test_name_filled_only_when_empty(self):
        existing = ExistingClient(client_id=7, status="client", name="Своё имя",
                                  address=None, service=None)
        decision = decide_exchange(outcome="won", incoming=self._incoming(),
                                   existing=existing)
        self.assertNotIn("full_name", decision.fields)

    def test_address_filled_only_when_empty(self):
        """Адрес заказа дописываем, чужой не переписываем (правила обмена)."""
        existing = ExistingClient(client_id=7, status="client", name="Дарья",
                                  address="Старый адрес", service=None)
        decision = decide_exchange(outcome="won", incoming=self._incoming(),
                                   existing=existing)
        self.assertNotIn("last_order_addr", decision.fields)

    def test_service_is_always_refreshed(self):
        existing = ExistingClient(client_id=7, status="client", name="Дарья",
                                  address="Панина 7к2", service="Уборка")
        decision = decide_exchange(outcome="won", incoming=self._incoming(),
                                   existing=existing)
        self.assertEqual(decision.fields["last_service"], "Чистка мебели")

    def test_nothing_to_change_is_skipped(self):
        existing = ExistingClient(client_id=7, status="client", name="Дарья",
                                  address="Панина 7к2", service="Чистка мебели")
        decision = decide_exchange(outcome="won", incoming=self._incoming(),
                                   existing=existing)
        self.assertEqual(decision.action, "skip")

    def test_deal_without_phone_is_skipped(self):
        decision = decide_exchange(outcome="won",
                                   incoming=self._incoming(phone=None, digits=None),
                                   existing=None)
        self.assertEqual(decision.action, "skip")
        self.assertIn("телефон", decision.reason)

    def test_bonuses_and_birthday_are_never_written(self):
        """Бонусы и день рождения ведёт бот — обмен их не трогает никогда."""
        decision = decide_exchange(outcome="won", incoming=self._incoming(),
                                   existing=None)
        self.assertNotIn("bonus_balance", decision.fields)
        self.assertNotIn("birthday", decision.fields)


if __name__ == "__main__":
    unittest.main()
```

**Step 2: Запустить тест и убедиться, что падает**

Run: `.venv-wahelp/bin/python -m unittest tests.test_amo_exchange`
Expected: FAIL — `ModuleNotFoundError: No module named 'notifications.amo_exchange'`

**Step 3: Написать минимальную реализацию**

```python
"""Правила автообмена amoCRM → база бота.

Чистые функции: ни сети, ни базы. Решение о том, что делать с записью клиента,
принимается здесь и проверяется тестами; запись в базу — отдельный тонкий слой.

Два защитных правила, ради которых всё и написано (решения владельца 2026-08-28):

1. **Понижения статуса не бывает.** Кто уже клиент — остаётся клиентом.
   Автомат, честно применяющий «отказ → лид», за месяц тихо разжалует
   половину постоянных клиентов.
2. **Отказная сделка существующей записи не меняет ничего.** Человек узнал
   цену и не заказал — менять в его карточке нечего.

Бонусы и день рождения обмен не пишет никогда: они рождаются в боте, и запись
из CRM затирала бы свежее старым.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class IncomingClient:
    """Что пришло из amoCRM по закрывшейся сделке."""

    phone: Optional[str]        # +7XXXXXXXXXX
    digits: Optional[str]       # только цифры, ключ сопоставления
    name: Optional[str] = None
    address: Optional[str] = None
    service: Optional[str] = None


@dataclass(frozen=True)
class ExistingClient:
    """Что уже лежит в базе бота по этому телефону."""

    client_id: int
    status: str                 # lead | client
    name: Optional[str] = None
    address: Optional[str] = None
    service: Optional[str] = None


@dataclass(frozen=True)
class ExchangeDecision:
    """Что робот сделает с записью."""

    action: str                 # create_lead | create_client | update | skip
    fields: dict[str, Any] = field(default_factory=dict)
    reason: str = ""


def decide_exchange(*, outcome: str, incoming: IncomingClient,
                    existing: Optional[ExistingClient]) -> ExchangeDecision:
    """Решить, что делать с записью клиента. `outcome` — won | lost."""
    if not incoming.digits:
        return ExchangeDecision("skip", reason="в сделке нет телефона")

    if outcome == "lost":
        if existing is not None:
            return ExchangeDecision(
                "skip", reason="отказ по известной записи — не трогаем")
        return ExchangeDecision("create_lead", fields={
            "status": "lead",
            "phone": incoming.phone,
            "full_name": incoming.name,
        }, reason="новый лид из отказной сделки")

    if existing is None:
        return ExchangeDecision("create_client", fields={
            "status": "client",
            "phone": incoming.phone,
            "full_name": incoming.name,
            "last_order_addr": incoming.address,
            "last_service": incoming.service,
        }, reason="новый клиент из успешной сделки")

    updates: dict[str, Any] = {}
    if existing.status != "client":
        updates["status"] = "client"          # только вверх, вниз — никогда
    if incoming.name and not existing.name:
        updates["full_name"] = incoming.name
    if incoming.address and not existing.address:
        updates["last_order_addr"] = incoming.address
    if incoming.service and incoming.service != existing.service:
        updates["last_service"] = incoming.service

    if not updates:
        return ExchangeDecision("skip", reason="всё уже заполнено")
    return ExchangeDecision("update", fields=updates, reason="дописал недостающее")


__all__ = ["ExchangeDecision", "ExistingClient", "IncomingClient", "decide_exchange"]
```

**Step 4: Запустить тесты и убедиться, что проходят**

Run: `.venv-wahelp/bin/python -m unittest tests.test_amo_exchange`
Expected: PASS, 12 тестов

**Step 5: Коммит**

```bash
git add notifications/amo_exchange.py tests/test_amo_exchange.py
git commit -m "feat(exchange): правила автообмена amoCRM -> база бота"
```

---

## Задача 2: Разбор сделки и контакта amoCRM

**Files:**
- Modify: `notifications/amo_exchange.py`
- Modify: `tests/test_amo_exchange.py`

**Step 1: Написать падающий тест**

```python
from notifications.amo_exchange import incoming_from_amo, outcome_of_lead


class ParseAmoTests(unittest.TestCase):
    LEAD = {
        "id": 31570885,
        "pipeline_id": 4482751,
        "status_id": 142,
        "custom_fields_values": [
            {"field_id": 18639, "values": [{"value": "Панина 7к2"}]},
            {"field_id": 271915, "values": [{"value": "Чистка мебели"}]},
        ],
    }
    CONTACT = {
        "id": 55,
        "name": "Дарья",
        "custom_fields_values": [
            {"field_code": "PHONE", "values": [{"value": "8 (900) 123-45-67"}]},
        ],
    }

    def test_reads_phone_name_address_and_service(self):
        incoming = incoming_from_amo(self.LEAD, self.CONTACT)
        self.assertEqual(incoming.digits, "79001234567")
        self.assertEqual(incoming.phone, "+79001234567")
        self.assertEqual(incoming.name, "Дарья")
        self.assertEqual(incoming.address, "Панина 7к2")
        self.assertEqual(incoming.service, "Чистка мебели")

    def test_multiselect_services_are_joined(self):
        lead = {**self.LEAD, "custom_fields_values": [
            {"field_id": 271915, "values": [{"value": "Чистка мебели"},
                                            {"value": "Мойка окон"}]},
        ]}
        self.assertEqual(incoming_from_amo(lead, self.CONTACT).service,
                         "Чистка мебели, Мойка окон")

    def test_contact_without_phone_gives_empty_digits(self):
        incoming = incoming_from_amo(self.LEAD, {"id": 55, "name": "Дарья"})
        self.assertIsNone(incoming.digits)

    def test_outcome_won_and_lost(self):
        self.assertEqual(outcome_of_lead(self.LEAD), "won")
        self.assertEqual(outcome_of_lead({**self.LEAD, "status_id": 143}), "lost")

    def test_other_pipeline_is_not_our_business(self):
        """Воронка реализации и ковры обменом не занимаются."""
        self.assertIsNone(outcome_of_lead({**self.LEAD, "pipeline_id": 7108250}))

    def test_intermediate_status_is_not_an_outcome(self):
        self.assertIsNone(outcome_of_lead({**self.LEAD, "status_id": 41463535}))
```

**Step 2: Запустить и убедиться, что падает**

Run: `.venv-wahelp/bin/python -m unittest tests.test_amo_exchange`
Expected: FAIL — `ImportError: cannot import name 'incoming_from_amo'`

**Step 3: Реализация**

Добавить в `notifications/amo_exchange.py`:

```python
AMO_PIPELINE_PRIMARY = 4482751       # Воронка первичной обработки
AMO_STATUS_WON = 142                 # «Передано в работу»
AMO_STATUS_LOST = 143                # «Закрыто и не реализовано»
AMO_FIELD_ADDRESS = 18639
AMO_FIELD_SERVICE = 271915


def outcome_of_lead(lead: dict) -> Optional[str]:
    """won | lost | None. None — сделка не наша или ещё в работе."""
    if int(lead.get("pipeline_id") or 0) != AMO_PIPELINE_PRIMARY:
        return None
    status_id = int(lead.get("status_id") or 0)
    if status_id == AMO_STATUS_WON:
        return "won"
    if status_id == AMO_STATUS_LOST:
        return "lost"
    return None


def _field_values(entity: dict, field_id: int) -> list[str]:
    for field_data in entity.get("custom_fields_values") or []:
        if int(field_data.get("field_id") or 0) != field_id:
            continue
        return [str(v.get("value")) for v in field_data.get("values") or []
                if v.get("value")]
    return []


def _contact_phone(contact: dict) -> Optional[str]:
    for field_data in (contact or {}).get("custom_fields_values") or []:
        if field_data.get("field_code") != "PHONE":
            continue
        for value in field_data.get("values") or []:
            if value.get("value"):
                return str(value["value"])
    return None


def incoming_from_amo(lead: dict, contact: Optional[dict]) -> IncomingClient:
    """Собрать данные клиента из сделки и её контакта."""
    from bot import _amo_normalize_phone   # нормализация одна на весь проект

    phone, digits = _amo_normalize_phone(_contact_phone(contact or {}) or "")
    address = next(iter(_field_values(lead, AMO_FIELD_ADDRESS)), None)
    services = _field_values(lead, AMO_FIELD_SERVICE)
    return IncomingClient(
        phone=phone, digits=digits,
        name=(contact or {}).get("name") or None,
        address=address,
        service=", ".join(services) if services else None,
    )
```

**ВНИМАНИЕ:** импорт из `bot.py` внутри функции — временное решение, чтобы не тянуть весь `bot.py` в тесты. Если тест упадёт на импорте (bot.py требует переменных окружения), перенести `_amo_normalize_phone` в `notifications/amo_exchange.py` и вызвать её из `bot.py` — тогда общий код живёт в модуле без побочных эффектов, а `bot.py` его импортирует.

**Step 4: Запустить тесты**

Run: `.venv-wahelp/bin/python -m unittest tests.test_amo_exchange`
Expected: PASS, 18 тестов

**Step 5: Коммит**

```bash
git add notifications/amo_exchange.py tests/test_amo_exchange.py
git commit -m "feat(exchange): разбор сделки и контакта amoCRM"
```

---

## Задача 3: Чтение существующей записи и применение решения

**Files:**
- Modify: `bot.py` (рядом с `process_amocrm_csv`, около строки 4334)

**Step 1: Реализация чтения**

```python
async def _exchange_find_existing(conn: asyncpg.Connection,
                                  digits: str) -> ExistingClient | None:
    """Найти запись по цифрам телефона. Сопоставление одно на весь обмен —
    поэтому второй записи на того же человека не появляется."""
    name_col = await _clients_name_column(conn)
    row = await conn.fetchrow(
        f"""
        SELECT id, status, {name_col} AS client_name, last_order_addr, last_service
        FROM clients
        WHERE phone_digits = $1
           OR regexp_replace(COALESCE(phone, ''), '[^0-9]+', '', 'g') = $1
        LIMIT 1
        """,
        digits,
    )
    if row is None:
        return None
    return ExistingClient(
        client_id=int(row["id"]),
        status=str(row["status"]),
        name=row["client_name"],
        address=row["last_order_addr"],
        service=row["last_service"],
    )
```

**Step 2: Реализация записи**

```python
async def _exchange_apply(conn: asyncpg.Connection, decision: ExchangeDecision,
                          existing: ExistingClient | None) -> str:
    """Применить решение. Возвращает, что произошло, — для отчёта владельцу.

    phone_digits не пишем никогда: колонка GENERATED (см. docs/db_production_contract.md).
    """
    if decision.action == "skip":
        return "skip"

    fields = {k: v for k, v in decision.fields.items() if v is not None}
    name_col = await _clients_name_column(conn)
    if "full_name" in fields and name_col != "full_name":
        fields[name_col] = fields.pop("full_name")

    if decision.action in ("create_lead", "create_client"):
        columns = list(fields)
        placeholders = ", ".join(f"${i + 1}" for i in range(len(columns)))
        await conn.execute(
            f"INSERT INTO clients ({', '.join(columns)}, created_at, last_updated) "
            f"VALUES ({placeholders}, NOW(), NOW()) "
            f"ON CONFLICT (phone) DO NOTHING",
            *[fields[c] for c in columns],
        )
        return decision.action

    assignments = ", ".join(f"{name} = ${i + 1}" for i, name in enumerate(fields))
    values = list(fields.values())
    values.append(existing.client_id)
    await conn.execute(
        f"UPDATE clients SET {assignments}, last_updated = NOW() "
        f"WHERE id = ${len(values)}",
        *values,
    )
    return "update"
```

**Step 3: Проверка на живой базе — НЕ на проде**

Тестов с базой в проекте нет, поэтому первая проверка — репетиция (задача 5).
До неё убедиться глазами:
- `status` принимает только `lead` / `client`;
- `phone_digits` в списках полей отсутствует;
- имя пишется в колонку из `_clients_name_column`.

**Step 4: Коммит**

```bash
git add bot.py
git commit -m "feat(exchange): чтение записи клиента и применение решения"
```

---

## Задача 4: Опрос событий amoCRM

**Files:**
- Modify: `bot.py` (рядом с `_amocrm_poll_new_leads_once`, строка 1541)

**Step 1: Реализация поллера**

Полностью по образцу `_amocrm_poll_new_leads_once`: своя закладка (поток
`exchange_events`), дедупликация через `amocrm_api_events`, курсор сдвигается
в самом конце — сбой означает «повторим», а не «пропустим».

```python
async def _amocrm_poll_exchange_once(client: AmoCRMAPIClient) -> dict[str, int]:
    """Один проход обмена: закрывшиеся сделки первичной воронки → база бота."""
    if pool is None:
        return {}

    counters: dict[str, int] = {}
    async with pool.acquire() as conn:
        cursor = await _amocrm_get_cursor(conn, "exchange_events",
                                          _amocrm_default_cursor())

    events = await client.fetch_events(
        event_types=["lead_status_changed"], created_from=cursor)
    max_created_at = cursor

    for event in events:
        event_id = str(event.get("id") or "")
        if not event_id:
            continue
        created_at = _amocrm_created_at(event, cursor)
        max_created_at = max(max_created_at, created_at)
        lead_id = extract_event_entity_id(event)
        if not lead_id:
            continue

        async with pool.acquire() as conn:
            inserted = await conn.fetchval(
                """
                INSERT INTO amocrm_api_events (
                    event_id, event_type, entity_id, payload, action, created_at
                )
                VALUES ($1, 'exchange_status', $2, $3::jsonb, 'pending', $4)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id
                """,
                f"exchange:{event_id}", str(lead_id),
                _amocrm_payload_json(event), created_at,
            )
        if not inserted:
            continue                      # это событие уже разбирали

        try:
            lead = await client.fetch_lead(lead_id)
            outcome = outcome_of_lead(lead)
            if outcome is None:
                counters["не наша сделка"] = counters.get("не наша сделка", 0) + 1
                continue

            contact = await _amocrm_fetch_first_contact(
                client, normalize_lead(lead).contact_ids)
            incoming = incoming_from_amo(lead, contact)

            async with pool.acquire() as conn:
                existing = (await _exchange_find_existing(conn, incoming.digits)
                            if incoming.digits else None)
                decision = decide_exchange(outcome=outcome, incoming=incoming,
                                           existing=existing)
                if AMOCRM_EXCHANGE_DRY_RUN:
                    result = f"репетиция: {decision.action}"
                else:
                    result = await _exchange_apply(conn, decision, existing)
            counters[result] = counters.get(result, 0) + 1
        except Exception as exc:          # noqa: BLE001 — одна сделка не роняет проход
            logger.exception("Обмен: сделка %s не разобрана: %s", lead_id, exc)
            counters["ошибка"] = counters.get("ошибка", 0) + 1

    async with pool.acquire() as conn:
        await _amocrm_set_cursor(conn, "exchange_events", max_created_at)
    return counters
```

**Step 2: Проверка типа события на живом API**

Тип события `lead_status_changed` взят из документации amoCRM v4 и **не проверен
на живом аккаунте**. Перед включением выполнить на VPS разовый запрос и убедиться,
что события такого типа приходят:

```bash
ssh admin@91.200.150.68
cd /opt/telegram-bot && .venv/bin/python -c "
import asyncio, os
from notifications.amocrm_api import AmoCRMAPIClient
async def main():
    async with AmoCRMAPIClient(os.environ['AMOCRM_API_BASE'], os.environ['AMOCRM_API_TOKEN']) as c:
        events = await c.fetch_events(event_types=['lead_status_changed'],
                                      created_from=0, limit=5)
        print(len(events), [e.get('type') for e in events])
asyncio.run(main())
"
```

Если тип называется иначе — поправить константу в поллере, тесты не затрагиваются.

**Step 3: Коммит**

```bash
git add bot.py
git commit -m "feat(exchange): опрос закрывшихся сделок amoCRM"
```

---

## Задача 5: Выключатель, репетиция и отчёт владельцу

**Files:**
- Modify: `bot.py` (константы окружения около строки 217, цикл поллинга около 1920)
- Modify: `.env.example`

**Step 1: Переменные окружения**

```python
AMOCRM_EXCHANGE_ENABLED = _env_bool("AMOCRM_EXCHANGE_ENABLED", False)
AMOCRM_EXCHANGE_DRY_RUN = _env_bool("AMOCRM_EXCHANGE_DRY_RUN", True)
```

Умолчания намеренно такие: функция выключена, а если включат — сперва репетиция.
Тот же порядок, что у календаря в админ-боте.

**Step 2: Встроить в существующий цикл**

В `amocrm_api_polling_loop` (около `bot.py:1920`), рядом с вызовом
`_amocrm_poll_new_leads_once`, добавить:

```python
if AMOCRM_EXCHANGE_ENABLED:
    counters = await _amocrm_poll_exchange_once(client)
    if counters:
        logger.info("Обмен amoCRM: %s", counters)
```

**Step 3: Отчёт владельцу**

Накопленные за сутки счётчики добавить строкой в существующий ежедневный отчёт
(`daily_reports`, около `bot.py:4960`): сколько лидов заведено, сколько клиентов,
сколько записей дописано, сколько пропущено без телефона, сколько ошибок.

**Step 4: Коммит**

```bash
git add bot.py .env.example
git commit -m "feat(exchange): выключатель, режим репетиции и отчёт"
```

---

## Задача 6: Включение на сервере

**Порядок — тот же, что был с календарём:**

1. Выкатить код, `AMOCRM_EXCHANGE_ENABLED=1`, `AMOCRM_EXCHANGE_DRY_RUN=1`.
2. Сутки посмотреть журнал: какие решения робот принимал бы и по каким сделкам.
   Владелец сверяет несколько случаев с CRM глазами.
3. Убедиться отдельно, что в репетиции **не было** ни одной записи в базу.
4. `AMOCRM_EXCHANGE_DRY_RUN=0` — боевой режим.
5. Первые сутки проверять: не появилось ли дублей записей на один телефон,
   не понизился ли кто-то из клиентов до лида.

**Проверка дублей после включения:**

```sql
SELECT phone_digits, count(*)
FROM clients
GROUP BY phone_digits
HAVING count(*) > 1;
```

Ожидание: пусто.

---

## Чего этот план намеренно НЕ делает

- Не переносит историю: в работу идут только сделки, закрывшиеся после включения.
- Не трогает бонусы, день рождения, район и дату последнего заказа.
- Не удаляет ручную загрузку CSV — она остаётся запасным путём.
- Не создаёт и не закрывает задачи в amoCRM.

## Открытый вопрос к владельцу

В правилах из файла «правила обмена» адрес заказа заполнялся, только если в базе
пусто, а услуга обновлялась всегда. В плане так и сделано. Если нужно, чтобы
адрес тоже обновлялся всегда, — это правка одной строки в `decide_exchange`.
