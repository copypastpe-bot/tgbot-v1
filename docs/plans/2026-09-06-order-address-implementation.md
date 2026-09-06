# План: адрес как свойство заказа

Дата: 2026-09-06. Статус: **согласовано с владельцем 2026-09-06**, к исполнению.
Основание: ТЗ `raketa-admin-bot/docs/plans/2026-09-06-order-address-handoff.md`
и два решения владельца (ниже). Затрагивает: проведение заказа мастером
(`bot.py` → `commit_order`), клиент amoCRM (`notifications/amocrm_api.py`),
новый модуль `notifications/order_address.py`, миграция `app/migrations/0008`.

## 1. Суть

Отчёт о проведённом заказе в чате подтверждений печатает адрес из карточки
клиента, а она устарела. Актуальный адрес владелец вносит в календарь, оттуда
админ-бот переносит его в поле «Адрес» (`18639`) открытой сделки воронки
реализации (`4482787`). Делаем адрес свойством **заказа**: в момент проведения
бот берёт адрес из этой сделки и сохраняет в `orders.address`; в отчёт и в
подпись кассы идёт он же. Если amoCRM недоступна или сделки нет — адрес из
карточки, как раньше. **Проведение заказа от amoCRM не зависит никогда.**

## 2. Решения владельца (2026-09-06)

1. `clients.last_order_addr` перезаписывается адресом проведённого заказа;
   `clients.address` не трогаем.
2. Подпись улицы в кассовом чате и в записи кассы берётся из адреса заказа.

Принято агентом (владелец уведомлён):
- у постоянного клиента может быть несколько открытых сделок реализации —
  берём ту, чья дата работы (`18701`) ближе всего к моменту проведения;
- ожидание amoCRM при проведении ограничено 8 секундами суммарно;
- заказы задним числом не переписываем; колонка заполняется только для новых.

## 3. Порядок источников адреса заказа

1. поле «Адрес» открытой сделки реализации в amoCRM;
2. `clients.address` (после upsert клиента — то есть с учётом адреса, который
   мастер ввёл вручную при пустой карточке);
3. `clients.last_order_addr`;
4. `None` — в отчёте строки адреса нет, как сейчас.

Пробелы обрезаются; пустая строка считается отсутствием.

## 4. Контракт: `notifications/order_address.py`

Импорты: `AMO_FIELD_ADDRESS`, `AMO_STATUS_WON`, `AMO_STATUS_LOST`, `field_values`
из `notifications.amo_exchange`; `AMO_PIPELINE_REALIZATION`,
`AMO_FIELD_ORDER_DATETIME` из `notifications.client_messaging`.
Логгер `logging.getLogger(__name__)`. Телефон в логах — только последние
четыре цифры (`…4567`). Модуль не импортирует `bot.py`.

```python
LOOKUP_TIMEOUT = 8.0   # секунд на весь поход в amoCRM при проведении заказа

def phone_query(phone: str | None) -> str | None:
    """Десять цифр для поиска контакта в amoCRM.
    '+79001234567' → '9001234567'; '89001234567' → '9001234567';
    '9001234567' → как есть; '8 (900) 123-45-67' → '9001234567'.
    Иное (None, пусто, не 10/11 цифр, 11 цифр не с 7/8) → None."""

def contact_matches_phone(contact: Mapping, phone10: str) -> bool:
    """Есть ли у контакта телефон (поле с field_code == 'PHONE', любое из
    значений), цифры которого оканчиваются на phone10. amoCRM ищет по
    подстроке во всех полях — чужой контакт отсеиваем здесь."""

def contact_lead_ids(contact: Mapping) -> list[int]:
    """id сделок из contact['_embedded']['leads'] (список {'id': ...});
    мусор пропускаем, порядок сохраняем."""

def pick_open_deal(deals: list[Mapping], *, now: datetime) -> Mapping | None:
    """Открытая сделка реализации, парная проводимому заказу.
    Берём только pipeline_id == AMO_PIPELINE_REALIZATION и
    status_id не в (AMO_STATUS_WON, AMO_STATUS_LOST).
    Порядок предпочтения:
      1) сделки с датой работы (18701, unix) — ближайшая к now по модулю;
         при равном расстоянии — прошедшая (дата <= now) раньше будущей;
      2) сделки без даты (или с испорченной датой) — после всех с датой,
         среди них самая свежая по created_at.
    Нет подходящих → None. `now` — aware datetime."""

def deal_address(deal: Mapping | None) -> str | None:
    """Первое непустое значение поля 18639, обрезанное. Нет → None."""

def resolve_order_address(*, deal_address: str | None,
                          card_address: str | None,
                          last_order_addr: str | None) -> str | None:
    """Порядок источников из раздела 3. Пробелы обрезаются, пустое → None."""

async def fetch_deal_address(client, phone: str | None, *, now: datetime,
                             timeout: float = LOOKUP_TIMEOUT) -> str | None:
    """Адрес открытой сделки реализации по телефону клиента. Никогда не
    бросает исключений: любая ошибка, таймаут, «не нашли» → None и
    предупреждение в лог (телефон замаскирован).
    Шаги (всё под asyncio.wait_for(timeout)):
      1) phone_query → None → сразу None, в amoCRM не ходим;
      2) contacts = await client.find_contacts_by_phone(phone10);
         оставляем только contact_matches_phone;
      3) ids = contact_lead_ids по всем подходящим контактам; пусто → None,
         fetch_leads_by_ids не вызываем;
      4) deals = await client.fetch_leads_by_ids(ids);
      5) deal_address(pick_open_deal(deals, now=now))."""
```

`client` — утиный тип: объект с `async find_contacts_by_phone(phone10)` и
`async fetch_leads_by_ids(ids)`. В тестах подменяется фейком.

## 5. Контракт: `notifications/amocrm_api.py`

- `AmoCRMAPIClient.get(path, *, params)` дополнительно принимает список пар
  `[(key, value), ...]` — для повторяющихся ключей вида `filter[id][]`.
  Список передаётся в `session.get(..., params=...)` как есть (aiohttp
  понимает список пар); Mapping — как раньше, через `dict(...)`.
- `async find_contacts_by_phone(self, phone10: str) -> list[dict]`:
  `GET /api/v4/contacts`, params `{"query": phone10, "with": "leads", "limit": 50}`,
  возвращает `_embedded.contacts` (нет → `[]`).
- `async fetch_leads_by_ids(self, lead_ids: Iterable[int]) -> list[dict]`:
  дубли отбрасываются, порядок сохраняется; пустой список → `[]` без запроса;
  запросы пакетами по 50: `GET /api/v4/leads` с params-списком
  `[("filter[id][]", id), ..., ("limit", 250)]`; результаты складываются.
- **Не использовать** `filter[contacts][id]` — amoCRM молча игнорирует его и
  отдаёт чужие сделки (проверено админ-ботом 2026-08-25). Существующий
  `fetch_contact_leads` не трогать: это отдельная задача.
- Новые имена добавить в `__all__`.

## 6. Контракт: `bot.py`

1. Импорт `fetch_deal_address`, `resolve_order_address` из
   `notifications.order_address` рядом с импортами `client_messaging`.
2. `async def ensure_orders_address_schema(conn)` рядом с
   `ensure_orders_wire_schema` (~строка 3317):
   `ALTER TABLE orders ADD COLUMN IF NOT EXISTS address text;`
   Вызов на старте сразу после `await ensure_orders_wire_schema(_conn)` (~14640).
3. `async def _order_address_from_amo(phone: str | None) -> str | None`
   (рядом с `commit_order` или с другими amo-обёртками): если не заданы
   `AMOCRM_API_BASE` и `AMOCRM_API_TOKEN` → None; иначе
   `async with AmoCRMAPIClient(AMOCRM_API_BASE, AMOCRM_API_TOKEN) as client:`
   `return await fetch_deal_address(client, phone, now=datetime.now(MOSCOW_TZ))`;
   любое исключение → `logging.warning` и None. Ходить в amoCRM **до**
   открытия транзакции БД, не внутри неё.
4. В `commit_order`:
   - перед `async with pool.acquire()`: `deal_address_val = await _order_address_from_amo(phone_in)`;
   - в RETURNING upsert-а клиента добавить `last_order_addr`;
   - переменную `client_address_val` переименовать в `order_address_val`
     (все вхождения внутри `commit_order`), значение:
     `resolve_order_address(deal_address=deal_address_val, card_address=client.get("address"), last_order_addr=client.get("last_order_addr"))`;
   - в `INSERT INTO orders` добавить колонку `address` (значение `order_address_val`);
   - после вставки заказа, если `order_address_val`:
     `UPDATE clients SET last_order_addr=$1, last_updated=NOW() WHERE id=$2 AND last_order_addr IS DISTINCT FROM $1`;
   - `street_label = extract_street(order_address_val)` (подпись кассы — решение 2);
   - строка отчёта `📍 Адрес:` печатает `order_address_val`.
5. Больше нигде адрес не менять: карточка клиента, поиск, превью «Проверьте»
   перед подтверждением остаются как есть.

## 7. Миграция

`app/migrations/0008_orders_address.sql`:
```sql
-- Адрес — свойство заказа, а не клиента: куда фактически ездил мастер.
-- Заполняется при проведении заказа (сделка amoCRM → карточка клиента),
-- дальше не меняется. Старые заказы не заполняются.
ALTER TABLE orders ADD COLUMN IF NOT EXISTS address text;
```
Колонка появляется и через `ensure_orders_address_schema()` на старте бота
(гибридная схема проекта) — ручного захода в базу при выкатке не нужно.

## 8. Тесты

Стиль проекта: `unittest`, докстринги по-русски объясняют бизнес-правило,
без pytest-фикстур. Запуск: `.venv-wahelp/bin/python -m pytest tests/ -q`.

`tests/test_order_address.py`:
- `phone_query`: `+7…`, `8…`, `9…` (10 цифр), формат с пробелами/скобками;
  None/пусто/7 цифр/11 цифр не с 7 или 8 → None.
- `contact_matches_phone`: совпадение по окончанию; чужой номер → False;
  контакт без телефона → False; несколько значений PHONE — достаточно одного.
- `contact_lead_ids`: обычный случай, пустой `_embedded`, мусор в списке.
- `pick_open_deal`: чужая воронка пропускается; 142 и 143 пропускаются;
  из двух открытых берётся ближайшая по дате к now (старая «ждёт оплаты»
  против сегодняшней); при равном расстоянии — прошедшая; сделка без даты
  проигрывает сделке с датой; из двух без даты — свежая по created_at;
  испорченная дата (`"abc"`) не роняет выбор; пусто → None.
- `deal_address`: значение с пробелами обрезается; пустое поле → None;
  None → None.
- `resolve_order_address`: порядок 1→2→3→None; пробельная строка = пусто.
- `fetch_deal_address` (`IsolatedAsyncioTestCase`, фейковый клиент с
  двумя async-методами и журналом вызовов): счастливый путь; телефон
  не разбирается → None и ни одного вызова; контакт с чужим номером →
  None и `fetch_leads_by_ids` не вызван; контакты без сделок → то же;
  `find_contacts_by_phone` бросает → None, исключение наружу не выходит;
  таймаут (фейк спит 5 с, `timeout=0.05`) → None быстро.

`tests/test_amocrm_api.py` (дописать, фейки `FakeHTTPResponse`,
`SequenceHTTPSession` уже есть):
- `find_contacts_by_phone`: URL `/api/v4/contacts`, params `query`, `with=leads`;
  ответ без `_embedded` → `[]`.
- `fetch_leads_by_ids`: пустой список → `[]` и ни одного запроса; дубли
  схлопываются; 51 id → два запроса; params — список пар с `filter[id][]`.
- `get` со списком пар передаёт его в сессию как список.

## 9. Проверка в бою (после выкатки)

1. Клиент, у которого адрес в календаре отличается от карточки. Провести
   заказ мастером. Ожидание: в чате подтверждений адрес из календаря;
   `orders.address` = он же; `clients.last_order_addr` = он же;
   `clients.address` не изменился; сделка в amoCRM не изменилась.
2. Клиент без сделки в amoCRM: заказ проводится, адрес из карточки,
   поведение как раньше.
3. Хвост для `raketa-admin-bot` (отдельная сессия): `adminbot/db.py:43`
   читать `o.address` первым источником.
