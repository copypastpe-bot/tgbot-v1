# Клининг-бригадир и сценарий проведения уборки — дизайн

Date: 2026-05-21
Status: approved, ready for implementation plan

## Цель

Ввести параллельный финансовый контур для уборок:
- сущность «клининг-бригадир» (пользователь бота, проводит уборки);
- отдельная касса клининга (полная изоляция в БД);
- отдельные DIV-выплаты для партнёров клининга;
- сценарий проведения уборки в боте.

Химчистка не трогается. Клиенты, бонусы и notification_outbox остаются общими.

## Архитектурный подход

Вариант B из брейншторма: **параллель только в финансовом контуре + re-use бизнес-уровня**.

Новые сущности в БД:
- `cleaning_foremen` — бригадиры (tg-доступ);
- `cleaning_orders` — заказы уборки;
- `cleaning_order_payments` — мульти-оплата;
- `cleaning_cashbook` — отдельная касса с типизированным `kind`.

Общие сущности (без изменений):
- `clients`, бонусы, `notification_outbox`, Wahelp-роутинг.

DIV-партнёры **не моделируются** отдельной таблицей: получатель указывается вручную в комментарии (как сейчас в химчистке). Источник средств у клининга один, выбора нет.

## 1. Схема БД

```sql
CREATE TABLE cleaning_foremen (
    id         serial PRIMARY KEY,
    tg_user_id bigint UNIQUE,
    fn         text NOT NULL,
    ln         text,
    phone      text,
    is_active  boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE cleaning_orders (
    id             serial PRIMARY KEY,
    client_id      integer NOT NULL REFERENCES clients(id),
    foreman_id     integer NOT NULL REFERENCES cleaning_foremen(id),
    address        text NOT NULL,
    total_amount   numeric(12,2) NOT NULL,
    bonuses_used   numeric(12,2) NOT NULL DEFAULT 0,
    bonuses_earned numeric(12,2) NOT NULL DEFAULT 0,
    client_op_id   uuid UNIQUE,
    happened_at    timestamptz NOT NULL DEFAULT now(),
    created_at     timestamptz NOT NULL DEFAULT now(),
    deleted_at     timestamptz
);
CREATE INDEX ix_cleaning_orders_client   ON cleaning_orders(client_id);
CREATE INDEX ix_cleaning_orders_happened ON cleaning_orders(happened_at);

CREATE TABLE cleaning_order_payments (
    id         serial PRIMARY KEY,
    order_id   integer NOT NULL REFERENCES cleaning_orders(id) ON DELETE CASCADE,
    method     text NOT NULL,
    amount     numeric(12,2) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_cleaning_op_order ON cleaning_order_payments(order_id);

CREATE TABLE cleaning_cashbook (
    id          serial PRIMARY KEY,
    kind        text NOT NULL,    -- 'income'|'expense'|'dividend'|'withdrawal'|'deposit'
    method      text NOT NULL,    -- для income: канонические методы; для остальных: категория
    amount      numeric(12,2) NOT NULL,
    comment     text,
    order_id    integer REFERENCES cleaning_orders(id),
    happened_at timestamptz NOT NULL DEFAULT now(),
    created_at  timestamptz NOT NULL DEFAULT now(),
    deleted_at  timestamptz
);
CREATE INDEX ix_cleaning_cb_kind_method ON cleaning_cashbook(kind, method);
CREATE INDEX ix_cleaning_cb_order       ON cleaning_cashbook(order_id);
CREATE INDEX ix_cleaning_cb_happened    ON cleaning_cashbook(happened_at);
```

### Правило: сертификат не идёт в кассу

```text
для каждой строки cleaning_order_payments:
  if method == 'Подарочный сертификат':
      → НЕ создаём cleaning_cashbook row (повторяем контракт химчистки)
  else:
      → INSERT cleaning_cashbook(kind='income', method, amount, order_id)
```

## 2. FSM проведения заказа уборки

Команда: `/cleaning_order`. Доступ — только активным `cleaning_foremen`.

Шаги:
1. Телефон клиента → поиск/создание в общей `clients`.
2. Адрес уборки.
3. Сумма чека.
4. Бонусы клиента (показ баланса, опциональное списание; правила те же, что в химчистке).
5. Способ оплаты (мульти): `Наличные`, `Карта`, `Расчётный`, `Подарочный сертификат`. Сумма частей = (сумма чека − списанные бонусы).
6. Расходы (несколько строк): `Химия`, `ЗП клинеров`, `ГСМ`, `Прочее`.
7. Подтверждение → транзакция.

Транзакция (одной BEGIN/COMMIT):
```text
INSERT cleaning_orders(...)
для каждой части оплаты:
  INSERT cleaning_order_payments(order_id, method, amount)
  если method != 'Подарочный сертификат':
    INSERT cleaning_cashbook(kind='income', method, amount, order_id, comment='Заказ #N')
для каждого расхода:
  INSERT cleaning_cashbook(kind='expense', method=<категория>, amount, order_id, comment=<кат>)
обновить общие bonuses клиента (списать/начислить)
INSERT notification_outbox: 'cleaning_order_completed_summary', 'cleaning_order_rating_reminder'
COMMIT
после COMMIT: send_cleaning_money_flow(<сводка по заказу>)
```

Идемпотентность: `cleaning_orders.client_op_id` (UUID, формируется в начале FSM, UNIQUE).

## 3. DIV-выплата

Команда: `/cleaning_dividend`. Доступ — админ.

FSM:
1. Сумма.
2. Комментарий (кому выдали — свободный текст, обязателен).
3. Подтверждение.

Запись:
```sql
INSERT INTO cleaning_cashbook(kind, method, amount, comment, happened_at)
VALUES ('dividend', 'Касса клининга', :amount, :comment, now());
```

После COMMIT — `send_cleaning_money_flow(<сводка по DIV>)`.

DIV уменьшает баланс кассы, но **не** входит в P&L.

## 4. Оповещения в чат

Env: `CLEANING_MONEY_FLOW_CHAT_ID=3687084157`.

Хелпер `send_cleaning_money_flow(text)` шлёт сообщения синхронно после COMMIT (как существующий `MONEY_FLOW_CHAT_ID`-паттерн). Если env пуст — no-op.

События, попадающие в чат:
- проведение заказа уборки (с сводкой: клиент, адрес, оплата, расходы, бонусы, прибыль по заказу, баланс кассы);
- DIV-выплата;
- ручные кассовые операции (`/cleaning_cash_add`, `/cleaning_cash_expense`, `/cleaning_cash_withdrawal`).

## 5. Вспомогательные команды

| Команда | Кому | Что делает |
|---|---|---|
| `/cleaning_balance` | бригадир, админ | Баланс кассы клининга. |
| `/cleaning_cash day|month|year` | админ | Сводка кассы за период (приход, расход по категориям, DIV, P&L, сертификаты отдельно). |
| `/cleaning_orders day|month` | админ, бригадир | Список заказов уборки за период. |
| `/cleaning_cash_add` | админ | Ручной приход. |
| `/cleaning_cash_expense` | админ | Ручной расход. |
| `/cleaning_cash_withdrawal` | админ | Изъятие. |
| `/cleaning_dividend` | админ | См. §3. |
| `/cleaning_cancel_order <id>` | админ | Soft-delete заказа + всех строк кассы по order_id, откат бонусов, оповещение. |

Баланс кассы:
```sql
SELECT COALESCE(SUM(
  CASE
    WHEN kind IN ('income', 'deposit')                 THEN amount
    WHEN kind IN ('expense', 'dividend', 'withdrawal') THEN -amount
  END
), 0)
FROM cleaning_cashbook
WHERE deleted_at IS NULL;
```

P&L за период:
```sql
SELECT
  SUM(CASE WHEN kind='income'  THEN amount ELSE 0 END) AS income,
  SUM(CASE WHEN kind='expense' THEN amount ELSE 0 END) AS expense,
  SUM(CASE WHEN kind='income'  THEN amount ELSE 0 END)
  - SUM(CASE WHEN kind='expense' THEN amount ELSE 0 END) AS profit
FROM cleaning_cashbook
WHERE happened_at >= :start AND happened_at < :end
  AND deleted_at IS NULL
  AND kind NOT IN ('dividend','withdrawal','deposit');
```

## 6. Миграция и план выката

Коммиты по порядку:

1. `app/migrations/0006_cleaning.sql` + `ensure_cleaning_schema()` в `bot.py`.
2. Константы и хелперы: `CLEANING_PAYMENT_METHODS`, `CLEANING_EXPENSE_CATEGORIES`, `send_cleaning_money_flow`, `get_cleaning_balance`, `get_cleaning_pnl`, `is_cleaning_foreman`.
3. `CleaningOrderFSM` + хендлеры.
4. `CleaningDividendFSM` + ручные кассовые операции.
5. Отчётные команды.
6. Шаблоны в `docs/notification_rules.json`: `cleaning_order_completed_summary`, `cleaning_order_rating_reminder`.
7. `.env.example`: `CLEANING_MONEY_FLOW_CHAT_ID=`.
8. Тесты:
   - `tests/test_cleaning_orders.py` — проведение, строки в БД, бонусы.
   - `tests/test_cleaning_dividend.py` — DIV не в P&L, баланс уменьшается.
   - `tests/test_cleaning_certificate.py` — сертификат не порождает income.
9. Прод-выкат: pull + restart, INSERT первого бригадира руками, добавить `CLEANING_MONEY_FLOW_CHAT_ID=3687084157` в `.env`, тестовый заказ.

Откат: химчистка не тронута, достаточно `git revert` + restart. Новые таблицы можно оставить пустыми или дропнуть отдельной миграцией.

## Открытые вопросы

1. **Бонусы за уборку** — процент тот же, что у химчистки? (Пока в дизайне — «как в химчистке»; уточнить устно перед имплементацией.)
2. **`/cleaning_cancel_order`** — откатывать ли расходы (химия/ГСМ) при отмене заказа? По умолчанию — да, soft-delete всех строк. Подтвердить с бизнесом.
3. **Transfer между кассами** — пока не нужен, заложить `kind='transfer'` при появлении потребности.
