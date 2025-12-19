# Контракт прод-БД: clients и orders

**Версия:** 1.0  
**Дата:** 2025-12-19  
**Цель:** Зафиксировать ключевые поля, constraints и правила работы с таблицами `clients` и `orders`, чтобы новые фичи не ломались об прод.

---

## ⚠️ КРИТИЧЕСКИЕ ОГРАНИЧЕНИЯ

### 1. Generated колонки (НЕЛЬЗЯ обновлять напрямую)

#### `clients.phone_digits`
- **Тип:** GENERATED ALWAYS AS (regexp_replace(phone, '[^0-9]', '', 'g'))
- **Правило:** БД автоматически заполняет эту колонку на основе `phone`
- **❌ НЕЛЬЗЯ:** `UPDATE clients SET phone_digits = ...` — вызовет ошибку `GeneratedAlwaysError`
- **✅ ПРАВИЛЬНО:** Обновлять только `phone`, колонка заполнится автоматически

**Пример ошибки:**
```python
# ❌ НЕПРАВИЛЬНО
await conn.execute("UPDATE clients SET phone_digits = $1 WHERE id = $2", digits, client_id)
# Ошибка: GeneratedAlwaysError: column "phone_digits" can only be updated to DEFAULT

# ✅ ПРАВИЛЬНО
await conn.execute("UPDATE clients SET phone = $1 WHERE id = $2", phone, client_id)
# phone_digits обновится автоматически
```

---

### 2. Constraints на колонке `status`

#### `clients.status`
- **CHECK constraint:** `clients_status_check`
- **Допустимые значения:** `'lead'` или `'client'` (точное совпадение)
- **❌ НЕЛЬЗЯ:** Использовать другие значения (`'active'`, `'new'`, `'inactive'` и т.д.)
- **✅ ПРАВИЛЬНО:** При создании нового клиента использовать `status = 'client'`

**Пример ошибки:**
```python
# ❌ НЕПРАВИЛЬНО
await conn.execute("INSERT INTO clients (..., status) VALUES (..., 'active')")
# Ошибка: CheckViolationError: new row violates check constraint "clients_status_check"

# ✅ ПРАВИЛЬНО
await conn.execute("INSERT INTO clients (..., status) VALUES (..., 'client')")
```

**Важно:** При создании клиента через клиентский бот всегда использовать `status = 'client'`, так как клиент уже подписался на бота.

---

## 📋 Таблица `clients`

### Ключевые поля

| Поле | Тип | Nullable | Default | Особенности |
|------|-----|----------|---------|-------------|
| `id` | BIGSERIAL | NO | - | PRIMARY KEY |
| `full_name` | TEXT | YES | NULL | Может быть NULL |
| `name` | TEXT | YES | NULL | ⚠️ **УСТАРЕВШЕЕ** — может отсутствовать в прод |
| `phone` | TEXT | YES | NULL | **UNIQUE**, формат `+7XXXXXXXXXX` |
| `phone_digits` | TEXT | NO | GENERATED | ⚠️ **GENERATED** — нельзя обновлять |
| `birthday` | DATE | YES | NULL | Формат: YYYY-MM-DD |
| `bonus_balance` | INTEGER | NO | 0 | Диапазон: -2147483648 до 2147483647 |
| `status` | TEXT | NO | - | ⚠️ **CHECK**: только `'lead'` или `'client'` |
| `address` | TEXT | YES | NULL | Основной адрес клиента |
| `last_order_at` | TIMESTAMPTZ | YES | NULL | Дата последнего заказа |
| `created_at` | TIMESTAMPTZ | NO | NOW() | Автоматически |
| `last_updated` | TIMESTAMPTZ | YES | NULL | Обновлять при изменениях |

### Важные поля для клиентского бота

| Поле | Тип | Nullable | Default | Назначение |
|------|-----|----------|---------|------------|
| `bot_tg_user_id` | BIGINT | YES | NULL | **UNIQUE** — TG ID клиента в боте |
| `bot_started` | BOOLEAN | NO | false | Флаг подписки на бота |
| `bot_started_at` | TIMESTAMPTZ | YES | NULL | Дата первого `/start` |
| `bot_bonus_granted` | BOOLEAN | NO | false | Бонус за подписку начислен |
| `preferred_contact` | TEXT | NO | 'unknown' | Предпочтительный канал связи |

### Wahelp поля

| Поле | Тип | Nullable | Default | Назначение |
|------|-----|----------|---------|------------|
| `wahelp_preferred_channel` | TEXT | YES | NULL | Предпочтительный канал Wahelp |
| `wahelp_user_id_wa` | BIGINT | YES | NULL | User ID в WhatsApp канале |
| `wahelp_user_id_tg` | BIGINT | YES | NULL | User ID в Telegram канале |
| `wahelp_user_id_max` | BIGINT | YES | NULL | User ID в MAX канале |
| `wahelp_requires_connection` | BOOLEAN | NO | false | Требуется подключение мессенджера |

### Правила работы с `clients`

#### ✅ МОЖНО обновлять:
- `full_name`, `phone`, `birthday`, `bonus_balance`
- `address`, `last_order_at`, `last_service`, `last_order_addr`, `district`
- `bot_tg_user_id`, `bot_started`, `bot_started_at`, `bot_bonus_granted`, `preferred_contact`
- `wahelp_*` поля
- `last_updated` (обновлять при любых изменениях)

#### ❌ НЕЛЬЗЯ обновлять напрямую:
- `phone_digits` (GENERATED колонка)
- `id` (PRIMARY KEY)
- `created_at` (автоматически)

#### ⚠️ ОСОБЕННОСТИ:

1. **Определение колонки имени:**
   - В прод может быть `full_name` или `name` (старая схема)
   - Использовать функцию `_clients_name_column()` для определения актуальной колонки
   - Пример из кода:
   ```python
   name_col = await _clients_name_column(conn)
   await conn.execute(f"UPDATE clients SET {name_col} = $1 WHERE id = $2", name, client_id)
   ```

2. **Формат телефона:**
   - `phone` должен быть в формате `+7XXXXXXXXXX` (11 символов)
   - `phone_digits` генерируется автоматически (только цифры)
   - При поиске использовать `phone_digits` для надежности

3. **Статус клиента:**
   - Новые клиенты: `status = 'client'`
   - Лиды: `status = 'lead'` (но обычно они в таблице `leads`)

---

## 📋 Таблица `orders`

### Ключевые поля

| Поле | Тип | Nullable | Default | Особенности |
|------|-----|----------|---------|-------------|
| `id` | BIGSERIAL | NO | - | PRIMARY KEY |
| `client_id` | BIGINT | YES | NULL | FK → `clients(id)` ON DELETE SET NULL |
| `master_id` | BIGINT | YES | NULL | FK → `staff(id)` ON DELETE SET NULL |
| `amount_total` | NUMERIC(12,2) | NO | - | Общая сумма заказа |
| `amount_cash` | NUMERIC(12,2) | YES | NULL | Оплата наличными |
| `amount_upsell` | NUMERIC(12,2) | YES | NULL | Дополнительные продажи |
| `bonus_spent` | INTEGER | NO | 0 | Потрачено бонусов |
| `bonus_earned` | INTEGER | NO | 0 | Начислено бонусов |
| `payment_method` | TEXT | NO | 'cash' | Способ оплаты |
| `created_at` | TIMESTAMPTZ | NO | NOW() | Дата создания заказа |

### Поля для перемыва (rewash)

| Поле | Тип | Nullable | Default | Назначение |
|------|-----|----------|---------|------------|
| `rewash_flag` | BOOLEAN | NO | false | Флаг перемыва |
| `rewash_marked_at` | TIMESTAMPTZ | YES | NULL | Дата отметки перемыва |
| `rewash_marked_by_master_id` | INTEGER | YES | NULL | FK → `staff(id)` |
| `rewash_followup_scheduled_at` | TIMESTAMPTZ | YES | NULL | Когда отправить follow-up |
| `rewash_result` | SMALLINT | YES | NULL | 1=устранено, 2=осталось |
| `rewash_result_at` | TIMESTAMPTZ | YES | NULL | Дата ответа клиента |
| `rewash_cycle` | INTEGER | NO | 1 | Номер попытки перемыва |

### Правила работы с `orders`

#### ✅ МОЖНО обновлять:
- Все поля, кроме `id` и `created_at`
- `rewash_*` поля для отслеживания перемывов

#### ❌ НЕЛЬЗЯ обновлять:
- `id` (PRIMARY KEY)
- `created_at` (автоматически)

#### ⚠️ ОСОБЕННОСТИ:

1. **Связи:**
   - `client_id` может быть NULL (если клиент удален)
   - `master_id` может быть NULL (если мастер удален)
   - Использовать `ON DELETE SET NULL` для безопасности

2. **Бонусы:**
   - `bonus_spent` и `bonus_earned` — INTEGER (целые числа)
   - Не использовать DECIMAL для бонусов

3. **Суммы:**
   - `amount_*` поля — NUMERIC(12,2) (до 2 знаков после запятой)
   - Максимальное значение: 9999999999.99

---

## 🔍 Частые ошибки и их решения

### Ошибка 1: `GeneratedAlwaysError` при обновлении `phone_digits`

**Симптом:**
```
asyncpg.exceptions.GeneratedAlwaysError: column "phone_digits" can only be updated to DEFAULT
```

**Причина:** Попытка обновить GENERATED колонку напрямую.

**Решение:** Обновлять только `phone`, `phone_digits` обновится автоматически.

```python
# ❌ НЕПРАВИЛЬНО
await conn.execute("UPDATE clients SET phone_digits = $1 WHERE id = $2", digits, client_id)

# ✅ ПРАВИЛЬНО
await conn.execute("UPDATE clients SET phone = $1 WHERE id = $2", phone, client_id)
```

---

### Ошибка 2: `CheckViolationError` при вставке с неправильным `status`

**Симптом:**
```
asyncpg.exceptions.CheckViolationError: new row for relation "clients" violates check constraint "clients_status_check"
```

**Причина:** Использование недопустимого значения для `status`.

**Решение:** Использовать только `'client'` или `'lead'`.

```python
# ❌ НЕПРАВИЛЬНО
await conn.execute("INSERT INTO clients (..., status) VALUES (..., 'active')")

# ✅ ПРАВИЛЬНО
await conn.execute("INSERT INTO clients (..., status) VALUES (..., 'client')")
```

---

### Ошибка 3: `UndefinedColumnError` при использовании `name` вместо `full_name`

**Симптом:**
```
asyncpg.exceptions.UndefinedColumnError: column "name" does not exist
```

**Причина:** В прод используется `full_name`, а не `name`.

**Решение:** Использовать функцию `_clients_name_column()` для определения актуальной колонки.

```python
# ❌ НЕПРАВИЛЬНО (если в прод нет колонки name)
await conn.execute("SELECT name FROM clients WHERE id = $1", client_id)

# ✅ ПРАВИЛЬНО
name_col = await _clients_name_column(conn)
await conn.execute(f"SELECT {name_col} FROM clients WHERE id = $1", client_id)
```

---

## 📝 Чеклист перед добавлением новой фичи

Перед добавлением новой фичи, которая работает с `clients` или `orders`, проверьте:

- [ ] Не обновляете ли вы `phone_digits` напрямую?
- [ ] Используете ли правильное значение для `status` (`'client'` или `'lead'`)?
- [ ] Определяете ли актуальную колонку имени (`full_name` vs `name`)?
- [ ] Обновляете ли `last_updated` при изменениях?
- [ ] Используете ли правильные типы данных (INTEGER для бонусов, NUMERIC для сумм)?
- [ ] Проверяете ли существование колонок перед использованием (если они опциональные)?

---

## 🔗 Связанные документы

- `DB/README_DataOps_Clients_Leads.md` — процедуры загрузки данных
- `docs/bots_technical_overview.md` — техническая документация по ботам
- Миграции: `app/migrations/0003_client_bot.sql`, `app/migrations/0004_rewash_fields.sql`, `app/migrations/0005_wahelp_max_channel.sql`

---

## 📞 Контакты

При возникновении вопросов или проблем с БД обращайтесь к разработчику или проверьте логи бота.

---

_Документ обновлён:_ 2025-12-19  
_Версия:_ 1.0

