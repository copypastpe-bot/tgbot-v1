# project.md — Контекст проекта для AI-агентов (`tgbot-v1`)

## 1. Что это за проект
Внутренний Telegram-бот клининговой компании.

Основные задачи:
- фиксация выполненных заказов мастерами;
- работа с клиентами, бонусами и бонусными транзакциями;
- расчёт зарплат/отчётов по деньгам;
- отправка сервисных сообщений через Wahelp (каналы WA/TG/MAX);
- обработка входящих webhook (Wahelp, OnlinePBX);
- запуск плановых задач (ежедневные отчёты, ДР-бонусы, промо, follow-up).

Ключевой факт по архитектуре:
- runtime почти полностью в `bot.py` (монолит ~12k строк).

## 2. Как использовать этот файл в новой сессии
Рекомендуемый стартовый промпт для нового агента:

```text
Сначала прочитай project.md, затем выполни задачу: <описание>.
Ограничения: не ломать прод-логику, правки делать минимальными, результат подтвердить SQL/логами.
```

Если задача про данные, сразу добавляй:
- окружение (локально / прод через туннель);
- идентификаторы (телефон, client_id, order_id);
- что именно нужно в результате (статусы, таймлайн, строки из БД).

## 3. Стек и запуск
- Python 3.10+
- aiogram 3.x
- asyncpg
- aiohttp (вебхук-сервер)
- PostgreSQL

Главный вход:
- `bot.py` -> `main()` -> инициализация схем/воркеров/джоб -> `dp.start_polling()`.

Таймзона бизнес-логики:
- в основном `Europe/Moscow`.

## 4. Карта репозитория
- `bot.py` — хендлеры, FSM, SQL, джобы, bootstrap.
- `notifications/`:
  - `rules.py` — загрузка правил уведомлений;
  - `outbox.py` — очередь/статусы доставки;
  - `worker.py` — фоновая отправка;
  - `webhook.py` — HTTP webhook endpoint'ы.
- `crm/`:
  - `wahelp_client.py` — низкоуровневый клиент Wahelp;
  - `wahelp_service.py` — конфиг каналов + send/ensure_user;
  - `wahelp_dispatcher.py` — маршрутизация, fallback, флаги доступности каналов.
- `docs/`:
  - `notification_rules.json` — шаблоны уведомлений;
  - `db_production_contract.md` — важные ограничения прод-БД.
- `app/migrations/` — SQL-миграции (не полный источник истины).
- `scripts/` — служебные скрипты (backup/restart/import/sync).

Важно:
- схема БД поддерживается гибридно.
- часть изменений создаётся SQL-миграциями, часть через `ensure_*_schema()` в `bot.py` при старте.

## 5. Ключевые бизнес-потоки

### 5.1 Завершение заказа
- мастер создаёт заказ через интерфейс бота;
- заказ/касса/бонусы пишутся в БД;
- в `notification_outbox` ставятся события (например `order_completed_summary`, `order_rating_reminder`);
- воркер пытается отправить уведомления по каналам Wahelp.

### 5.2 Поток уведомлений
1. Создаётся запись в `notification_outbox` со статусом `pending`.
2. Воркер берёт запись, переводит в `sending`.
3. Формируется `ClientContact` из `clients`.
4. `send_with_rules()` выбирает канал и отправляет.
5. При успехе:
- outbox -> `sent`;
- создаётся запись в `notification_messages`.
6. При ошибках:
- либо ретрай;
- либо `failed`/`cancelled` (зависит от причины).

### 5.3 Wahelp-логика маршрутизации (критично)
- если есть сохранённый `wahelp_user_id_*`, отправка идёт по нему;
- если `user_id` нет, вызывается `ensure_user_in_channel()` по номеру телефона;
- состояние каналов хранится в `client_channels`: `empty`, `priority`, `dead`, `unavailable`;
- если система считает, что мессенджер недоступен, может выставляться `clients.wahelp_requires_connection=true`;
- при этом флаге outbox-воркер перестаёт пытаться и отменяет отправки (анти-спам защита).

### 5.4 OnlinePBX -> SMS
- `/onlinepbx/webhook` принимает события звонков с проверкой токена/IP;
- для подходящих входящих звонков бот запрашивает подтверждение у админов;
- после подтверждения отправляет SMS через sms.ru.

## 6. Плановые задачи (MSK)
Стартуют из `main()`:
- `22:00` — ежедневные отчёты;
- `12:00` — ДР-бонусы;
- `11:00` — promo reminders;
- `14:00` — промо-рассылка лидам (`LEADS_SEND_START_HOUR=14`);
- `20:00` — напоминания по оплате на р/с;
- периодические ретраи `sent`;
- каждые 2 часа — follow-up по перемывам;
- `10:00` — проверка счётчика перемывов;
- еженедельно — очистка `dead` каналов.

## 7. Таблицы, которые чаще всего нужны агенту
Операционка:
- `clients`, `orders`, `staff`
- `bonus_transactions`, `cashbook_entries`, `payroll_items`

Уведомления/каналы:
- `notification_outbox`, `notification_messages`
- `client_channels`, `wahelp_delivery_issues`

Лиды/маркетинг:
- `leads`, `lead_logs`, `promo_reengagements`

Технические:
- `daily_job_runs`, `onlinepbx_sms_requests`

## 8. Контракт прод-БД (обязательно перед правками SQL)
Сначала прочитать:
- `docs/db_production_contract.md`

Критичные моменты:
- нельзя апдейтить `clients.phone_digits` напрямую (generated column);
- `clients.status` ограничен check constraint;
- учитывать расхождение `full_name` vs legacy `name` между окружениями.

## 9. Важные env-переменные
Минимум для запуска:
- `BOT_TOKEN`
- `DB_DSN`

Высокий риск/влияние:
- `ADMIN_IDS` / `ADMIN_TG_IDS`
- параметры бонусов/зарплаты (`BONUS_RATE_PERCENT`, и т.д.)
- Wahelp (`WAHELP_*` project/channel/token/login/password + webhook host/port/token)
- OnlinePBX (`ONLINEPBX_WEBHOOK_TOKEN`, `ONLINEPBX_ALLOWED_IPS`)
- sms.ru (`SMSRU_API_ID`, sender, low balance threshold)
- флаги доставки (`WA_FOLLOWUP_DISABLED`, `WA_ALLOW_WA_FALLBACK`)
- `CLIENT_BOT_TOKEN` (если нужен клиентский бот)

Секреты из `.env` не коммитить.

## 10. Локальный runbook
Установка:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Запуск:
```bash
python3 bot.py
```

Служебные скрипты:
- `scripts/reset_and_restart_bot.sh` — стоп, очистка public-таблиц, рестарт;
- `scripts/backup_telegram_bot.sh` — бэкап БД/конфигов.

Важный факт:
- формального test-suite/CI-конфига в репозитории нет.

## 11. Быстрые playbook'и диагностики

### 11.1 «Клиенту не ушло уведомление»
1. Найти клиента в `clients` по `phone/phone_digits`.
2. Взять его последний заказ из `orders`.
3. Проверить `notification_outbox` (статусы, attempts, last_error).
4. Проверить `notification_messages` (что реально ушло).
5. Проверить `clients.wahelp_requires_connection` и `wahelp_user_id_*`.
6. Проверить `client_channels` (status + last_error_code + таймлайн).
7. Проверить `wahelp_delivery_issues`.

Обычно интерпретация:
- `outbox=cancelled` + `last_error='wahelp requires connection'` + пустой `notification_messages`
означает, что защита остановила отправки, потому что не найден доступный мессенджер.

### 11.2 «Webhook не применяет статусы»
1. Убедиться, что включён webhook-сервер (`WAHELP_WEBHOOK_PORT > 0`).
2. Проверить токен/IP-ограничения.
3. Проверить форму payload и нормализацию.
4. Проверить логи around `apply_provider_status_update` и handler exceptions.

### 11.3 «В БД нет колонки / не сходится схема»
- не доверять только `app/migrations/*`;
- сначала смотреть фактическую схему в `information_schema.columns`;
- затем сверять с `ensure_*_schema()` в `bot.py`.

## 12. SQL-шаблоны для типовых задач
Клиент по номеру:
```sql
SELECT *
FROM clients
WHERE phone = '+7...'
   OR phone_digits = '7...'
   OR phone_digits = '9...';
```

Последний заказ клиента:
```sql
SELECT *
FROM orders
WHERE client_id = :client_id
ORDER BY created_at DESC, id DESC
LIMIT 1;
```

Outbox и фактическая доставка:
```sql
SELECT id, event_key, status, attempts, scheduled_at, last_attempt_at, last_error
FROM notification_outbox
WHERE client_id = :client_id
ORDER BY created_at DESC;

SELECT id, outbox_id, event_key, channel, status, sent_at, delivered_at, read_at, failed_at
FROM notification_messages
WHERE client_id = :client_id
ORDER BY created_at DESC;
```

Статусы каналов клиента:
```sql
SELECT channel, status, wahelp_user_id, last_attempt_at, last_error_code
FROM client_channels
WHERE client_id = :client_id
ORDER BY channel;
```

## 13. Стратегия изменений для агентов
- делать минимальные и локальные правки;
- для hotfix избегать широкого рефакторинга монолита;
- не снимать защиту от «долбления в стену», если это не отдельная осознанная задача;
- при data-задачах подтверждать результат реальными строками БД;
- при добавлении схемы учитывать startup-инициализацию через `ensure_*_schema()`.

## 14. Риски и частые ошибки
- сильная связность `bot.py` (скрытые побочные эффекты);
- дрейф схемы между окружениями;
- «молчаливое» переключение доставки в blocked/cancelled через channel flags;
- наличие служебных скриптов/бэкапов в repo: не трогать без необходимости.

## 15. Definition of Done для типовой задачи
Перед завершением убедиться:
- бизнес-эффект подтверждён БД-данными, а не только diff;
- нет регрессии в смежном потоке (уведомления/джобы/хендлеры);
- зафиксированы предположения и расхождения схемы/данных;
- в отчёте даны конкретные id/времена/статусы.
