# 03. Telegram-бот: код и БД

Полные детальные отчёты со ссылками файл:строка — `recon/drafts/agent-bot-flow-import-phones.md` и `recon/drafts/agent-bot-stack-scheduler-integrations.md`. Здесь — выжимка.

## Стек

Python 3.10+/3.11, aiogram 3.22 (long-polling), asyncpg, aiohttp; монолит `bot.py` (13 389 строк) + пакеты `notifications/`, `crm/`, `cleaning/`, `analytics_app/`. Вебхуки принимает собственный aiohttp-сервер (`/wahelp/webhook`, `/onlinepbx/webhook`, `/amocrm/webhook`). Планировщик самописный (`asyncio.create_task` + `schedule_daily_job`/`schedule_periodic_job`, защита от повторов через таблицу `daily_job_runs`). Тесты: 83 на unittest (bot.py не покрыт). Миграции гибридные: SQL-файлы `app/migrations/0003–0007` без раннера + идемпотентные `ensure_*_schema()` при старте. Деплой: `local → git → VPS /opt/telegram-bot`, systemd `telegram-bot.service`, логи в journald. Секреты — только `.env`.

## Флоу мастера (кратко)

Кнопка «🧾 Заказ» → FSM из 17 состояний (`bot.py:11497`): телефон (строгая валидация, `bot.py:179`) → поиск клиента по цифрам телефона → имя (если новый; клиент НЕ создаётся до подтверждения) → сумма → доп. продажа → списание бонусов → способ оплаты (+сплит) → доп. мастера (до 5) → ДР (если нет) → адрес (если нет) → подтверждение. `commit_order` (`bot.py:12587`) в одной транзакции: upsert клиента (ON CONFLICT по phone), INSERT заказа (**«Заказ №582» = PK `orders.id`, отдельного счётчика нет**), бонусы (начисление TTL 365 дн; при «р/с» — нет), payroll, оплаты, касса. После: чек в чат `ORDERS_CONFIRM_CHAT_ID` (HTML, «🧾 Заказ №N…»), строка в чат денег `MONEY_FLOW_CHAT_ID`, клиенту — `order_completed_summary` через очередь `notification_outbox` (WA→TG→MAX) + отложенный запрос оценки.

## Импорт из амо (та самая «ручная выгрузка»)

Команда `/import_amocrm` (`bot.py:9632`, права `import_leads` — admin/superadmin): ждёт CSV из амо → dry-run превью со счётчиками → подтверждение → запись. Ядро `process_amocrm_csv` (`bot.py:4334`): агрегирует строки по цифрам телефона (несколько сделок одного клиента схлопываются, побеждает свежайшая «Дата и время заказа»), матчит с `clients.phone_digits`, обновляет поля (бонусы/ДР — только если NULL в БД), «промоутит» лидов в клиенты при наличии адреса/даты заказа, остальных ведёт в таблице `leads`. Строки без телефона пропускаются. Отдельный legacy-контур: `/upload_clients` + `/import_leads` через staging `clients_raw`.

## Нормализация телефона — ГЛАВНЫЙ РИСК

В кодовой базе **минимум 5 разных семейств алгоритмов** (полная таблица из 25 мест — в drafts):

1. Канон `normalize_phone_for_db` (`bot.py:3251`) — посимвольный скан, при неудаче возвращает исходный мусор (не None);
2. её PL/pgSQL-копия в БД возвращает NULL (расхождение запись/поиск);
3. клининговый контур — свой упрощённый алгоритм и заметно более мягкая валидация;
4. amo-модули — regex `(?:\+7|8)…`, который НЕ ловит номера, начинающиеся с 9 без префикса; причём две одноимённые `_extract_phone` ведут себя по-разному;
5. импорт CSV имеет слепой fallback `+7 + последние 10 цифр`.

## БД (43 таблицы, полный DDL — `recon/data/bot_db_schema.sql`)

Ядро: `clients` (4 389; `phone` unique, `phone_digits` — generated column) ← `orders` (546; 171 за 90 дней) ← `order_masters`/`order_payments`/`payroll_items`; `bonus_transactions` (9 362), `cashbook_entries` (1 004), `notification_outbox`/`notification_messages` (~10 тыс. каждая), `client_channels` (13 164), `leads` (6 359), `staff` (11). amo-контур: `amocrm_api_events` (6 763), `amocrm_unsorted_seen`, `amocrm_webhook_events`, `amocrm_api_state` (курсоры polling). Клининг: `cleaning_orders` (1 тестовый), `cleaning_foremen` (тест). Мусор/staging: `clients_backup_20251112` (4 065), `clients_import_ready` (10 183), `clients_raw` (0), пустые `cashbook`/`payroll`.

## Интеграции сегодня

- **amoCRM**: API-polling каждые 30 с (только алерты админам о новых лидах/неразобранном/неотвеченных сообщениях; ничего не пишет в амо) + legacy-вебхук (взаимоисключён с polling). Плюс ручной CSV-импорт (выше).
- **Wahelp**: канал доставки клиентских сообщений WA→TG→MAX с fallback-цепочкой и статусами каналов.
- **OnlinePBX + SMS.ru**: входящий звонок ≥21 с → предложение отправить SMS; **Google Sheets**: тексты промо/ДР (внимание: запрос выполняется синхронно в event loop).

## Точки встраивания (как здесь принято, описательно)

Отдельный пакет с чистыми функциями (клиент + билдеры) → реэкспорт в `__init__` → env-константы + функция-гейт `_x_enabled()` → таблицы через `ensure_x_schema()` в `main()` → вечная корутина с курсорами в БД, регистрируемая в `main()` под `if task is None and enabled()`. Исходящие сообщения — только через `notification_outbox` + шаблон в `docs/notification_rules.json`. Эталоны: amoCRM polling (`notifications/amocrm_api.py` + `bot.py:1919`) и Wahelp (`crm/`). 11 текстов в `notification_rules.json` уже заготовлены под календарный флоу и не отправляются.
