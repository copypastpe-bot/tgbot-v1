# Черновик агента: стек, планировщик, интеграции, точки встраивания

(сырой отчёт разведагента, войдёт в 03-bot.md)

## 1. Стек и инфраструктура кода

- Python 3.10+ (заявлено `project.md:34`), локальный `.venv-wahelp` на 3.11.
- `requirements.txt`: `aiogram==3.22.0`, `asyncpg==0.30.0`, `aiohttp==3.12.15`, `requests==2.32.3`, `google-auth==2.35.0`, `python-dotenv==1.1.1`. Тестовых пакетов нет.
- aiogram 3.x, long-polling: `bot.py:13375` `await dp.start_polling(bot)`. Телеграм-вебхуков нет.
- HTTP-сервер входящих вебхуков — `aiohttp.web`: `notifications/webhook.py:46-62`; маршруты `/wahelp/webhook`, `/onlinepbx/webhook`, `/amocrm/webhook` (`webhook.py:48-53`).
- Аналитика — отдельное приложение `analytics_app/` (`python -m analytics_app`).
- Точка входа: `bot.py:13275` `main()`, запуск `bot.py:13388-13389`.

Живое: `bot.py` (монолит 13 389 строк), `notifications/`, `crm/`, `cleaning/`, `expense_categories.py`, `analytics_app/`, `app/migrations/` (5 SQL), `scripts/`, `docs/`, `tests/`.
Мёртвое/рудименты: `handlers/` (пустой, но упомянут в устаревшем `docs/bots_technical_overview.md:37`), `import/`, бэкапы `bot.py.bak.*` (7 шт.), `test.db`, `trace.txt`, крупные `wahelp_*.jsonl`.

Деплой: прод-runbook бота в repo нет; путь `/opt/telegram-bot` виден из `scripts/backup_telegram_bot.sh:5`; конвенция `local -> git -> VPS` (`CLAUDE.md`). Формализованный runbook только у аналитики (`docs/analytics_deploy.md`).

Логи: `logging.basicConfig(INFO)` `bot.py:314` — stdout/stderr (journald на проде). Часть событий дублируется в TG-чаты `LOGS_CHAT_ID`, `MARKETING_LOG_CHAT_ID`.

Тесты: 13 файлов, 83 метода, чистый `unittest` (запуск `python -m unittest`, предположение — не задокументирован). CI нет. `bot.py` тестами не покрыт; покрыты `amocrm*` (33), `analytics` (34), `cleaning helpers` (16).

Миграции: гибрид — 5 SQL-файлов `app/migrations/0003..0007` (раннера нет, применение ручное) + идемпотентные `ensure_*_schema()` при старте (`bot.py:13281-13299`, 18 функций). `project.md:59-62,188-191`: «не доверять только app/migrations». Ограничения прод-БД — `docs/db_production_contract.md`.

## 2. Планировщик и фоновые задачи

APScheduler/cron НЕ используются. Самописные вечные корутины `asyncio.create_task` + `asyncio.sleep`:
- `schedule_daily_job(hour_msk, minute_msk, coro, name)` — `bot.py:5498` (Europe/Moscow, guard от повтора через таблицу `daily_job_runs`: `_should_run_daily_job` `bot.py:4918`).
- `schedule_periodic_job(interval, coro, name)` — `bot.py:5526`.

Задачи (регистрация `bot.py:13301-13355`):

| Имя | Расписание | Реализация | Назначение |
|---|---|---|---|
| reports | 22:00 MSK | `send_daily_reports` `bot.py:4940` | отчёты: касса, прибыль, заказы |
| birthday_bonuses | 12:00 MSK | `run_birthday_jobs` `bot.py:5241` | ДР-бонусы + сгорание + сводка |
| promo_reminders | 11:00 MSK | `run_promo_reminders` `bot.py:5418` | реактивация спящих (этап 2 отключён) |
| leads_promo | 14:00 MSK | `_send_leads_campaign_batch` `bot.py:2175` | рассылка по лидам week1..6 |
| wire_pending_reminder | 20:00 MSK | `bot.py:5181` | ОТКЛЮЧЕНА (return в начале, `bot.py:5182`) |
| sent_retry | poll 60 c | `retry_pending_sent_messages` `bot.py:4977` | ретрай зависших sent |
| rewash_counter | 10:00 MSK | `check_rewash_master_counter` `bot.py:5432` | алерт ≥5 перемывов/мес |
| dead_channels_cleanup | раз в 7 дней | `clear_dead_channels_weekly` `bot.py:5666` | сброс dead-каналов старше 30 дн |
| client_bot_health | каждые 60 c | `check_client_bot_health` `bot.py:5555` | heartbeat клиентского бота |
| amoCRM polling | цикл 30 c (мин.10) | `amocrm_api_polling_loop` `bot.py:1919` | 4 подцикла: лиды/неразобранное/чат/без ответа |
| notification-worker | poll 5 c, батч 10 | `notifications/worker.py:27` | отправка `notification_outbox` |

Конвенция новой задачи: глобальный хендл + в `global`-список `bot.py:13276`; корутина с guard `if pool is None: return`; для daily — `_should_run_daily_job`/`_mark_daily_job_run`; регистрация в `main()`.

## 3. Интеграции и HTTP-клиенты

### amoCRM — две независимые ветки
Вебхуки (legacy): `webhook.py:52-53` → `handle_amocrm_webhook` `bot.py:1336` — пишет в `amocrm_webhook_events`, шлёт алерт всем `ADMIN_TG_IDS` в Telegram. События: `leads.add`, `unsorted.add`, `message.add`, `leads.chat` (`notifications/amocrm.py:10`). `.env.example`: webhook-токен пустой, когда работает polling — ветки взаимоисключающие.

API-polling (актуальная): клиент `AmoCRMAPIClient` `notifications/amocrm_api.py:62` (aiohttp). Гейт `_amocrm_api_enabled()` `bot.py:1404` (нужны `AMOCRM_API_BASE`+`TOKEN`+`PIPELINE_ID`). Цикл `bot.py:1919-1940`: `_amocrm_poll_new_leads_once` `bot.py:1541`, `_amocrm_poll_unsorted_once` `bot.py:1619`, `_amocrm_poll_chat_events_once` `bot.py:1751` (+`_amocrm_close_pending_for_outgoing` `bot.py:1709`), `_amocrm_notify_due_unanswered_once` `bot.py:1847`. Курсоры в БД (`_amocrm_get_cursor`/`_set_cursor` `bot.py:1049/1068`), дедуп `_amocrm_has_notified_lead_alert` `bot.py:1459`. Алерты — только Telegram админам (`bot.py:1496-1503`). Схема `ensure_amocrm_api_schema` `bot.py:983`.

### Wahelp (WA/TG/MAX)
Три слоя: `crm/wahelp_client.py:148` (транспорт, `https://app.wahelp.me`) → `crm/wahelp_service.py` (каналы) → `crm/wahelp_dispatcher.py` (`send_with_rules` `:448`, цепочка каналов, статусы `empty/priority/dead/unavailable`, суточный лимит `:360`, followup `:668`). Прямая отправка через клиентский TG-бот `send_to_client_bot` `:369` (свой `Bot` на `CLIENT_BOT_TOKEN`). Входящий вебхук → `handle_wahelp_inbound` `bot.py:2669` (ответы «1»/«STOP», оценки 1–5). Статусы доставки `apply_provider_status_update` `notifications/outbox.py:542`.

### OnlinePBX + SMS.ru
Вебхук `webhook.py:50-51` → `handle_onlinepbx_inbound` `bot.py:1237`: входящий `call_end` с разговором ≥21 c (`bot.py:233`) → запрос в `onlinepbx_sms_requests` + подтверждение у админов. SMS через `requests.post("https://sms.ru/sms/send")` `bot.py:1165` в `to_thread`. Контроль баланса `bot.py:1203-1215`.

### Google Sheets
Тексты промо/ДР: service-account `google-auth` (`bot.py:439-442`), `requests.get(sheets.googleapis.com...)` `bot.py:456-457` — БЕЗ to_thread (блокирует event loop). Кэш 900 с. Креды `docs/Sheets.json` (`bot.py:240`).

### Секреты
Только env-файлы (`.env`, `.env.local`, шаблон `.env.example`), `os.getenv` при импорте. Полный список имён — в отчёте агента (сохранён факт: ~80 переменных, группы AMOCRM_*, WAHELP_*, ONLINEPBX_*, SMSRU_*, TEXTS_*, бизнес-константы, чаты).

## 4. Функционал, заменяющий воронку реализации CRM

- Проведение заказа: `commit_order` `bot.py:12587` (см. черновик flow). Параллельный клининг-контур: `cleaning/handlers.py`, `cleaning/orders.py`.
- Бонусы: `bonus_transactions` (списание `bot.py:12701`, начисление `bot.py:12719`); ДР `_accrue_birthday_bonuses` `bot.py:4712`; сгорание `_expire_old_bonuses`; промо-минимум `_ensure_min_bonus_for_promo` `bot.py:2486` (200 б., TTL 365 дн); откат при удалении заказа `bot.py:10250`.
- Обратная связь: постановка `bot.py:3042`; поля рейтинга `ensure_orders_rating_schema` `bot.py:2456`; приём «1»–«5» `bot.py:2698-2704`, `_select_pending_rating_order` `bot.py:2916`, `_process_rating_response` `bot.py:2931`; ветвление 5/4/≤3 (`bot.py:2956/2964/2972`), алерт `_notify_rating_admins` `bot.py:2979`.
- Отзывы: отдельного модуля нет; ссылки Яндекс/2ГИС вшиты в шаблоны `docs/notification_rules.json:238,297`. Факт отзыва не отслеживается.
- Рассылки: промо `run_promo_reminders` `bot.py:5418` → `_process_promo_stage` `bot.py:2618` (≥8 мес. без заказа, лимит 30/день); лиды `_send_leads_campaign_batch` `bot.py:2175` (тексты `bot.py:1943-1968`); ДР `_schedule_birthday_congrats` `bot.py:2568`. Транспорт — `notification_outbox` + NotificationWorker, шаблоны `docs/notification_rules.json` (24 события).

## 5. Точки встраивания (как есть)

- Модуль интеграции = отдельный пакет верхнего уровня с чистыми функциями (клиент, нормализация, билдеры текстов), реэкспорт в `__init__.py`; БД/pool/Telegram остаются в `bot.py`.
- Env-константы на уровне модуля + функция-гейт вида `_amocrm_api_enabled()`.
- Таблицы через идемпотентную `ensure_<name>_schema(conn)` в `main()` (`bot.py:13281-13299`); SQL-файл миграции опционален.
- Эталон polling: `notifications/amocrm_api.py` + цикл в `bot.py:1919` + курсоры в БД + дедуп + запуск под `if task is None and enabled()` + отмена в `finally` (`bot.py:13381-13386`).
- Эталон исходящих: хендлер кладёт в `notification_outbox` (`_try_enqueue_notification` `bot.py:552`), тексты в `docs/notification_rules.json`, доставка NotificationWorker → `send_with_rules`.
- Новый вебхук: маршрут в `WahelpWebhookServer.create_app()` (`webhook.py:46-54`), обработчик передаётся из `main()` (`bot.py:13358-13369`), поднимается при `WAHELP_WEBHOOK_PORT > 0`, падение не валит бота.
- Отключение функционала — мягкое: ранний `return` + комментарий с датой (примеры `bot.py:5182`, `bot.py:13325-13327`).
- Таймзона бизнеса `Europe/Moscow`, в БД UTC, конверсия в каждой джобе.
