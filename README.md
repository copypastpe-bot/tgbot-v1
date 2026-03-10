cat > README.md <<'MD'
# tgbot-v1

Внутренний Telegram-бот для учёта заказов, бонусов и расчёта ЗП мастеров.

## Возможности (MVP)
- Кнопка **«🧾 Я ВЫПОЛНИЛ ЗАКАЗ»** — пошаговый ввод:
  - номер телефона клиента
  - имя (если нового клиента нет в БД)
  - сумма чека и доп. продажи
  - списание бонусов (автоограничения)
  - способ оплаты
  - ДР (если известно)
  - подтверждение и сохранение
- Автозапись в БД:
  - `clients`, `orders`, `bonus_transactions`, `cashbook_entries`, `payroll_items`
  - автоподсчёт начисленных/списанных бонусов
  - расчёт ЗП мастера (база + бензин + допы)
- Поиск клиента: `/find +7XXXXXXXXXX` или `9XXXXXXXXX`
- Админ-команды:
  - `/list_masters` — список мастеров/админов
  - `/add_master <tg_user_id>` — выдать роль мастера
  - `/remove_master <tg_user_id>` — деактивировать мастера
  - `/payroll YYYY-MM` — отчёт по ЗП за месяц

## Технологии
- Python 3.10+, [aiogram 3](https://docs.aiogram.dev/)
- PostgreSQL 14+, `asyncpg`
- systemd service

## Переменные окружения (.env)
- `WAHELP_WEBHOOK_HOST` — адрес, на котором поднимается http-сервер вебхука Wahelp (по умолчанию `0.0.0.0`).
- `WAHELP_WEBHOOK_PORT` — порт вебхука. Если `0`, сервер не запускается.
- `WAHELP_WEBHOOK_TOKEN` — необязательный секрет. Передавайте его в заголовке `X-Wahelp-Token` или как `?token=...` при настройке вебхука в Wahelp.
- `ONLINEPBX_WEBHOOK_TOKEN` — необязательный секрет для вебхука OnlinePBX (`X-Onlinepbx-Token` или `?token=...`).
- `ONLINEPBX_ALLOWED_IPS` — список разрешённых IP для вебхука OnlinePBX (через запятую/пробел), например `85.119.145.174,92.53.102.165`.
- `SMSRU_API_ID` — API-ключ sms.ru для отправки SMS из подтверждения администратора.
- `SMSRU_FROM` — (опционально) имя отправителя в sms.ru.
- `SMSRU_LOW_BALANCE_ALERT_THRESHOLD` — порог баланса sms.ru для тревоги админам (по умолчанию `100`).
- `ONLINEPBX_SMS_TEXT` — текст SMS, который отправляется клиенту после подтверждения админом.

## Система уведомлений
- Правила лежат в `docs/notification_rules.json`. При изменении файлов шаблонов перезапускать бота не нужно — правила перечитываются при старте.
- При создании событий в коде формируются записи в таблице `notification_outbox`. Фоновой воркер (`NotificationWorker`) забирает готовые задачки и отправляет текст через Wahelp с учётом приоритетов.
- Каждое фактически отправленное сообщение фиксируется в `notification_messages`. Статусы (`sent`, `delivered`, `read`, `failed`) обновляются через вебхук `/wahelp/webhook`.
- Вебхук настраивается в Wahelp на URL `https://<host>:<port>/wahelp/webhook?token=...` (см. переменные окружения выше). При прочтении клиентом сообщения в Telegram автоматически отменяется отложенный WhatsApp-follow-up.
- Вебхук OnlinePBX принимает `POST application/x-www-form-urlencoded` на `/onlinepbx/webhook` и создаёт запрос админам на отправку SMS по звонкам `call_end` (`direction=inbound`) с длительностью разговора `>= 21` сек.
