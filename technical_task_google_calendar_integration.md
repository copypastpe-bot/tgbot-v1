# Техническое задание: Интеграция Google Calendar -> `tgbot-v1`

## 0) Обязательный контекст
Перед началом работы агент обязан прочитать [project.md](/Users/evgenijpastusenko/Projects/tgbot-v1/project.md).

Это ТЗ дополняет `project.md` и определяет конкретную реализацию календарного потока.

## 1) Цель
Перевести создание заказов в поток «Google Calendar -> БД -> Telegram-бот», чтобы:
- заказы автоматически появлялись в БД из календаря;
- мастер выбирал заказ из inline-списка (без ручного ввода клиента/адреса);
- клиент получал напоминание за 24 часа;
- по реакциям/не-реакциям клиента шли автоматические эскалации админам.

## 2) Зафиксированные решения
- Календарь: один аккаунт Google Calendar.
- Интеграция: backend подключается к Google Calendar API.
- Синхронизация: polling каждые 2 минуты.
- Источник истины по заказам: календарь (односторонне calendar -> bot).
- Идентификатор внешнего заказа: `google_event_id`.
- Мастера при импорте не назначаются.
- В списке мастеру показывать непроведённые заказы на сегодня/завтра/послезавтра.
- Максимум 10 заказов в inline-списке.
- Если заказ уже проведён мастером, изменения календаря его не меняют.
- Если заказ не проведён и в календаре отменён/удалён, заказ архивируется (клиент остаётся в БД).
- Админ-уведомления: в личные сообщения админов по `ADMIN_IDS`/`ADMIN_TG_IDS`.
- Повтор админ-эскалации: каждые 10 минут до нажатия `Я связался`.
- Таймзона: `Europe/Moscow`.

## 3) Входные данные события календаря
Пример карточки:
- `summary`: `Авт! Диван и ковер Наталья`
- `start/end`: дата и время
- `location`: адрес
- `description`: услуги + строка с именем и телефоном, например `Наталья +79519177091`

Что извлекаем:
- `google_event_id`
- `start_at`, `end_at`
- `client_name`
- `client_phone` (нормализовать до `+7XXXXXXXXXX`)
- `address`
- `services_text`
- raw payload (для трассировки)

## 4) Область работ (MVP)
1. Синхронизация Google Calendar в БД.
2. Inline-выбор календарного заказа мастером.
3. Проведение заказа мастером по существующему flow с минимальными изменениями.
4. Напоминание клиенту за 24 часа.
5. Фиксация реакции клиента (подтверждение/отмена/нет ответа).
6. Эскалация админам с кнопкой `Я связался` и повторами до ack.

## 5) Не входит в MVP
- Двусторонняя синхронизация (bot -> calendar).
- Автоматическое назначение мастера из календаря.
- Сложная NLP-классификация ответов клиента.
- Рефакторинг монолита `bot.py`.

## 6) Изменения схемы БД

### 6.1 Таблица `calendar_orders`
Поля (минимально необходимые):
- `id bigserial primary key`
- `google_event_id text not null unique`
- `status text not null default 'active'`  
  Допустимые: `active`, `archived`, `completed`
- `start_at timestamptz not null`
- `end_at timestamptz`
- `timezone text not null default 'Europe/Moscow'`
- `title_raw text`
- `description_raw text`
- `location_raw text`
- `client_name text`
- `client_phone text`
- `address text`
- `services_text text`
- `client_id bigint references clients(id) on delete set null`
- `google_updated_at timestamptz`
- `payload_json jsonb not null default '{}'::jsonb`
- `archived_reason text`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

Индексы:
- `(status, start_at)`
- `(client_id)`

### 6.2 Таблица `order_confirmations`
Поля:
- `id bigserial primary key`
- `calendar_order_id bigint not null unique references calendar_orders(id) on delete cascade`
- `state text not null default 'pending'`  
  Допустимые: `pending`, `confirmed`, `cancelled`, `no_response`, `delivery_failed`, `acked`
- `reminder_due_at timestamptz not null`
- `reminder_sent_at timestamptz`
- `delivery_state text`
- `delivery_checked_at timestamptz`
- `response_text text`
- `responded_at timestamptz`
- `admin_alert_sent_at timestamptz`
- `next_repeat_at timestamptz`
- `admin_ack_by bigint`
- `admin_ack_at timestamptz`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

Индексы:
- `(state, next_repeat_at)`
- `(reminder_due_at)`

### 6.3 Связь с уже существующими заказами
В `orders` добавить:
- `calendar_order_id bigint unique references calendar_orders(id) on delete set null`

Назначение:
- исключить двойное проведение одного календарного заказа.

## 7) Синк календаря (backend, polling)

### 7.1 Интервал
- Каждые 2 минуты.

### 7.2 Алгоритм
1. Получить изменения событий календаря.
2. Для каждого события:
- извлечь данные;
- нормализовать телефон;
- найти/создать клиента по телефону;
- upsert в `calendar_orders` по `google_event_id`.
3. Если событие отменено/удалено:
- если связанный `orders` отсутствует -> `calendar_orders.status='archived'`;
- если `orders` уже существует -> не менять проведённый заказ.
4. Если событие изменилось (дата/время/адрес/текст):
- обновить `calendar_orders`, но только если ещё не проведено.

### 7.3 Идемпотентность
- Все операции синка должны быть безопасны к повтору.
- Ключ дедупликации: `google_event_id`.

## 8) Изменения в боте (мастерский flow)

### 8.1 Шаг выбора заказа
Команда `/order`:
- сначала показать inline-список из `calendar_orders`:
  - `status='active'`
  - дата в диапазоне [сегодня, +2 дня] по MSK
  - ещё не проведён (`orders.calendar_order_id is null`)
  - лимит 10

### 8.2 После выбора
- в FSM сохранить `calendar_order_id`, `client_id`, `phone`.
- далее оставить существующий flow:
  1. телефон (можно предзаполнить/подтвердить)
  2. сумма чека
  3. доп. продажа (да/нет; если да -> сумма)
  4. списание бонусов
  5. способ оплаты
  6. добавить второго мастера (опционально)
  7. подтверждение

### 8.3 Финал проведения
- создать запись в `orders` с `calendar_order_id`.
- выставить `calendar_orders.status='completed'`.

## 9) Контур напоминания клиенту за 24 часа

### 9.1 Постановка напоминания
Для каждого `calendar_order` в `active`:
- `reminder_due_at = start_at - interval '24 hours'`
- создать/обновить `order_confirmations`.

### 9.2 Отправка
Когда `now() >= reminder_due_at` и `reminder_sent_at is null`:
- отправить сообщение клиенту через текущую Wahelp-логику;
- сохранить `reminder_sent_at` и `delivery_state`.

### 9.3 Текст и реакции
- Тексты подтверждения/отмены задаются бизнесом отдельно (конфиг).
- Обработчик входящих сообщений классифицирует:
  - `confirmed`
  - `cancelled`
  - `other`

## 10) Контур эскалации админам

### 10.1 Когда эскалировать
Эскалация обязательна, если:
- клиент явно отменил (`cancelled`), или
- через 3 часа после `reminder_sent_at` нет ответа (`no_response`), или
- доставка напоминания неуспешна (`delivery_failed`).

### 10.2 Формат алерта
Отправлять каждому админу в личку:
- клиент, телефон, адрес, время заказа, причина эскалации.
- inline-кнопка: `Я связался`.

### 10.3 Повторы
- если никто не нажал `Я связался`, повторять каждые 10 минут.
- остановка повторов только после `admin_ack_at`.

## 11) Логирование и диагностика
Нужно логировать:
- поллинг календаря (успех/ошибка, количество событий);
- парсинг карточек (особенно ошибки телефона);
- изменения статуса `calendar_orders`;
- отправку напоминаний;
- классификацию ответов;
- отправку/повторы/ack админ-эскалаций.

## 12) Критерии приёмки
1. Новое событие календаря появляется в `calendar_orders` максимум через 2 минуты.
2. Изменение даты/времени в календаре отражается в `calendar_orders`, если заказ не проведён.
3. Отменённый непроведённый заказ получает `status='archived'` и исчезает из inline.
4. Мастер может выбрать заказ inline (до 10 шт) и провести его без ручного ввода данных клиента.
5. За 24 часа уходит напоминание клиенту.
6. Ответ клиента фиксируется в `order_confirmations`.
7. При отмене / no-response (3ч) / delivery_failed уходит алерт всем админам.
8. Повтор алерта идёт каждые 10 минут до нажатия `Я связался`.

## 13) Порядок реализации
1. Миграции БД (`calendar_orders`, `order_confirmations`, `orders.calendar_order_id`).
2. Backend polling + parser + upsert/archiving.
3. Inline-список заказов в `/order`.
4. Привязка проведения к `calendar_order_id`.
5. Job напоминаний за 24 часа.
6. Классификация ответов клиента.
7. Эскалации и цикл повторов до ack.
8. Финальная диагностика и smoke-check на проде.

## 14) Технические ограничения
- Правки должны быть минимальными и локальными.
- Не отключать существующую защиту от бесконечных попыток доставки.
- Секреты/ключи не хранить в репозитории.
- При расхождении схемы — доверять фактической БД, не только migration-файлам.
