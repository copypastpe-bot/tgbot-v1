# Черновик агента: флоу мастера, импорт amoCRM CSV, нормализация телефонов

(сырой отчёт разведagenta, войдёт в 03-bot.md и 08-current-processes.md)

# 1. ФЛОУ МАСТЕРА — создание заказа

## 1.1 Точка входа и FSM

- FSM: `bot.py:11497-11514` — `class OrderFSM(StatesGroup)` со стейтами: `phone, name, amount, upsell_flag, upsell_amount, bonus_spend, bonus_custom, waiting_payment_method, payment_split_prompt, payment_split_amount, payment_split_method, add_more_masters, pick_extra_master, maybe_bday, name_fix, waiting_address, confirm`.
- Кнопка запуска: `bot.py:11528` — `@dp.message(F.text.in_(["🧾 Я ВЫПОЛНИЛ ЗАКАЗ", "🧾 Заказ"]))` → `start_order` (`bot.py:11529-11537`). Проверка прав: `ensure_master(msg.from_user.id)` (`bot.py:11531`).
- Клавиатура мастера `master_kb` содержит `"🧾 Заказ"` и `"🔍 Клиент"`.
- Отдельный (параллельный) клининговый флоу живёт в `cleaning/handlers.py` (`CleaningOrderFSM`, `got_phone` — `cleaning/handlers.py:288`).

## 1.2 Ввод телефона клиента

- Шаг `OrderFSM.phone`: хэндлер `got_phone` — `bot.py:11539-11582`.
  - Валидация формата: `is_valid_phone_format` (`bot.py:11542`, определение — `bot.py:179-182`). Допускаются 11 цифр с началом `7`/`8` либо 10 цифр с началом `9`.
  - Нормализация: `phone_in = normalize_phone_for_db(user_input)` — `bot.py:11548`.
  - Поиск клиента: `_find_client_by_phone(conn, user_input)` — `bot.py:11550`, определение `bot.py:3385-3410`. Матч по `regexp_replace(phone,'[^0-9]+','','g') = ANY($1)` с двумя кандидатами: цифры нормализованного номера и «сырые» цифры ввода.

## 1.3 Если клиента с таким телефоном нет

- Ветка «не найден»: `bot.py:11575-11582` — в state кладётся `client_id=None`, `bonus_balance=0`, `client_address=""`, стейт → `OrderFSM.name`, сообщение `"Клиент не найден. Введите имя клиента:"`.
- Новый клиент в БД в этот момент НЕ создаётся. Запись создаётся только при подтверждении заказа: `bot.py:12632-12645` — `INSERT INTO clients ... ON CONFLICT (phone) DO UPDATE ...`. Upsert по уникальному `phone`; при конфликте обновляются `full_name`/`birthday` через `COALESCE`, статус принудительно `'client'`, адрес перезаписывается только если был пустым (`bot.py:12637-12642`).
- Если клиент найден, но имя «плохое» или `status == 'lead'` — принудительный шаг `OrderFSM.name_fix` (`bot.py:11562-11569`, хэндлер `fix_name` — `bot.py:11587-11597`).

## 1.4 Шаги и обязательность полей

| Шаг | Файл:строка | Обязательность |
|---|---|---|
| Телефон | `bot.py:11539` | обязателен, строгий формат |
| Имя (если новый клиент) | `bot.py:11608-11611` | обязательно (в `name_fix` — проверка на пустое и `is_bad_name`, `bot.py:11591-11594`) |
| Сумма чека | `bot.py:11614-11629` (`parse_money` — `bot.py:11599`) | обязательна, число ≥ 0 |
| Доп. продажа Да/Нет | `bot.py:11631-11638` | обязателен выбор |
| Сумма доп. продажи | `bot.py:11640-11649` | только если «Да» |
| Списание бонусов | `ask_bonus` — `bot.py:11651-11682`; `got_bonus_spend` — `bot.py:11684`; `bonus_custom_amount` — `bot.py:11716` | пропускается, если баланс/лимит = 0 (`bot.py:11663-11670`). Лимиты: `MAX_BONUS_RATE`, `MIN_CASH` |
| Способ оплаты | `order_pick_method` — `bot.py:11743-11778` | обязателен, только из `PAYMENT_METHODS + [GIFT_CERT_LABEL]` |
| Сплит оплаты | `_prompt_payment_split` — `bot.py:11781`; `bot.py:11841` | опционально; для `р/с` и сертификата пропускается (`bot.py:11764`, `11776-11777`) |
| Доп. мастера | `ask_extra_master` — `bot.py:11975`; `bot.py:12031`; `bot.py:12044` | опционально, максимум `MAX_ORDER_MASTERS` (5) |
| ДР клиента | `proceed_order_finalize` — `bot.py:12455-12468`; `got_bday` — `bot.py:12472-12480` | пропускается, если ДР уже есть; иначе можно ввести `-` |
| Адрес | `_ensure_address_before_confirm` — `bot.py:12441-12452`; `capture_order_address` — `bot.py:12483-12495` | спрашивается только если адреса нет ни в базе, ни введённого; «Нет адреса»/`-` → пустой |
| Подтверждение | `show_confirm` — `bot.py:12497-12580`; `commit_order` — `bot.py:12587` | текст «подтвердить»/«отмена» |

Расчёт З/П и бонусов в `show_confirm`: `bonus_earned = qround_ruble(cash_payment * BONUS_RATE)`, `base_pay` с полом 1000, `FUEL_PAY`, `upsell_pay` (`bot.py:12505-12518`); для `р/с` бонусы и зарплата = 0/только бензин. Доли между мастерами — `_split_amount` (`bot.py:12525-12535`).

## 1.5 После подтверждения (`commit_order`, `bot.py:12587-12881`)

Всё в одной транзакции (`bot.py:12629-12630`):

1. Upsert клиента — `bot.py:12631-12645`, получает `client_id`.
2. INSERT заказа — `bot.py:12653-12665`: `INSERT INTO orders (...) RETURNING id`, `order_id = order["id"]`. **Номер заказа = PK `orders.id`, отдельного счётчика нет.** `master_id` подзапросом из `staff` по `tg_user_id`; `phone_digits` = `regexp_replace($3,'[^0-9]+','','g')` (в `clients` колонка GENERATED — `docs/db_production_contract.md:13-27,64`).
3. Флаг `awaiting_wire_payment` для «р/с» — `bot.py:12666-12670`.
4. Апсерт мастера в `staff` — `bot.py:12672-12690`.
5. Бонусы: списание `reason='spend'` — `bot.py:12699-12717`; начисление `reason='accrual'`, `expires_at` +365 дней — `bot.py:12718-12737` (не при «р/с»). Обе с `_enqueue_bonus_change`.
6. `order_masters` + `payroll_items` по долям — `bot.py:12739-12775`.
7. `order_payments` по частям, расхождение добивается в первую часть — `bot.py:12776-12806`.
8. Касса: `_record_order_income` per часть (`bot.py:12822-12830`, опр. `bot.py:5998-6042`) — `cashbook_entries kind='income'`, коммент `"Поступление по заказу #{order_id}"`; «Карта Женя» дублируется с `"{notify_label} / Заказ №{order_id}"` (`bot.py:6027`).
9. Клиентское уведомление в очередь: `_enqueue_order_completed_notification` — `bot.py:12839-12850`, опр. `bot.py:3042-3092`.

### 1.5.1 Сообщение в чат «деньги» (касса)

- `_notify_order_income` — `bot.py:12832-12837`, опр. `bot.py:5979-5995`. Чат: env `MONEY_FLOW_CHAT_ID` (`bot.py:205`).
- Шаблон (`bot.py:5986-5993`): `✅-{сумма}₽ {display}` + `Касса - {баланс}₽`; `display` = `"{notify_label} / Заказ №{order_id}"` либо `"Поступление по заказу #{order_id}"`.
- `notify_label` (`bot.py:12808-12816`): улица (`extract_street`) либо `"{Имя} …1234"` (`mask_phone_last4`, `bot.py:3313-3317`).

### 1.5.2 Сообщение-«чек» в чат подтверждения заказов

- Блок `bot.py:12857-12879`, только если задан `ORDERS_CONFIRM_CHAT_ID` (`bot.py:204`).
- HTML (`bot.py:12876`): `🧾 Заказ №{order_id}` (`bot.py:12869`), клиент `{Имя …1234}`, адрес (если есть), ДР, оплата (с разбивкой по методам), итоговый чек, бонусы списано/начислено, доп. продажа, мастера, пометка «р/с».
- Ошибки отправки только логируются (`bot.py:12878-12879`), заказ уже сохранён.
- Мастеру в ЛС: `"Готово ✅ Заказ сохранён."` — `bot.py:12882`.

### 1.5.3 Сообщение клиенту (чек и бонусы)

- `_enqueue_order_completed_notification` (`bot.py:3042-3092`) кладёт в `notification_outbox`: `order_completed_summary` (payload `total_sum, used_bonus, earned_bonus, bonus_balance, amount_due, bonus_expire_date`, `bot.py:3066-3078`) либо `order_completed_wire_pending` при «р/с» (`bot.py:3057-3063`); плюс `order_rating_reminder` (`bot.py:3079-3084`) и `UPDATE orders SET rating_requested_at = NOW()` (`bot.py:3085-3092`).
- Шаблон — `docs/notification_rules.json:170-187`, ключ `order_completed_summary`.
- Доставка через wahelp-диспетчер WA→TG→MAX (`crm/wahelp_dispatcher.py:92`, `notifications/worker.py:107-114`).
- Для не-«р/с» дополнительно `post_order_bonus_delta(conn, order_id)` — `bot.py:12852-12856`.

# 2. ИМПОРТ ИЗ amoCRM (CSV)

## 2.1 Команда и права

- Команда: `/import_amocrm` — `bot.py:9632`, хэндлер `import_amocrm_start` (`bot.py:9633-9642`). В меню: `bot.py:6221`; в help: `bot.py:7166`.
- Права: `has_permission(..., "import_leads")` (`bot.py:9634`) — роли `superadmin` (`bot.py:3172,3183`) и `admin` (`bot.py:3201`); у `master` нет (`bot.py:3211-3215`).
- FSM: `AmoImportFSM: waiting_file, waiting_confirm` — `bot.py:9535-9537`.

## 2.2 Пайплайн

1. `/import_amocrm` → `waiting_file`, просят CSV UTF-8 с `;` (`bot.py:9636-9641`).
2. `import_amocrm_file` (`bot.py:9651-9711`): права (`9653`), расширение `.csv` (`9659-9660`), скачивание (`9662-9669`), декодирование `utf-8-sig`/`utf-8` (`9671-9681`), текст в state (`9682`).
3. Dry-run превью: `process_amocrm_csv(conn, csv_text, dry_run=True)` (`bot.py:9686`), счётчики + первые 10 ошибок (`bot.py:9696-9702`), кнопки Да/Нет.
4. `import_amocrm_confirm_yes` (`bot.py:9721-9752`) → запись (`bot.py:9733`) + отчёт.
5. Отмена: `bot.py:9645-9649`, `9754-9758`; нефайловое сообщение — `bot.py:9714-9716`.

Отдельная другая команда `/upload_clients` (`bot.py:9539-9629`) грузит простой CSV в staging `clients_raw`, в связке с `/import_leads_dryrun` (`bot.py:9158`) и `/import_leads` (`bot.py:9298`) — другой контур.

## 2.3 Ядро: `process_amocrm_csv` (`bot.py:4334-4707`)

Парсинг (`bot.py:4339-4348`): автоопределение разделителя (`;` по умолчанию, `,` если запятых больше); `csv.DictReader`; чистка BOM.

Читаемые колонки: телефон — первый непустой из 11 вариантов колонок (`bot.py:4358-4372`); `Основной контакт` → имя (`4409-4411`); `Бонусные баллы (контакт)` (`4413-4415`); `День рождения (контакт)` (`4417-4419`); `Услуга` — сплит по `\n;,` (`4421-4422`, `_amo_split_services` `bot.py:4277-4286`); `Адрес` (`4424-4426`); `Район города` (`4428-4430`); `Адрес (контакт)` (`4432-4434`); `Источник трафика (контакт)`/`Источник траффика` (`4436-4439` — два написания!); `Источник сделки` (`4441-4442`); `Дата и время заказа` → max = «лучшая строка» (`4444-4451`); `Дата закрытия` → `max_closed_at` (`4453-4455`).

Даты: `%d.%m.%Y %H:%M:%S` / `%H:%M` / `%d.%m.%Y`, затем ISO (`_amo_parse_datetime` `bot.py:4208-4230`); naive = UTC.

Дедуп: строки агрегируются по `digits` телефона (`bot.py:4374-4407`) — несколько сделок с одним телефоном схлопываются; «лучшая» = max «Дата и время заказа». Без телефона — skip, счётчик `skipped_no_phone` (`4375-4377`).

Матч с БД (`bot.py:4505-4514`): `clients.phone_digits=$1`, затем `regexp_replace(phone,...) = $1 LIMIT 1`.

Обновление существующего клиента (`bot.py:4516-4590`): `phone` если отличается; `bonus_balance` только если NULL; `birthday` только если NULL; `last_order_at` если новее; `last_service` merge без дублей (`_amo_merge_services` `bot.py:4289-4312`); адреса/район если отличаются; промоушен лида в клиента при `has_address_or_order` (`4498`, `4562-4568`) + удаление из `leads` (`4582-4589`).

Создание: новый клиент при `has_address_or_order` (`bot.py:4597-4620`) + удаление лида (`4622-4626`); иначе работа с `leads` — update (`4628-4676`) или insert (`4678-4700`).

Транзакция одна на файл; dry-run → rollback (`bot.py:4471-4473`, `4702-4706`). Ошибки: список `errors`, фактически только `"{digits}: нет данных по строке"` (`4488-4496`). Счётчики: `rows, phones, clients_updated, clients_inserted, clients_promoted, leads_inserted, leads_updated, leads_deleted, skipped_no_phone` (`4457-4468`).

Факт: `_format_amocrm_counters` определена ТРИЖДЫ с одинаковым телом — `bot.py:4239-4251`, `4264-4276`, `4315-4327` (побеждает последняя).

# 3. НОРМАЛИЗАЦИЯ ТЕЛЕФОНА — все места

Канон: `bot.py:3251-3308` — `normalize_phone_for_db(s)`: посимвольный скан, старт с 7/8/9; 7/8 → 11 цифр, 9 → 10; `+7...`; fallback по всем цифрам; при неудаче возвращает исходную строку (не None). `only_digits` — `bot.py:3248-3249`.

| # | Файл:строка | Функция | Алгоритм | Статус |
|---|---|---|---|---|
| 1 | `bot.py:3251` | `normalize_phone_for_db` | посимвольный скан | эталон |
| 2 | `bot.py:179-182` | `is_valid_phone_format` | 11 и 7/8, или 10 и 9 | валидатор |
| 3 | `bot.py:1091-1102` | `_normalize_onlinepbx_phone` | канон + ужесточение, иначе None | обёртка |
| 4 | `bot.py:4182-4197` | `_amo_normalize_phone` | канон + правки; ветка len==10 НЕ проверяет ведущую 9 | обёртка с отличием |
| 5 | `bot.py:3385-3410` | `_find_client_by_phone` | 2 кандидата, regexp_replace | поиск |
| 6 | `bot.py:3313-3317` | `mask_phone_last4` | последние 4 | маска |
| 7-8 | `bot.py:9165-9205`, `9310-9350` | PL/pgSQL функция (2 копии) | копия канона, но NULL вместо исходной строки | эквивалент с отличием |
| 9 | `bot.py:12654-12660` | INSERT заказа | только цифры | — |
| 10 | `cleaning/client.py:19-30` | `normalize_phone` | без посимвольного скана | ОТЛИЧАЕТСЯ |
| 11 | `cleaning/handlers.py:190-192` | `_is_valid_phone` | 10–11 цифр без проверки первой | ОТЛИЧАЕТСЯ (мягче) |
| 12 | `cleaning/client.py:48` | `find_client_by_phone` | аналог п.5 | — |
| 13 | `notifications/amocrm.py:259-268` | `_extract_phone` | regex `(?:\+7|8)...`, не берёт номера с 9 без префикса | ОТЛИЧАЕТСЯ |
| 14 | `notifications/amocrm.py:249-256` | `_first_phone` | обёртка п.13 | — |
| 15 | `notifications/amocrm_api.py:409-411` | `_extract_phone` | та же regex, но возврат СЫРОЙ подстроки без нормализации | ОТЛИЧАЕТСЯ (одно имя — разное поведение с п.13) |
| 16 | `notifications/amocrm_api.py:161-172` | `extract_contact_phone` | field_code PHONE, strip, без нормализации | — |
| 17 | `scripts/get_wahelp_user_id.py:39-49` | `_normalize_phone` | итог `7XXXXXXXXXX` без `+`; 10 цифр без проверки 9 | ОТЛИЧАЕТСЯ |
| 18 | `scripts/send_test_message_to_client_bot.py:86-94` | `normalize_phone` | близко к канону, без скана | — |
| 19 | `scripts/export_wahelp_contacts.py:53-69` | `_normalize_phone` | список вариантов, хвосты -10/-7 | ОТЛИЧАЕТСЯ (fuzzy) |
| 20 | `crm/wahelp_service.py:108-116,142-151` | send/ensure | телефон как есть | без нормализации |
| 21 | `notifications/outbox.py:276-316`, `worker.py:107-114` | доставка | как есть из БД | — |
| 22 | `bot.py:6370-6378` | `add_master_phone` | канон + `+7` required | обёртка |
| 24 | `bot.py:7375-7385` | `client_set_phone` | канон + `+7` + 11 цифр | строгая обёртка |
| 25 | `bot.py:4385,4485,4610,4692` | fallback импорта | `"+7" + digits[-10:]` вслепую | ОТЛИЧАЕТСЯ |

Вывод: минимум ПЯТЬ разных семейств алгоритмов: (1) посимвольный скан; (2) все цифры + правила длины; (3) regex `(?:\+7|8)` — игнорирует номера с 9; (4) хвостовой fuzzy; (5) слепой `+7 + last10`. Валидация в мастерском и клининговом флоу не совпадает. Python-канон возвращает мусор как есть, SQL-двойник — NULL (возможное расхождение запись/поиск — помечено как предположение).
