# amoCRM API Polling Alerts Design

## Purpose

Replace noisy amoCRM webhooks with a controlled API polling integration that alerts admins only about actionable inbound work in one selected amoCRM pipeline.

The integration must help admins avoid missing:

- new non-call deals;
- new unsorted items;
- incoming chat messages from new or existing clients when nobody answers for 10 minutes.

## Scope

In scope:

- Use a 5-year long-lived amoCRM token from the existing external integration.
- Poll amoCRM REST API from the existing Telegram bot process.
- Filter every alert to one configured amoCRM pipeline.
- Suppress new-deal alerts when the deal lands in the configured "Новый лид" status and was created from an accepted inbound call.
- Delay incoming-message alerts for 10 minutes and suppress them if an outgoing response appears first.
- Keep the existing amoCRM webhook route in code, but allow production to disable it by clearing `AMOCRM_WEBHOOK_TOKEN`.

Out of scope:

- Creating, updating, or closing amoCRM entities.
- OAuth authorization-code and refresh-token flow.
- Admin acknowledgement buttons, repeated escalations, or SLA dashboards.
- Full chat history synchronization.

## Configuration

Add these environment variables:

```text
AMOCRM_API_BASE=https://raketacleancrm.amocrm.ru
AMOCRM_API_TOKEN=
AMOCRM_PIPELINE_ID=
AMOCRM_NEW_LEAD_STATUS_ID=
AMOCRM_POLL_INTERVAL_SEC=30
AMOCRM_UNANSWERED_DELAY_SEC=600
AMOCRM_ACCEPTED_CALL_MIN_DURATION_SEC=20
AMOCRM_LOOKBACK_MINUTES=30
```

`AMOCRM_API_TOKEN` must be stored only in `.env` / production env, never committed.

## Runtime Architecture

Add a focused `notifications.amocrm_api` module with three responsibilities:

- amoCRM HTTP client with bearer-token auth, JSON parsing, timeout, and basic backoff on `429`;
- normalization helpers for leads, contacts, unsorted items, events, notes, and alert DTOs;
- polling service functions that perform one bounded polling cycle and return admin alerts to send.

Keep Telegram sending and startup wiring in `bot.py`, matching existing project style.

The bot will start one background task when `AMOCRM_API_TOKEN`, `AMOCRM_API_BASE`, and `AMOCRM_PIPELINE_ID` are configured. If any required value is missing, API polling stays disabled and the rest of the bot starts normally.

## Data Flow

### New Deals

1. Poll `/api/v4/events` for `lead_added` since the last saved cursor.
2. For each event, fetch the lead via `/api/v4/leads/{lead_id}?with=contacts`.
3. Skip if `lead.pipeline_id != AMOCRM_PIPELINE_ID`.
4. If `lead.status_id == AMOCRM_NEW_LEAD_STATUS_ID`, inspect recent lead notes for `call_in`.
5. If a `call_in` note has `duration >= AMOCRM_ACCEPTED_CALL_MIN_DURATION_SEC`, mark the event ignored.
6. Otherwise fetch contact phone/name and send a new-deal alert.

### Unsorted

1. Poll `/api/v4/leads/unsorted` with the configured pipeline filter and recent `created_at` lower bound.
2. Deduplicate by `uid`.
3. Send an alert for each new unsorted item in the target pipeline.
4. Keep SIP/missed-call unsorted items visible because missed calls are actionable.

### Incoming Messages

1. Poll `/api/v4/events` for both `incoming_chat_message` and `outgoing_chat_message`.
2. For incoming messages, resolve contact/lead/talk identity from event data and API enrichment.
3. If the resolved lead exists, keep only target-pipeline leads.
4. Insert a pending unanswered row with `due_at = event.created_at + AMOCRM_UNANSWERED_DELAY_SEC`.
5. For outgoing messages, close matching pending rows by lead, contact, or talk.
6. On every cycle, send alerts for due pending rows that still have no outgoing response.

The primary SLA criterion is "no outgoing answer for 10 minutes", not "unread". Reading without replying must still alert.

## Persistence

Create API-specific tables instead of reusing webhook storage:

```sql
CREATE TABLE IF NOT EXISTS amocrm_api_state (
    stream text PRIMARY KEY,
    cursor_created_at integer NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS amocrm_api_events (
    event_id text PRIMARY KEY,
    event_type text NOT NULL,
    entity_id text,
    payload jsonb NOT NULL,
    action text NOT NULL,
    error text,
    created_at integer,
    processed_at timestamptz NOT NULL DEFAULT NOW(),
    notified_at timestamptz
);

CREATE TABLE IF NOT EXISTS amocrm_unsorted_seen (
    uid text PRIMARY KEY,
    pipeline_id bigint,
    payload jsonb NOT NULL,
    action text NOT NULL,
    error text,
    created_at integer,
    processed_at timestamptz NOT NULL DEFAULT NOW(),
    notified_at timestamptz
);

CREATE TABLE IF NOT EXISTS amocrm_pending_incoming (
    id bigserial PRIMARY KEY,
    event_id text NOT NULL UNIQUE,
    message_id text,
    lead_id bigint,
    contact_id bigint,
    talk_id text,
    payload jsonb NOT NULL,
    created_at integer NOT NULL,
    due_at timestamptz NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    notified_at timestamptz,
    answered_at timestamptz,
    error text
);
```

Use `action` values: `notified`, `ignored`, `pending`, `answered`, `error`.

## Admin Alerts

Keep the current operational tone:

```text
🚨 amoCRM: новая входящая заявка

Тип: новое входящее сообщение без ответа 10 минут
Сделка: #123456
Клиент: Иван
Телефон: +7 999 123-45-67
Источник: WhatsApp
Текст: Хочу уборку завтра
Ссылка: https://raketacleancrm.amocrm.ru/leads/detail/123456

Нужно ответить или позвонить.
```

Alert types:

- `new_lead`: new deal in target pipeline, not an accepted call in "Новый лид".
- `new_unsorted`: new unsorted item in target pipeline.
- `unanswered_message`: inbound chat message not answered for 10 minutes.

## Error Handling

- `401`: log and disable polling until restart, because token is invalid.
- `429`: back off and retry on the next cycle without advancing cursor.
- `5xx` / network timeout: log and retry on the next cycle without advancing cursor.
- Per-entity enrichment failure: store event as `error` with payload; do not block the full cycle.
- Telegram send failure: keep event unnotified with error so it can be inspected manually.

## Testing

Unit tests must cover:

- extracting lead/contact ids and phone from amoCRM API payloads;
- suppressing accepted-call new leads;
- allowing non-call new leads;
- creating pending rows for incoming messages;
- closing pending rows on outgoing messages;
- producing due unanswered alerts after 10 minutes;
- deduplicating event ids and unsorted uids;
- keeping cursors unchanged when the API cycle fails before completion.

Manual production verification:

1. Set env values on the server.
2. Restart `telegram-bot.service`.
3. Confirm logs show amoCRM API polling enabled.
4. Create one site/form deal in the target pipeline and confirm admin alert.
5. Create/receive an accepted inbound call in "Новый лид" and confirm no new-deal alert.
6. Create one unsorted missed call/message and confirm alert.
7. Send a chat message from an existing client, do not reply for 10 minutes, confirm alert.
8. Repeat the same but reply before 10 minutes, confirm no alert.

## References

- amoCRM integrations intro: https://www.amocrm.ru/developers/content/integrations/intro
- amoCRM events and notes API: https://www.amocrm.ru/developers/content/crm_platform/events-and-notes
- amoCRM leads API: https://www.amocrm.ru/developers/content/crm_platform/leads-api
- amoCRM contacts API: https://www.amocrm.ru/developers/content/crm_platform/contacts-api
- amoCRM unsorted API: https://www.amocrm.ru/developers/content/crm_platform/unsorted-api
- amoCRM talks API: https://www.amocrm.ru/developers/content/crm_platform/talks-api
