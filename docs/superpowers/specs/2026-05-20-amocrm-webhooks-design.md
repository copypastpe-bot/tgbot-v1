# amoCRM Webhooks Design

## Purpose

Add a narrow inbound amoCRM webhook integration so admins receive urgent Telegram alerts for new incoming requests and do not miss leads, unsorted items, or incoming messages.

## Scope

The first release handles only three business events:

- New deal added: amoCRM webhook `leads.add` / subscription event `add_lead`.
- New unsorted item added: amoCRM webhook `unsorted.add` / subscription event `add_unsorted`.
- New incoming message: amoCRM webhook `message.add` / subscription event `add_message`, with fallback support for Digital Pipeline `leads.chat` if amoCRM sends chat activity through that path.

Out of scope for the first release:

- Creating or updating bot clients/orders from amoCRM.
- OAuth/token management for calling amoCRM REST API.
- Admin acknowledgement buttons and repeated escalations.
- Bidirectional sync.

## Public Endpoint

Use a dedicated integration domain:

```text
https://hooks.crmfit.ru/amocrm/webhook?token=$AMOCRM_WEBHOOK_TOKEN
```

The domain should terminate HTTPS in nginx and proxy only `/amocrm/webhook` and `/amocrm/webhook/` to the existing local aiohttp webhook server on `127.0.0.1:8080`.

Do not expose `http://91.200.150.68:8080` as the production webhook URL.

## Runtime Architecture

Extend the existing `notifications.webhook.WahelpWebhookServer` instead of starting a second HTTP server. The class already owns `/wahelp/webhook` and `/onlinepbx/webhook`, so adding `/amocrm/webhook` keeps all inbound integration routes in one aiohttp app and one systemd runtime.

Add these constructor parameters:

- `amocrm_token: str | None`
- `amocrm_handler: Callable[[Mapping[str, Any]], Awaitable[bool]] | None`

Add these routes:

- `POST /amocrm/webhook`
- `POST /amocrm/webhook/`

The handler must:

- accept `application/x-www-form-urlencoded`, matching amoCRM webhook delivery;
- optionally accept JSON only for local diagnostics and tests;
- verify the token from `?token=` or header `X-Amocrm-Token`;
- return `401` for invalid token;
- return `400` for malformed payload;
- return fast `2xx` after the event is accepted and processed locally.

## Persistence

Create schema on startup via `ensure_amocrm_webhook_schema(conn)` in `bot.py`:

```sql
CREATE TABLE IF NOT EXISTS amocrm_webhook_events (
    id              bigserial PRIMARY KEY,
    event_type      text NOT NULL,
    entity_kind     text,
    entity_id       text,
    payload         jsonb NOT NULL,
    handled         boolean NOT NULL DEFAULT false,
    error           text,
    received_at     timestamptz NOT NULL DEFAULT NOW(),
    notified_at     timestamptz
);

CREATE INDEX IF NOT EXISTS idx_amocrm_webhook_events_received
ON amocrm_webhook_events(received_at DESC);

CREATE INDEX IF NOT EXISTS idx_amocrm_webhook_events_type
ON amocrm_webhook_events(event_type, received_at DESC);
```

Store the raw normalized payload before formatting the admin message. This protects diagnostics when amoCRM changes or nests fields differently.

## Event Normalization

amoCRM sends nested form keys, for example keys shaped like `leads[add][0][id]`. Normalize the flat form payload into enough structured data for alerting:

- `event_type`: canonical value such as `leads.add`, `unsorted.add`, `message.add`, or `leads.chat`.
- `entity_kind`: `lead`, `unsorted`, `message`, or `chat`.
- `entity_id`: best available amoCRM id from the payload.
- `payload`: full flat payload as JSON object, preserving all original keys and values.

The first implementation should not depend on perfect deep reconstruction. It should identify the event by key prefixes and extract obvious fields by suffixes such as `[id]`, `[name]`, `[price]`, `[status_id]`, `[pipeline_id]`, `[responsible_user_id]`, `[text]`, `[message]`, `[phone]`.

## Admin Alerts

Send alerts to existing `ADMIN_TG_IDS`. If no admins are configured, log and store the event as unnotified.

Alert tone should be operational and urgent:

```text
🚨 amoCRM: новая входящая заявка

Тип: новое сообщение
Сделка: #123456
Клиент: Иван
Телефон: +7 999 123-45-67
Текст: Хочу уборку завтра
Ссылка: https://$AMOCRM_ACCOUNT_DOMAIN/leads/detail/123456

Нужно ответить или позвонить.
```

Event labels:

- `leads.add`: `новая сделка`
- `unsorted.add`: `новое неразобранное`
- `message.add`: `новое входящее сообщение`
- `leads.chat`: `новый чат/сообщение`

Add env `AMOCRM_ACCOUNT_DOMAIN` to build links. If it is absent, omit the link instead of guessing.

## Configuration

Add to `.env.example`:

```text
AMOCRM_WEBHOOK_TOKEN=your_amocrm_webhook_token
AMOCRM_ACCOUNT_DOMAIN=yourcompany.amocrm.ru
```

The webhook route may exist when `AMOCRM_WEBHOOK_TOKEN` is unset, but production deploy must set the token before registering the URL in amoCRM.

## nginx

Add a dedicated server block for `hooks.crmfit.ru`:

```nginx
server {
    listen 80;
    server_name hooks.crmfit.ru;
    return 301 https://$host$request_uri;
}

server {
    listen 443 ssl;
    server_name hooks.crmfit.ru;

    location /amocrm/webhook {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Certificate provisioning should use the existing server convention. Do not deploy nginx changes until DNS for `hooks.crmfit.ru` points to `91.200.150.68`.

## Testing

Local tests should cover:

- valid form payload for `leads.add` returns `200` and stores `event_type='leads.add'`;
- invalid token returns `401` and stores nothing;
- unknown event returns `200`, stores payload, and does not notify admins;
- notifier failure stores the payload with `handled=false` and an error.

Manual production smoke test after deploy:

```bash
curl -fsS -X POST \
  "https://hooks.crmfit.ru/amocrm/webhook?token=$AMOCRM_WEBHOOK_TOKEN" \
  --data-urlencode "leads[add][0][id]=123456" \
  --data-urlencode "leads[add][0][name]=Тестовая заявка"
```

Expected result:

- HTTP `200`;
- one row in `amocrm_webhook_events`;
- admins receive a Telegram alert.

## Rollout

1. Implement endpoint and storage locally.
2. Add env examples and docs.
3. Commit code changes.
4. Deploy from clean committed state.
5. Configure DNS `hooks.crmfit.ru -> 91.200.150.68`.
6. Add nginx route and certificate.
7. Set production env `AMOCRM_WEBHOOK_TOKEN` and optionally `AMOCRM_ACCOUNT_DOMAIN`.
8. Restart bot.
9. Run curl smoke test.
10. Register webhook URL in amoCRM for `add_lead`, `add_unsorted`, and `add_message` or the matching UI labels.

## References

- amoCRM webhook API: https://www.amocrm.ru/developers/content/crm_platform/webhooks-api
- amoCRM webhook format: https://www.amocrm.ru/developers/content/crm_platform/webhooks-format
- amoCRM Digital Pipeline webhooks: https://www.amocrm.ru/developers/content/digital_pipeline/webhooks
