# amoCRM API Polling Alerts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build API-based amoCRM admin alerts that replace noisy webhooks with filtered notifications for new deals, new unsorted items, and unanswered incoming messages.

**Architecture:** Add a focused `notifications/amocrm_api.py` module for amoCRM HTTP access, normalization, filtering, and polling-cycle logic. Keep PostgreSQL schema, task startup, Telegram delivery, and env wiring in `bot.py`, following the existing bot architecture.

**Tech Stack:** Python 3.10+, aiohttp client, aiogram 3, asyncpg, PostgreSQL, unittest.

---

## File Structure

- Create `notifications/amocrm_api.py`: amoCRM API client, data classes, payload extraction helpers, alert formatting, and pure polling-cycle orchestration helpers.
- Create `tests/test_amocrm_api.py`: pure unit tests for parsing, filtering, accepted-call suppression, pending-message state transitions, and alert formatting.
- Modify `bot.py`: env vars, schema creation, background polling task, DB-backed repository functions, Telegram delivery.
- Modify `.env.example`: new amoCRM API env vars and note that webhook token can be empty when API polling is used.
- Keep `notifications/amocrm.py` and `notifications/webhook.py`: current webhook code remains for diagnostics/backward compatibility but should not be used in production once `AMOCRM_WEBHOOK_TOKEN` is cleared.

---

### Task 1: amoCRM API Domain Models and Extraction Helpers

**Files:**
- Create: `notifications/amocrm_api.py`
- Create: `tests/test_amocrm_api.py`

- [ ] **Step 1: Write failing tests for API payload normalization**

Add this initial test file:

```python
import unittest
from datetime import datetime, timezone

from notifications.amocrm_api import (
    AmoCRMAlert,
    AmoCRMEvent,
    AmoCRMLead,
    build_lead_link,
    extract_contact_phone,
    extract_event_message_id,
    extract_event_type,
    extract_lead_contact_ids,
    is_accepted_call_note,
)


class AmoCRMApiExtractionTests(unittest.TestCase):
    def test_extracts_contact_phone_from_custom_fields(self):
        contact = {
            "id": 10,
            "name": "Иван",
            "custom_fields_values": [
                {
                    "field_code": "PHONE",
                    "values": [{"value": "+7 999 123-45-67", "enum_code": "WORK"}],
                }
            ],
        }

        self.assertEqual(extract_contact_phone(contact), "+7 999 123-45-67")

    def test_extracts_lead_contact_ids_from_embedded_contacts(self):
        lead = {
            "id": 123,
            "_embedded": {
                "contacts": [
                    {"id": 10, "is_main": True},
                    {"id": 11, "is_main": False},
                ]
            },
        }

        self.assertEqual(extract_lead_contact_ids(lead), [10, 11])

    def test_identifies_accepted_call_note(self):
        note = {
            "note_type": "call_in",
            "params": {
                "duration": 35,
                "phone": "+79991234567",
            },
        }

        self.assertTrue(is_accepted_call_note(note, min_duration_sec=20))

    def test_rejects_short_call_note(self):
        note = {"note_type": "call_in", "params": {"duration": 0}}

        self.assertFalse(is_accepted_call_note(note, min_duration_sec=20))

    def test_extracts_event_type_and_message_id(self):
        event = {
            "id": "ev-1",
            "type": "incoming_chat_message",
            "value_after": [
                {"message": {"id": "msg-1", "text": "Хочу уборку"}},
            ],
        }

        self.assertEqual(extract_event_type(event), "incoming_chat_message")
        self.assertEqual(extract_event_message_id(event), "msg-1")

    def test_builds_lead_link(self):
        self.assertEqual(
            build_lead_link("https://raketacleancrm.amocrm.ru", 123),
            "https://raketacleancrm.amocrm.ru/leads/detail/123",
        )


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests and confirm failure**

Run:

```bash
python3.11 -m unittest tests.test_amocrm_api -v
```

Expected: import fails because `notifications.amocrm_api` does not exist.

- [ ] **Step 3: Implement minimal extraction helpers**

Create `notifications/amocrm_api.py` with:

```python
"""amoCRM API polling helpers for admin alerts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping


@dataclass(slots=True, frozen=True)
class AmoCRMEvent:
    event_id: str
    event_type: str
    entity_id: int | None
    entity_type: str | None
    created_at: int
    payload: dict[str, Any]


@dataclass(slots=True, frozen=True)
class AmoCRMLead:
    lead_id: int
    name: str | None
    pipeline_id: int | None
    status_id: int | None
    created_at: int | None
    contact_ids: list[int]
    payload: dict[str, Any]


@dataclass(slots=True, frozen=True)
class AmoCRMAlert:
    alert_type: str
    title: str | None
    lead_id: int | None
    contact_id: int | None
    contact_name: str | None
    phone: str | None
    source: str | None
    text: str | None
    comment: str | None
    link: str | None


def extract_contact_phone(contact: Mapping[str, Any]) -> str | None:
    for field in contact.get("custom_fields_values") or []:
        if not isinstance(field, Mapping):
            continue
        if str(field.get("field_code") or "").upper() != "PHONE":
            continue
        for value in field.get("values") or []:
            if isinstance(value, Mapping) and value.get("value"):
                return str(value["value"]).strip()
    return None


def extract_lead_contact_ids(lead: Mapping[str, Any]) -> list[int]:
    contacts = ((lead.get("_embedded") or {}).get("contacts") or [])
    ids: list[int] = []
    for contact in contacts:
        if not isinstance(contact, Mapping):
            continue
        try:
            ids.append(int(contact["id"]))
        except Exception:
            continue
    return ids


def is_accepted_call_note(note: Mapping[str, Any], *, min_duration_sec: int) -> bool:
    if str(note.get("note_type") or "") != "call_in":
        return False
    params = note.get("params") if isinstance(note.get("params"), Mapping) else {}
    try:
        duration = int(params.get("duration") or 0)
    except Exception:
        duration = 0
    return duration >= min_duration_sec


def extract_event_type(event: Mapping[str, Any]) -> str:
    return str(event.get("type") or "").strip()


def extract_event_message_id(event: Mapping[str, Any]) -> str | None:
    for change in event.get("value_after") or []:
        if not isinstance(change, Mapping):
            continue
        message = change.get("message")
        if isinstance(message, Mapping) and message.get("id"):
            return str(message["id"]).strip()
    return None


def build_lead_link(api_base: str, lead_id: int | str | None) -> str | None:
    if not api_base or not lead_id:
        return None
    return f"{api_base.rstrip('/')}/leads/detail/{lead_id}"
```

- [ ] **Step 4: Run tests and confirm pass**

Run:

```bash
python3.11 -m unittest tests.test_amocrm_api -v
```

Expected: all tests in `AmoCRMApiExtractionTests` pass.

- [ ] **Step 5: Commit task**

```bash
git add notifications/amocrm_api.py tests/test_amocrm_api.py
git commit -m "feat: add amocrm api extraction helpers"
```

---

### Task 2: amoCRM HTTP Client

**Files:**
- Modify: `notifications/amocrm_api.py`
- Modify: `tests/test_amocrm_api.py`

- [ ] **Step 1: Add tests for request construction and error classes**

Append:

```python
from notifications.amocrm_api import AmoCRMAPIAuthError, AmoCRMAPIClient, AmoCRMAPIError, AmoCRMAPIRateLimitError


class FakeHTTPResponse:
    def __init__(self, status, payload):
        self.status = status
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self):
        return self._payload

    async def text(self):
        return str(self._payload)


class FakeHTTPSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, url, *, headers, params, timeout):
        self.calls.append((url, headers, params, timeout))
        return self.response


class AmoCRMApiClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_get_adds_bearer_token_and_base_url(self):
        session = FakeHTTPSession(FakeHTTPResponse(200, {"ok": True}))
        client = AmoCRMAPIClient("https://example.amocrm.ru", "token", session=session)

        result = await client.get("/api/v4/events", params={"limit": 1})

        self.assertEqual(result, {"ok": True})
        url, headers, params, timeout = session.calls[0]
        self.assertEqual(url, "https://example.amocrm.ru/api/v4/events")
        self.assertEqual(headers["Authorization"], "Bearer token")
        self.assertEqual(params, {"limit": 1})

    async def test_get_raises_auth_error_on_401(self):
        session = FakeHTTPSession(FakeHTTPResponse(401, {"detail": "bad token"}))
        client = AmoCRMAPIClient("https://example.amocrm.ru", "token", session=session)

        with self.assertRaises(AmoCRMAPIAuthError):
            await client.get("/api/v4/events")

    async def test_get_raises_rate_limit_error_on_429(self):
        session = FakeHTTPSession(FakeHTTPResponse(429, {"detail": "too many"}))
        client = AmoCRMAPIClient("https://example.amocrm.ru", "token", session=session)

        with self.assertRaises(AmoCRMAPIRateLimitError):
            await client.get("/api/v4/events")
```

- [ ] **Step 2: Run tests and confirm failure**

Run:

```bash
python3.11 -m unittest tests.test_amocrm_api.AmoCRMApiClientTests -v
```

Expected: imports fail for client/error classes.

- [ ] **Step 3: Implement client and errors**

Add to `notifications/amocrm_api.py`:

```python
import aiohttp


class AmoCRMAPIError(RuntimeError):
    def __init__(self, status: int, message: str):
        super().__init__(f"amoCRM API error {status}: {message}")
        self.status = status
        self.message = message


class AmoCRMAPIAuthError(AmoCRMAPIError):
    pass


class AmoCRMAPIRateLimitError(AmoCRMAPIError):
    pass


class AmoCRMAPIClient:
    def __init__(
        self,
        api_base: str,
        token: str,
        *,
        session: aiohttp.ClientSession | Any | None = None,
        timeout_sec: float = 15.0,
    ) -> None:
        self.api_base = api_base.rstrip("/")
        self.token = token.strip()
        self.session = session
        self.timeout_sec = timeout_sec
        self._owns_session = session is None

    async def __aenter__(self) -> "AmoCRMAPIClient":
        if self.session is None:
            self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self._owns_session and self.session is not None:
            await self.session.close()

    async def get(self, path: str, *, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        if self.session is None:
            self.session = aiohttp.ClientSession()
            self._owns_session = True
        url = f"{self.api_base}{path}"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        }
        async with self.session.get(url, headers=headers, params=params or {}, timeout=self.timeout_sec) as resp:
            try:
                payload = await resp.json()
            except Exception:
                payload = {"text": await resp.text()}
            if resp.status == 401:
                raise AmoCRMAPIAuthError(resp.status, str(payload))
            if resp.status == 429:
                raise AmoCRMAPIRateLimitError(resp.status, str(payload))
            if resp.status >= 400:
                raise AmoCRMAPIError(resp.status, str(payload))
            if not isinstance(payload, dict):
                raise AmoCRMAPIError(resp.status, "unexpected non-object response")
            return payload
```

- [ ] **Step 4: Run tests and confirm pass**

Run:

```bash
python3.11 -m unittest tests.test_amocrm_api.AmoCRMApiClientTests -v
```

Expected: client tests pass.

- [ ] **Step 5: Commit task**

```bash
git add notifications/amocrm_api.py tests/test_amocrm_api.py
git commit -m "feat: add amocrm api client"
```

---

### Task 3: Lead, Contact, Notes, Events, and Unsorted Fetchers

**Files:**
- Modify: `notifications/amocrm_api.py`
- Modify: `tests/test_amocrm_api.py`

- [ ] **Step 1: Add tests for fetcher paths and filters**

Append:

```python
class SequenceHTTPSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, *, headers, params, timeout):
        self.calls.append((url, params))
        return self.responses.pop(0)


class AmoCRMFetchersTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_events_uses_type_and_created_at_filters(self):
        session = SequenceHTTPSession([
            FakeHTTPResponse(200, {"_embedded": {"events": [{"id": "ev-1", "type": "lead_added"}]}})
        ])
        client = AmoCRMAPIClient("https://example.amocrm.ru", "token", session=session)

        events = await client.fetch_events(event_types=["lead_added"], created_from=100)

        self.assertEqual(events[0]["id"], "ev-1")
        url, params = session.calls[0]
        self.assertEqual(url, "https://example.amocrm.ru/api/v4/events")
        self.assertEqual(params["filter[type]"], "lead_added")
        self.assertEqual(params["filter[created_at][from]"], 100)

    async def test_fetch_lead_requests_contacts(self):
        session = SequenceHTTPSession([
            FakeHTTPResponse(200, {"id": 123, "_embedded": {"contacts": [{"id": 10}]}})
        ])
        client = AmoCRMAPIClient("https://example.amocrm.ru", "token", session=session)

        lead = await client.fetch_lead(123)

        self.assertEqual(lead["id"], 123)
        url, params = session.calls[0]
        self.assertEqual(url, "https://example.amocrm.ru/api/v4/leads/123")
        self.assertEqual(params["with"], "contacts")

    async def test_fetch_unsorted_filters_pipeline_and_created_from(self):
        session = SequenceHTTPSession([
            FakeHTTPResponse(200, {"_embedded": {"unsorted": [{"uid": "u-1"}]}})
        ])
        client = AmoCRMAPIClient("https://example.amocrm.ru", "token", session=session)

        items = await client.fetch_unsorted(pipeline_id=55, created_from=100)

        self.assertEqual(items[0]["uid"], "u-1")
        url, params = session.calls[0]
        self.assertEqual(url, "https://example.amocrm.ru/api/v4/leads/unsorted")
        self.assertEqual(params["filter[pipeline_id]"], 55)
        self.assertEqual(params["filter[created_at][from]"], 100)
```

- [ ] **Step 2: Run tests and confirm failure**

Run:

```bash
python3.11 -m unittest tests.test_amocrm_api.AmoCRMFetchersTests -v
```

Expected: methods `fetch_events`, `fetch_lead`, `fetch_unsorted` are missing.

- [ ] **Step 3: Implement fetchers**

Add methods to `AmoCRMAPIClient`:

```python
    async def fetch_events(self, *, event_types: list[str], created_from: int, limit: int = 100) -> list[dict[str, Any]]:
        payload = await self.get(
            "/api/v4/events",
            params={
                "filter[type]": ",".join(event_types),
                "filter[created_at][from]": created_from,
                "limit": limit,
            },
        )
        return list(((payload.get("_embedded") or {}).get("events") or []))

    async def fetch_lead(self, lead_id: int) -> dict[str, Any]:
        return await self.get(f"/api/v4/leads/{lead_id}", params={"with": "contacts"})

    async def fetch_contact(self, contact_id: int) -> dict[str, Any]:
        return await self.get(f"/api/v4/contacts/{contact_id}")

    async def fetch_lead_notes(self, lead_id: int) -> list[dict[str, Any]]:
        payload = await self.get(f"/api/v4/leads/{lead_id}/notes", params={"limit": 100})
        return list(((payload.get("_embedded") or {}).get("notes") or []))

    async def fetch_unsorted(self, *, pipeline_id: int, created_from: int, limit: int = 100) -> list[dict[str, Any]]:
        payload = await self.get(
            "/api/v4/leads/unsorted",
            params={
                "filter[pipeline_id]": pipeline_id,
                "filter[created_at][from]": created_from,
                "limit": limit,
            },
        )
        return list(((payload.get("_embedded") or {}).get("unsorted") or []))
```

- [ ] **Step 4: Run tests and confirm pass**

Run:

```bash
python3.11 -m unittest tests.test_amocrm_api.AmoCRMFetchersTests -v
```

Expected: fetcher tests pass.

- [ ] **Step 5: Commit task**

```bash
git add notifications/amocrm_api.py tests/test_amocrm_api.py
git commit -m "feat: add amocrm api fetchers"
```

---

### Task 4: Alert Formatting and Business Filters

**Files:**
- Modify: `notifications/amocrm_api.py`
- Modify: `tests/test_amocrm_api.py`

- [ ] **Step 1: Add tests for alert formatting and lead-call suppression**

Append:

```python
from notifications.amocrm_api import format_amocrm_api_alert, should_skip_new_lead_alert


class AmoCRMAlertRulesTests(unittest.TestCase):
    def test_skips_new_lead_when_new_lead_status_and_accepted_call(self):
        lead = AmoCRMLead(
            lead_id=123,
            name="Входящий звонок",
            pipeline_id=55,
            status_id=777,
            created_at=100,
            contact_ids=[10],
            payload={},
        )
        notes = [{"note_type": "call_in", "params": {"duration": 30}}]

        self.assertTrue(
            should_skip_new_lead_alert(
                lead,
                target_pipeline_id=55,
                new_lead_status_id=777,
                notes=notes,
                accepted_call_min_duration_sec=20,
            )
        )

    def test_does_not_skip_non_call_new_lead(self):
        lead = AmoCRMLead(
            lead_id=123,
            name="Заявка с сайта",
            pipeline_id=55,
            status_id=777,
            created_at=100,
            contact_ids=[10],
            payload={},
        )

        self.assertFalse(
            should_skip_new_lead_alert(
                lead,
                target_pipeline_id=55,
                new_lead_status_id=777,
                notes=[],
                accepted_call_min_duration_sec=20,
            )
        )

    def test_formats_unanswered_message_alert(self):
        alert = AmoCRMAlert(
            alert_type="unanswered_message",
            title="Открытая сделка",
            lead_id=123,
            contact_id=10,
            contact_name="Иван",
            phone="+79991234567",
            source="WhatsApp",
            text="Хочу уборку",
            comment=None,
            link="https://example.amocrm.ru/leads/detail/123",
        )

        text = format_amocrm_api_alert(alert)

        self.assertIn("Тип: новое входящее сообщение без ответа 10 минут", text)
        self.assertIn("Сделка: #123", text)
        self.assertIn("Клиент: Иван", text)
        self.assertIn("Телефон: +79991234567", text)
        self.assertIn("Текст: Хочу уборку", text)
```

- [ ] **Step 2: Run tests and confirm failure**

Run:

```bash
python3.11 -m unittest tests.test_amocrm_api.AmoCRMAlertRulesTests -v
```

Expected: rule and formatter functions are missing.

- [ ] **Step 3: Implement rule and formatter**

Add:

```python
def should_skip_new_lead_alert(
    lead: AmoCRMLead,
    *,
    target_pipeline_id: int,
    new_lead_status_id: int,
    notes: list[Mapping[str, Any]],
    accepted_call_min_duration_sec: int,
) -> bool:
    if lead.pipeline_id != target_pipeline_id:
        return True
    if lead.status_id != new_lead_status_id:
        return False
    return any(is_accepted_call_note(note, min_duration_sec=accepted_call_min_duration_sec) for note in notes)


def format_amocrm_api_alert(alert: AmoCRMAlert) -> str:
    labels = {
        "new_lead": "новая сделка",
        "new_unsorted": "новое неразобранное",
        "unanswered_message": "новое входящее сообщение без ответа 10 минут",
    }
    lines = [
        "🚨 amoCRM: новая входящая заявка",
        "",
        f"Тип: {labels.get(alert.alert_type, alert.alert_type)}",
    ]
    if alert.lead_id:
        lines.append(f"Сделка: #{alert.lead_id}")
    if alert.title:
        lines.append(f"Название: {alert.title}")
    if alert.contact_name:
        lines.append(f"Клиент: {alert.contact_name}")
    if alert.phone:
        lines.append(f"Телефон: {alert.phone}")
    if alert.source:
        lines.append(f"Источник: {alert.source}")
    if alert.text:
        lines.append(f"Текст: {alert.text[:700]}")
    if alert.comment:
        lines.append(f"Комментарий: {alert.comment[:700]}")
    if alert.link:
        lines.append(f"Ссылка: {alert.link}")
    lines.extend(["", "Нужно ответить или позвонить."])
    return "\n".join(lines)
```

- [ ] **Step 4: Run tests and confirm pass**

Run:

```bash
python3.11 -m unittest tests.test_amocrm_api.AmoCRMAlertRulesTests -v
```

Expected: alert rule tests pass.

- [ ] **Step 5: Commit task**

```bash
git add notifications/amocrm_api.py tests/test_amocrm_api.py
git commit -m "feat: add amocrm api alert rules"
```

---

### Task 5: Database Schema and Repository Functions

**Files:**
- Modify: `bot.py`

- [ ] **Step 1: Add schema function near `ensure_amocrm_webhook_schema`**

Add:

```python
async def ensure_amocrm_api_schema(conn: asyncpg.Connection) -> None:
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS amocrm_api_state (
            stream text PRIMARY KEY,
            cursor_created_at integer NOT NULL DEFAULT 0,
            updated_at timestamptz NOT NULL DEFAULT NOW()
        );
        """
    )
    await conn.execute(
        """
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
        """
    )
    await conn.execute(
        """
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
        """
    )
    await conn.execute(
        """
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
        """
    )
    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_amocrm_pending_incoming_due
        ON amocrm_pending_incoming(status, due_at);
        """
    )
```

- [ ] **Step 2: Wire schema creation in `main()`**

Add after `await ensure_amocrm_webhook_schema(_conn)`:

```python
        await ensure_amocrm_api_schema(_conn)
```

- [ ] **Step 3: Add cursor helper functions near the schema**

Add:

```python
async def _amocrm_get_cursor(conn: asyncpg.Connection, stream: str, default_created_at: int) -> int:
    value = await conn.fetchval(
        "SELECT cursor_created_at FROM amocrm_api_state WHERE stream=$1",
        stream,
    )
    if value is None:
        await conn.execute(
            """
            INSERT INTO amocrm_api_state (stream, cursor_created_at)
            VALUES ($1, $2)
            ON CONFLICT (stream) DO NOTHING
            """,
            stream,
            default_created_at,
        )
        return default_created_at
    return int(value)


async def _amocrm_set_cursor(conn: asyncpg.Connection, stream: str, cursor_created_at: int) -> None:
    await conn.execute(
        """
        INSERT INTO amocrm_api_state (stream, cursor_created_at, updated_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (stream) DO UPDATE
        SET cursor_created_at=EXCLUDED.cursor_created_at,
            updated_at=NOW()
        """,
        stream,
        cursor_created_at,
    )
```

- [ ] **Step 4: Run compile check**

Run:

```bash
python3.11 -m py_compile bot.py
```

Expected: exits `0`.

- [ ] **Step 5: Commit task**

```bash
git add bot.py
git commit -m "feat: add amocrm api polling schema"
```

---

### Task 6: New Lead and Unsorted Polling Cycle

**Files:**
- Modify: `notifications/amocrm_api.py`
- Modify: `bot.py`
- Modify: `tests/test_amocrm_api.py`

- [ ] **Step 1: Add tests for converting leads and unsorted items into alerts**

Append:

```python
from notifications.amocrm_api import build_new_lead_alert, build_unsorted_alert, normalize_lead


class AmoCRMPollingAlertBuildTests(unittest.TestCase):
    def test_normalize_lead_keeps_pipeline_status_and_contacts(self):
        lead = normalize_lead(
            {
                "id": 123,
                "name": "Заявка с сайта",
                "pipeline_id": 55,
                "status_id": 777,
                "created_at": 1710000000,
                "_embedded": {"contacts": [{"id": 10}]},
            }
        )

        self.assertEqual(lead.lead_id, 123)
        self.assertEqual(lead.pipeline_id, 55)
        self.assertEqual(lead.status_id, 777)
        self.assertEqual(lead.contact_ids, [10])

    def test_build_new_lead_alert_uses_contact_phone(self):
        lead = normalize_lead(
            {
                "id": 123,
                "name": "Заявка с сайта",
                "pipeline_id": 55,
                "status_id": 777,
                "_embedded": {"contacts": [{"id": 10}]},
            }
        )
        contact = {
            "id": 10,
            "name": "Иван",
            "custom_fields_values": [
                {"field_code": "PHONE", "values": [{"value": "+79991234567"}]},
            ],
        }

        alert = build_new_lead_alert(lead, contact=contact, api_base="https://example.amocrm.ru")

        self.assertEqual(alert.alert_type, "new_lead")
        self.assertEqual(alert.lead_id, 123)
        self.assertEqual(alert.contact_name, "Иван")
        self.assertEqual(alert.phone, "+79991234567")

    def test_build_unsorted_alert_uses_uid_and_phone(self):
        item = {
            "uid": "u-1",
            "pipeline_id": 55,
            "source_name": "SIP",
            "_embedded": {"leads": [{"id": 123}]},
            "metadata": {"phone": "+79991234567"},
        }

        alert = build_unsorted_alert(item, api_base="https://example.amocrm.ru")

        self.assertEqual(alert.alert_type, "new_unsorted")
        self.assertEqual(alert.lead_id, 123)
        self.assertEqual(alert.phone, "+79991234567")
        self.assertEqual(alert.source, "SIP")
```

- [ ] **Step 2: Implement alert builders**

Add to `notifications/amocrm_api.py`:

```python
def normalize_lead(payload: Mapping[str, Any]) -> AmoCRMLead:
    return AmoCRMLead(
        lead_id=int(payload["id"]),
        name=str(payload.get("name") or "").strip() or None,
        pipeline_id=int(payload["pipeline_id"]) if payload.get("pipeline_id") is not None else None,
        status_id=int(payload["status_id"]) if payload.get("status_id") is not None else None,
        created_at=int(payload["created_at"]) if payload.get("created_at") is not None else None,
        contact_ids=extract_lead_contact_ids(payload),
        payload=dict(payload),
    )


def build_new_lead_alert(
    lead: AmoCRMLead,
    *,
    contact: Mapping[str, Any] | None,
    api_base: str,
) -> AmoCRMAlert:
    return AmoCRMAlert(
        alert_type="new_lead",
        title=lead.name,
        lead_id=lead.lead_id,
        contact_id=int(contact["id"]) if contact and contact.get("id") is not None else None,
        contact_name=str(contact.get("name") or "").strip() if contact else None,
        phone=extract_contact_phone(contact) if contact else None,
        source=None,
        text=None,
        comment=None,
        link=build_lead_link(api_base, lead.lead_id),
    )


def build_unsorted_alert(item: Mapping[str, Any], *, api_base: str) -> AmoCRMAlert:
    embedded = item.get("_embedded") if isinstance(item.get("_embedded"), Mapping) else {}
    leads = embedded.get("leads") or []
    lead_id = None
    if leads and isinstance(leads[0], Mapping) and leads[0].get("id") is not None:
        lead_id = int(leads[0]["id"])
    metadata = item.get("metadata") if isinstance(item.get("metadata"), Mapping) else {}
    return AmoCRMAlert(
        alert_type="new_unsorted",
        title=str(item.get("name") or item.get("source_name") or "").strip() or None,
        lead_id=lead_id,
        contact_id=None,
        contact_name=str(metadata.get("from") or metadata.get("name") or "").strip() or None,
        phone=str(metadata.get("phone") or "").strip() or None,
        source=str(item.get("source_name") or item.get("category") or "").strip() or None,
        text=str(metadata.get("text") or "").strip() or None,
        comment=str(item.get("uid") or "").strip() or None,
        link=build_lead_link(api_base, lead_id),
    )
```

- [ ] **Step 3: Run tests**

Run:

```bash
python3.11 -m unittest tests.test_amocrm_api.AmoCRMPollingAlertBuildTests -v
```

Expected: tests pass.

- [ ] **Step 4: Implement DB-backed lead/unsorted cycle in `bot.py`**

Add functions:

```python
async def _notify_admins_amocrm_api_alert(alert: AmoCRMAlert) -> bool:
    if not ADMIN_TG_IDS:
        return False
    text = format_amocrm_api_alert(alert)
    sent = 0
    for admin_id in ADMIN_TG_IDS:
        try:
            await bot.send_message(admin_id, text, disable_web_page_preview=True)
            sent += 1
        except Exception as exc:
            logger.warning("Failed to notify admin %s about amoCRM API alert: %s", admin_id, exc)
    return sent > 0
```

Then add `_amocrm_poll_new_leads_once(client)` and `_amocrm_poll_unsorted_once(client)` that:

```python
# Required behavior:
# - read cursor with _amocrm_get_cursor()
# - fetch API records
# - insert each event/uid with ON CONFLICT DO NOTHING before notifying
# - skip already-seen rows when INSERT returns no row
# - enrich lead/contact/notes before alerting
# - write action='ignored' for accepted-call lead suppression
# - write action='notified' and notified_at=NOW() only after Telegram send succeeds
# - advance cursor only after the API fetch and per-record loop completes
```

- [ ] **Step 5: Run compile and unit tests**

Run:

```bash
python3.11 -m unittest tests.test_amocrm_api -v
python3.11 -m py_compile notifications/amocrm_api.py bot.py
```

Expected: tests pass and compile exits `0`.

- [ ] **Step 6: Commit task**

```bash
git add notifications/amocrm_api.py tests/test_amocrm_api.py bot.py
git commit -m "feat: poll amocrm leads and unsorted"
```

---

### Task 7: Incoming and Outgoing Chat Message SLA

**Files:**
- Modify: `notifications/amocrm_api.py`
- Modify: `bot.py`
- Modify: `tests/test_amocrm_api.py`

- [ ] **Step 1: Add tests for pending-message identity matching**

Append:

```python
from notifications.amocrm_api import build_unanswered_message_alert, extract_event_identity


class AmoCRMIncomingMessageTests(unittest.TestCase):
    def test_extract_event_identity_from_chat_event(self):
        event = {
            "id": "ev-1",
            "type": "incoming_chat_message",
            "entity_id": 10,
            "entity_type": "contact",
            "created_at": 1710000000,
            "value_after": [
                {
                    "message": {"id": "msg-1", "text": "Хочу уборку"},
                    "talk": {"id": "talk-1"},
                    "lead": {"id": 123},
                    "contact": {"id": 10},
                }
            ],
        }

        identity = extract_event_identity(event)

        self.assertEqual(identity["event_id"], "ev-1")
        self.assertEqual(identity["message_id"], "msg-1")
        self.assertEqual(identity["lead_id"], 123)
        self.assertEqual(identity["contact_id"], 10)
        self.assertEqual(identity["talk_id"], "talk-1")
        self.assertEqual(identity["text"], "Хочу уборку")

    def test_build_unanswered_message_alert(self):
        alert = build_unanswered_message_alert(
            lead={"id": 123, "name": "Открытая сделка"},
            contact={
                "id": 10,
                "name": "Иван",
                "custom_fields_values": [{"field_code": "PHONE", "values": [{"value": "+79991234567"}]}],
            },
            text="Хочу уборку",
            api_base="https://example.amocrm.ru",
        )

        self.assertEqual(alert.alert_type, "unanswered_message")
        self.assertEqual(alert.lead_id, 123)
        self.assertEqual(alert.contact_name, "Иван")
        self.assertEqual(alert.phone, "+79991234567")
```

- [ ] **Step 2: Implement identity extraction and unanswered alert builder**

Add:

```python
def extract_event_identity(event: Mapping[str, Any]) -> dict[str, Any]:
    identity: dict[str, Any] = {
        "event_id": str(event.get("id") or ""),
        "event_type": extract_event_type(event),
        "created_at": int(event.get("created_at") or 0),
        "message_id": extract_event_message_id(event),
        "lead_id": None,
        "contact_id": None,
        "talk_id": None,
        "text": None,
    }
    for change in event.get("value_after") or []:
        if not isinstance(change, Mapping):
            continue
        for key, target in (("lead", "lead_id"), ("contact", "contact_id"), ("talk", "talk_id")):
            value = change.get(key)
            if isinstance(value, Mapping) and value.get("id") is not None:
                identity[target] = value["id"]
        message = change.get("message")
        if isinstance(message, Mapping):
            identity["text"] = str(message.get("text") or message.get("message") or "").strip() or None
    if identity["contact_id"] is None and str(event.get("entity_type") or "") == "contact" and event.get("entity_id"):
        identity["contact_id"] = int(event["entity_id"])
    return identity


def build_unanswered_message_alert(
    *,
    lead: Mapping[str, Any] | None,
    contact: Mapping[str, Any] | None,
    text: str | None,
    api_base: str,
) -> AmoCRMAlert:
    lead_id = int(lead["id"]) if lead and lead.get("id") is not None else None
    return AmoCRMAlert(
        alert_type="unanswered_message",
        title=str(lead.get("name") or "").strip() if lead else None,
        lead_id=lead_id,
        contact_id=int(contact["id"]) if contact and contact.get("id") is not None else None,
        contact_name=str(contact.get("name") or "").strip() if contact else None,
        phone=extract_contact_phone(contact) if contact else None,
        source=None,
        text=text,
        comment=None,
        link=build_lead_link(api_base, lead_id),
    )
```

- [ ] **Step 3: Implement DB-backed message cycle in `bot.py`**

Add `_amocrm_poll_chat_events_once(client)` that:

```python
# Required behavior:
# - fetch events with filter[type]=incoming_chat_message,outgoing_chat_message
# - insert every raw event into amocrm_api_events with ON CONFLICT DO NOTHING
# - for incoming: insert into amocrm_pending_incoming with due_at derived from created_at + delay
# - for outgoing: update matching pending rows to status='answered'
# - matching priority: talk_id first, then lead_id, then contact_id
# - only keep incoming rows where resolved lead pipeline matches AMOCRM_PIPELINE_ID
# - if no lead is resolved but contact has open target-pipeline lead, use that lead
```

Add `_amocrm_notify_due_unanswered_once(client)` that:

```python
# Required behavior:
# - select pending rows where status='pending' and due_at <= NOW()
# - enrich lead/contact if ids exist
# - send alert with build_unanswered_message_alert()
# - update row to status='notified', notified_at=NOW() after successful Telegram send
# - store error text if enrichment or Telegram send fails
```

- [ ] **Step 4: Run tests and compile**

Run:

```bash
python3.11 -m unittest tests.test_amocrm_api.AmoCRMIncomingMessageTests -v
python3.11 -m py_compile notifications/amocrm_api.py bot.py
```

Expected: tests pass and compile exits `0`.

- [ ] **Step 5: Commit task**

```bash
git add notifications/amocrm_api.py tests/test_amocrm_api.py bot.py
git commit -m "feat: alert on unanswered amocrm messages"
```

---

### Task 8: Background Task, Env Wiring, and Disable Webhook in Production

**Files:**
- Modify: `bot.py`
- Modify: `.env.example`

- [ ] **Step 1: Add env vars in `bot.py`**

Near existing amoCRM webhook env vars add:

```python
AMOCRM_API_BASE = (os.getenv("AMOCRM_API_BASE") or "").strip().rstrip("/")
AMOCRM_API_TOKEN = (os.getenv("AMOCRM_API_TOKEN") or "").strip()
AMOCRM_PIPELINE_ID = _to_int(os.getenv("AMOCRM_PIPELINE_ID"), 0)
AMOCRM_NEW_LEAD_STATUS_ID = _to_int(os.getenv("AMOCRM_NEW_LEAD_STATUS_ID"), 0)
AMOCRM_POLL_INTERVAL_SEC = max(10, _to_int(os.getenv("AMOCRM_POLL_INTERVAL_SEC"), 30))
AMOCRM_UNANSWERED_DELAY_SEC = max(60, _to_int(os.getenv("AMOCRM_UNANSWERED_DELAY_SEC"), 600))
AMOCRM_ACCEPTED_CALL_MIN_DURATION_SEC = max(1, _to_int(os.getenv("AMOCRM_ACCEPTED_CALL_MIN_DURATION_SEC"), 20))
AMOCRM_LOOKBACK_MINUTES = max(1, _to_int(os.getenv("AMOCRM_LOOKBACK_MINUTES"), 30))
```

If `_to_int` is defined later in the file, place these assignments after `_to_int` or add a tiny `_env_int()` helper above env parsing.

- [ ] **Step 2: Add global task variable**

Extend the existing global task list:

```python
amocrm_api_task = None
```

And include it in `main()` global declaration.

- [ ] **Step 3: Add periodic loop**

Add:

```python
def _amocrm_api_enabled() -> bool:
    return bool(AMOCRM_API_BASE and AMOCRM_API_TOKEN and AMOCRM_PIPELINE_ID)


async def amocrm_api_polling_loop() -> None:
    if not _amocrm_api_enabled():
        logger.info("amoCRM API polling disabled")
        return
    logger.info("amoCRM API polling enabled for pipeline %s", AMOCRM_PIPELINE_ID)
    async with AmoCRMAPIClient(AMOCRM_API_BASE, AMOCRM_API_TOKEN) as client:
        while True:
            try:
                await _amocrm_poll_new_leads_once(client)
                await _amocrm_poll_unsorted_once(client)
                await _amocrm_poll_chat_events_once(client)
                await _amocrm_notify_due_unanswered_once(client)
            except AmoCRMAPIAuthError as exc:
                logger.error("amoCRM API auth failed; polling stopped: %s", exc)
                return
            except AmoCRMAPIRateLimitError as exc:
                logger.warning("amoCRM API rate limited; next cycle will retry: %s", exc)
            except Exception as exc:
                logger.exception("amoCRM API polling cycle failed: %s", exc)
            await asyncio.sleep(AMOCRM_POLL_INTERVAL_SEC)
```

- [ ] **Step 4: Start task in `main()`**

After other background tasks:

```python
    if amocrm_api_task is None and _amocrm_api_enabled():
        amocrm_api_task = asyncio.create_task(amocrm_api_polling_loop())
```

In shutdown:

```python
        if amocrm_api_task is not None:
            amocrm_api_task.cancel()
```

- [ ] **Step 5: Update `.env.example`**

Replace the amoCRM block with:

```text
# amoCRM API alerts. Leave AMOCRM_WEBHOOK_TOKEN empty when API polling is used.
AMOCRM_WEBHOOK_TOKEN=
AMOCRM_ACCOUNT_DOMAIN=yourcompany.amocrm.ru
AMOCRM_API_BASE=https://yourcompany.amocrm.ru
AMOCRM_API_TOKEN=
AMOCRM_PIPELINE_ID=
AMOCRM_NEW_LEAD_STATUS_ID=
AMOCRM_POLL_INTERVAL_SEC=30
AMOCRM_UNANSWERED_DELAY_SEC=600
AMOCRM_ACCEPTED_CALL_MIN_DURATION_SEC=20
AMOCRM_LOOKBACK_MINUTES=30
```

- [ ] **Step 6: Run verification**

Run:

```bash
python3.11 -m unittest tests.test_amocrm tests.test_amocrm_webhook_route tests.test_amocrm_api -v
python3.11 -m py_compile notifications/amocrm.py notifications/amocrm_api.py notifications/webhook.py bot.py
```

Expected: all tests pass and compile exits `0`.

- [ ] **Step 7: Commit task**

```bash
git add bot.py .env.example
git commit -m "feat: wire amocrm api polling"
```

---

### Task 9: Production Configuration and Smoke Verification

**Files:**
- Modify: central context files after successful deploy:
  - `/Users/evgenijpastusenko/Projects/agent1/project_ai_context/tgbot-v1/AGENT_STATE.md`
  - `/Users/evgenijpastusenko/Projects/agent1/project_ai_context/tgbot-v1/SESSION_LOG.md`

- [ ] **Step 1: Push committed code**

Run:

```bash
git status --short
git push origin main
```

Expected: `git status --short` is clean before push.

- [ ] **Step 2: Set production env safely**

On production, set:

```text
AMOCRM_WEBHOOK_TOKEN=
AMOCRM_API_BASE=https://raketacleancrm.amocrm.ru
AMOCRM_API_TOKEN=<long-lived token from amoCRM UI>
AMOCRM_PIPELINE_ID=<target pipeline id>
AMOCRM_NEW_LEAD_STATUS_ID=<Новый лид status id>
AMOCRM_POLL_INTERVAL_SEC=30
AMOCRM_UNANSWERED_DELAY_SEC=600
AMOCRM_ACCEPTED_CALL_MIN_DURATION_SEC=20
AMOCRM_LOOKBACK_MINUTES=30
```

Do not paste `AMOCRM_API_TOKEN` into chat or commit it.

- [ ] **Step 3: Deploy and restart**

Run on production:

```bash
cd /opt/telegram-bot
git pull --ff-only origin main
.venv/bin/python -m py_compile notifications/amocrm.py notifications/amocrm_api.py notifications/webhook.py bot.py
sudo -n systemctl restart telegram-bot.service
sudo -n systemctl status telegram-bot.service
```

Expected: service is `active (running)`.

- [ ] **Step 4: Verify startup logs**

Run:

```bash
journalctl -u telegram-bot.service -n 100 --no-pager | grep -E "amoCRM API polling|auth failed|polling cycle failed"
```

Expected: log contains `amoCRM API polling enabled for pipeline ...` and no auth failures.

- [ ] **Step 5: Run live business smoke tests**

Perform in amoCRM:

```text
1. Create a non-call deal in the target pipeline. Expected: Telegram alert.
2. Create/receive an accepted inbound call that creates a deal in "Новый лид". Expected: no new-deal alert.
3. Create a missed-call or message unsorted item in the target pipeline. Expected: Telegram alert.
4. Send an incoming chat message from an existing client and do not answer for 10 minutes. Expected: delayed Telegram alert.
5. Send another incoming chat message and answer within 10 minutes. Expected: no delayed alert.
```

- [ ] **Step 6: Inspect DB state**

Run:

```bash
psql "$DB_DSN" -P pager=off -c "SELECT stream, cursor_created_at, updated_at FROM amocrm_api_state ORDER BY stream;"
psql "$DB_DSN" -P pager=off -c "SELECT event_type, action, count(*) FROM amocrm_api_events GROUP BY 1,2 ORDER BY 1,2;"
psql "$DB_DSN" -P pager=off -c "SELECT status, count(*) FROM amocrm_pending_incoming GROUP BY 1 ORDER BY 1;"
```

Expected: cursors advance, events are deduplicated, pending rows move to `answered` or `notified`.

- [ ] **Step 7: Update central context**

Record:

```text
- amoCRM webhook alerts disabled in production by clearing AMOCRM_WEBHOOK_TOKEN.
- amoCRM API polling enabled with long-lived token, target pipeline id, and "Новый лид" status id.
- New alerts now use API filters for new deals, unsorted items, and unanswered messages after 10 minutes.
```

- [ ] **Step 8: Commit context update if files are inside current repo**

If central context files are tracked from this repo, commit them. If they are external context files only, leave project git clean and mention the external update in the final report.

---

## Self-Review

- Spec coverage: the plan covers long-lived token config, one-pipeline filtering, new leads, unsorted items, delayed unanswered messages, accepted-call suppression, webhook disablement, persistence, tests, and production verification.
- Placeholder scan: no task depends on an undefined "later" step; the only angle-bracket values are production secrets/ids that must be supplied from amoCRM and must not be committed.
- Type consistency: `AmoCRMAPIClient`, `AmoCRMAlert`, `AmoCRMLead`, and helper names are introduced before later tasks use them.
