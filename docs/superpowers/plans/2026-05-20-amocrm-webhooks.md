# amoCRM Webhooks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a secure amoCRM webhook endpoint that alerts admins about new deals, new unsorted items, and new incoming messages.

**Architecture:** Reuse the existing aiohttp webhook server and add `/amocrm/webhook` routes. Keep parsing and alert formatting in a focused `notifications/amocrm.py` module with unit tests; keep DB persistence and Telegram delivery in `bot.py`.

**Tech Stack:** Python 3.10+, aiohttp, aiogram 3, asyncpg, PostgreSQL, unittest.

---

### Task 1: Parser and Formatter

**Files:**
- Create: `notifications/amocrm.py`
- Create: `tests/test_amocrm.py`

- [ ] Write failing unit tests for `leads.add`, `unsorted.add`, `message.add`, `leads.chat`, and unknown payloads.
- [ ] Run `python3 -m unittest tests.test_amocrm -v` and confirm imports fail because `notifications.amocrm` does not exist.
- [ ] Implement `AmoCRMEvent`, `normalize_amocrm_payload()`, `format_amocrm_admin_alert()`.
- [ ] Run `python3 -m unittest tests.test_amocrm -v` and confirm all tests pass.

### Task 2: HTTP Route

**Files:**
- Modify: `notifications/webhook.py`

- [ ] Add constructor params `amocrm_token` and `amocrm_handler`.
- [ ] Register `POST /amocrm/webhook` and `POST /amocrm/webhook/`.
- [ ] Parse form payloads and optional JSON diagnostics payloads.
- [ ] Verify `?token=` or `X-Amocrm-Token`.
- [ ] Return `401` on bad token, `400` on malformed payload, `200` after handler execution.

### Task 3: Bot Handler and Schema

**Files:**
- Modify: `bot.py`
- Modify: `.env.example`

- [ ] Add env `AMOCRM_WEBHOOK_TOKEN` and `AMOCRM_ACCOUNT_DOMAIN`.
- [ ] Add `ensure_amocrm_webhook_schema(conn)`.
- [ ] Add `handle_amocrm_webhook(payload)`.
- [ ] Insert every received payload into `amocrm_webhook_events`.
- [ ] Send admin alerts only for supported events.
- [ ] Pass amoCRM token and handler into `start_wahelp_webhook()`.

### Task 4: Verification and Commit

**Files:**
- Modify: central context files only at session close if implementation is complete.

- [ ] Run `python3 -m unittest tests.test_amocrm -v`.
- [ ] Run `python3 -m py_compile notifications/amocrm.py notifications/webhook.py bot.py`.
- [ ] Run `git status --short`.
- [ ] Commit implementation as `feat: add amocrm webhook alerts`.
