# AGENT_STATE

project: tgbot-v1
last_updated: 2026-04-13
updated_by: codex
status: active
confidence: high

## Purpose

`tgbot-v1` is an internal Telegram bot for a cleaning company: orders, clients, loyalty, notifications, promo broadcasts, payroll, and cashflow.

## Current State

The project is described as fully working, with `bot.py` as the real runtime core. The repository has a large `bot.py` runtime, Postgres-oriented migrations under `app/migrations/`, notification modules under `notifications/`, Wahelp integration helpers under `crm/`, and operational scripts under `scripts/`. A shared `service_heartbeats` table plus a periodic checker in `bot.py` now monitor the companion client Telegram bot and notify admins when its heartbeat or Telegram probe goes stale.

## Active Focus

Preserve current admin, master, notification, payroll, and cashflow flows while adding operational safety around the companion client bot and preparing future Google Calendar work.

## Known Risks

- The new alerting depends on both repos sharing the same Postgres database and both updated runtimes being deployed.
- The repository is old and should still be reviewed for living code versus historical tail before broader changes.
- Notification behavior depends on both code and `docs/notification_rules.json`.

## Source Of Truth

- `README.md`
- `project.md`
- `bot.py`
- `notifications/`
- `docs/notification_rules.json`
- `crm/wahelp_service.py`
- `app/migrations/`
- `technical_task_google_calendar_integration.md`
