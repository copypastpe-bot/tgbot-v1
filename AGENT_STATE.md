# AGENT_STATE

project: tgbot-v1
last_updated: 2026-04-13
updated_by: agent1
status: active
confidence: high

## Purpose

`tgbot-v1` is an internal Telegram bot for a cleaning company: orders, clients, loyalty, notifications, promo broadcasts, payroll, and cashflow.

## Current State

The project is described as fully working, with `bot.py` as the real runtime core. The repository has a large `bot.py` runtime, Postgres-oriented migrations under `app/migrations/`, notification modules under `notifications/`, Wahelp integration helpers under `crm/`, and operational scripts under `scripts/`. The next planned product direction is Google Calendar exchange, documented in `technical_task_google_calendar_integration.md`.

## Active Focus

Prepare for Google Calendar integration while preserving the currently working admin, master, notification, payroll, and cashflow flows.

## Known Risks

- No active blocker is currently known.
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
