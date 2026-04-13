# CLAUDE

Read this before working in the project.

## Goal

Maintain the internal cleaning-company Telegram bot without destabilizing the currently working order, loyalty, payroll, cashflow, notification, and admin flows. The next strategic direction is Google Calendar integration.

## Read Order

1. `AGENT_STATE.md`
2. recent entries in `SESSION_LOG.md`
3. `project.md`
4. `technical_task_google_calendar_integration.md`
5. `README.md`
6. `bot.py`
7. `docs/notification_rules.json`
8. `notifications/`
9. `crm/wahelp_service.py`

## Key Sources

- `bot.py`
- `project.md`
- `README.md`
- `notifications/`
- `docs/notification_rules.json`
- `crm/`
- `app/migrations/`
- `scripts/`
- `technical_task_google_calendar_integration.md`

## Working Rules

- Treat `bot.py` as the main runtime source unless a refactor clearly changes the entrypoint.
- Keep notification behavior consistent across `notifications/` code and `docs/notification_rules.json`.
- Check migration impact before changing order, bonus, or payroll-related data flows.
- Record environment-sensitive changes clearly because webhook and token settings matter to runtime behavior.
- This is an old repository: verify whether a directory is still live before editing it.

## Git Hygiene

- Run `git status --short` before editing, before committing, and before deploy.
- Commit completed logical steps in small, focused commits.
- Keep `bot.py` fixes, notification-rule changes, and operational script changes separated when possible.
- Do not deploy from a dirty worktree.

## Deploy Rules

- Deploy only from committed and pushed state.
- If the task affects prod runtime, verify the relevant runbook in `project.md`, `scripts/`, or other project docs before deployment.
- If no trusted deploy sequence is documented for the task, stop and document that gap instead of guessing.

## End Of Session Requirements

Before ending the session:
1. run `git status --short`;
2. commit completed work in one or more small logical commits;
3. rewrite `AGENT_STATE.md` to reflect current state;
4. append one new entry to `SESSION_LOG.md`;
5. keep both files short, factual, and agent-readable.

## Current Focus

Prepare safe future work on Google Calendar integration while preserving the currently stable production flows.
