# Cleaning Contour Rework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rework the cleaning contour so cleaners use a dedicated role/menu and a dry-cleaning-style order workflow without touching stable dry-cleaning flows.

**Architecture:** Keep cleaning behavior inside `cleaning/` and use `bot.py` only for shared RBAC/menu wiring and notification enqueue helpers. Use the existing RBAC tables, shared `clients`, shared `bonus_transactions`, and shared `notification_outbox`; keep cleaning finance in `cleaning_cashbook`.

**Tech Stack:** Python 3, aiogram, asyncpg, JSON notification rules, unittest.

---

## Files

- Modify `bot.py`: RBAC permissions, role-aware menu selection, command menu, cleaner help, cleaning notification enqueue helper.
- Modify `cleaning/access.py`: DB-driven permission helpers for cleaner/admin checks.
- Modify `cleaning/orders.py`: money parsing, minimum order, bonus and payment validation helpers.
- Modify `cleaning/handlers.py`: cleaner menu, order FSM flow, client lookup, cleaner expense, admin permission checks.
- Modify `cleaning/notify.py`: better chat-id failure diagnostics.
- Modify `cleaning/fsm.py`: add states for address choice, cleaner lookup, and expense confirmation if needed.
- Modify `docs/notification_rules.json`: add separate cleaning event keys with existing message text/rules.
- Modify `.env.example`: add `CLEANING_MIN_ORDER_AMOUNT=5500`.
- Modify `tests/test_cleaning_helpers.py`: cover minimum order, bonus/payment helpers, and notification keys.

## Tasks

### Task 1: Tests For Cleaning Business Rules

- [ ] Add tests in `tests/test_cleaning_helpers.py` for:
  - `get_min_order_amount({}) == Decimal("5500")`;
  - env override for `CLEANING_MIN_ORDER_AMOUNT`;
  - bonus max follows `MAX_BONUS_SPEND_RATE_PERCENT` and `MIN_CASH`;
  - `Расчётный` cannot be mixed with other payment methods;
  - `Расчётный` yields zero bonus accrual;
  - notification rules contain `cleaning_order_completed_summary`, `cleaning_order_completed_wire`, `cleaning_order_rating_reminder`.
- [ ] Run `python3 -m unittest tests/test_cleaning_helpers.py`.
- [ ] Expected before implementation: failures for missing helper functions/rules.

### Task 2: Business Helpers

- [ ] Implement helpers in `cleaning/orders.py`:
  - `get_min_order_amount(env=os.environ)`;
  - `get_min_cash_amount(env=os.environ)`;
  - `get_bonus_rate(env=os.environ)`;
  - `get_max_bonus_spend_rate(env=os.environ)`;
  - `qround_ruble`;
  - `calculate_bonus_max(total, balance, env=os.environ)`;
  - `calculate_bonus_earned(payment_method, cash_payment, env=os.environ)`;
  - `validate_payment_parts(parts, total, bonuses_used)`;
  - `is_wire_payment_method(method)`.
- [ ] Run `python3 -m unittest tests/test_cleaning_helpers.py`.
- [ ] Commit helper/test changes.

### Task 3: RBAC And Menus

- [ ] Add cleaning permissions to `PERMISSIONS_CANON` and grants to `ROLE_MATRIX` in `bot.py`.
- [ ] Add `cleaning_main_kb()` in `cleaning/handlers.py`.
- [ ] Update `/start`, `/help`, `/find`, generic cancel, and fallback in `bot.py` to return the cleaner menu for `staff.role='cleaner'`.
- [ ] Add cleaning commands to `set_commands()`.
- [ ] Run `python3 -m py_compile bot.py cleaning/handlers.py cleaning/access.py`.
- [ ] Commit RBAC/menu changes.

### Task 4: Cleaner Access Helpers

- [ ] Add `get_user_role`, `has_permission`, `is_cleaning_admin`, and `can_create_cleaning_order` helpers in `cleaning/access.py`.
- [ ] Use these helpers in all cleaning handlers.
- [ ] Require both `cleaning_create_orders` and active `cleaning_foremen` for order creation.
- [ ] Require `cleaning_view_balance`, `cleaning_view_clients`, `cleaning_record_expense`, `cleaning_manage_cash`, `cleaning_view_reports`, and `cleaning_cancel_orders` on the relevant commands.
- [ ] Run `python3 -m py_compile cleaning/access.py cleaning/handlers.py`.
- [ ] Commit access changes.

### Task 5: Cleaning Order Flow

- [ ] Rework `CleaningOrderFSM` in `cleaning/fsm.py` to support address choice and the new payment-before-bonus order.
- [ ] Rework `cleaning/handlers.py` order flow:
  - phone lookup;
  - address reuse/new address;
  - minimum check via `CLEANING_MIN_ORDER_AMOUNT`;
  - first payment method selection;
  - `Расчётный` single-method branch with zero bonuses;
  - non-wire bonus spend with `Списать N бонусов` / `Не списывать`;
  - non-wire payment collection excluding `Расчётный`;
  - expense loop;
  - dry-cleaning-style confirmation.
- [ ] Preserve transaction boundaries and idempotent `client_op_id`.
- [ ] Run `python3 -m py_compile cleaning/handlers.py cleaning/fsm.py`.
- [ ] Run `python3 -m unittest tests/test_cleaning_helpers.py`.
- [ ] Commit order-flow changes.

### Task 6: Notifications

- [ ] Add cleaning notification event keys to `docs/notification_rules.json` using the existing dry-cleaning text and timing.
- [ ] Add a cleaning notification enqueue helper in `bot.py` or `cleaning/handlers.py` that calls shared `enqueue_notification`.
- [ ] Enqueue `cleaning_order_completed_summary` for non-wire orders.
- [ ] Enqueue `cleaning_order_completed_wire` for `Расчётный`.
- [ ] Enqueue `cleaning_order_rating_reminder` for all completed cleaning orders.
- [ ] Do not update `orders.rating_requested_at`; cleaning orders live in `cleaning_orders`.
- [ ] Run `python3 -m unittest tests/test_cleaning_helpers.py`.
- [ ] Commit notification changes.

### Task 7: Cleaner Client Lookup And Expense

- [ ] Add `🔍 Клиент` handler in `cleaning/handlers.py` using `cleaning_view_clients`.
- [ ] Add `➖ Добавить расход` handler in `cleaning/handlers.py` using `cleaning_record_expense`.
- [ ] Ensure cleaner expense writes `cleaning_cashbook(kind='expense', order_id=NULL)` only.
- [ ] Return `cleaning_main_kb()` after lookup, expense, and cancellation.
- [ ] Run `python3 -m py_compile cleaning/handlers.py`.
- [ ] Commit cleaner utility actions.

### Task 8: Money-Flow Diagnostics And Final Verification

- [ ] Update `cleaning/notify.py` warning to include configured chat id and Telegram exception details.
- [ ] Add `CLEANING_MIN_ORDER_AMOUNT=5500` to `.env.example`.
- [ ] Run:
  - `python3 -m unittest tests/test_cleaning_helpers.py`;
  - `python3 -m py_compile bot.py cleaning/*.py notifications/*.py`.
- [ ] Run `git status --short`.
- [ ] Commit final diagnostics/config changes if not already committed.

## Deployment Follow-Up

Before production deploy, check prod env for `CLEANING_MONEY_FLOW_CHAT_ID`. If Telegram says the chat is not found or forbidden, get the actual supergroup id (`-100...`) and update prod env before restart.
