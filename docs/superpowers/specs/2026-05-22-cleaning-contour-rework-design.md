# Cleaning Contour Rework Design

Date: 2026-05-22
Status: approved for implementation planning

## Goal

Rework the cleaning contour so cleaners have their own role, menu, and Telegram
workflow while preserving the isolated cleaning financial contour and the stable
dry-cleaning order, payroll, cashbook, and notification flows.

The cleaning code remains in the `cleaning/` package. The large dry-cleaning
flow in `bot.py` is used as the UX and business-rule reference, but the cleaning
implementation should not be merged into that monolith.

## Decisions

- Use `staff.role = 'cleaner'` as the real access role.
- Keep `cleaning_foremen` as the cleaning profile table used by cleaning orders.
- Cleaner and dry-cleaning master are mutually exclusive roles.
- Cleaning has its own main menu, not the dry-cleaning master menu.
- Cleaning minimum order amount is configured through env, default `5500`.
- Cleaning client notifications use separate event keys, but the same text and
  rules as dry cleaning.
- Cleaning `Расчётный` has no payment-linking workflow.
- Cleaning `Расчётный` gets no bonus spend and no bonus accrual.
- Cleaning `Расчётный` is not mixed with other payment methods.

## Roles And Permissions

Add cleaning permissions to the existing RBAC model:

- `cleaning_create_orders`
- `cleaning_view_balance`
- `cleaning_view_clients`
- `cleaning_record_expense`
- `cleaning_view_reports`
- `cleaning_manage_cash`
- `cleaning_cancel_orders`

Role grants:

- `cleaner`: `cleaning_create_orders`, `cleaning_view_balance`,
  `cleaning_view_clients`, `cleaning_record_expense`.
- `admin` and `superadmin`: all cleaning permissions.

The `cleaning_foremen` table still controls which cleaner profile is attached to
a cleaning order. A user who can run `/cleaning_order` must have both the RBAC
permission and an active `cleaning_foremen` row.

## Menus

Runtime menu selection:

```text
admin/superadmin -> admin_root_kb()
cleaner          -> cleaning_main_kb()
master           -> master_kb
other            -> main_kb
```

Cleaner menu:

```text
🧹 Провести уборку
🔍 Клиент
💰 Баланс
➖ Добавить расход
```

This menu is returned after successful cleaning actions, after cancellation, and
after unknown non-command text from a cleaner.

Telegram command menu should include the cleaning commands. Access remains
enforced in handlers.

## Cleaning Order Flow

The flow should feel like the dry-cleaning master order flow while keeping the
cleaning-specific address and expense steps.

1. Start from `🧹 Провести уборку` or `/cleaning_order`.
2. Require `cleaning_create_orders` and an active `cleaning_foremen` profile.
3. Ask for client phone and normalize it like the dry-cleaning flow.
4. Search the shared `clients` table.
5. If found, show name, phone, and bonus balance.
6. If not found, ask for client name and create the client during commit.
7. Ask for the cleaning address.
   - If `clients.address` exists, offer `Использовать этот адрес`,
     `Другой адрес`, and `Отмена`.
   - A new cleaning address is always saved to `cleaning_orders.address`.
   - Update `clients.address` only when it was empty.
8. Ask for total order amount and require it to be at least
   `CLEANING_MIN_ORDER_AMOUNT`.
9. Ask for payment method.
   - `Расчётный` is a single-method branch.
   - `Расчётный` skips bonus spend and bonus accrual.
   - `Расчётный` is saved immediately in `cleaning_order_payments` and
     `cleaning_cashbook(kind='income')`.
10. For non-`Расчётный` payment:
    - Show shared bonus balance.
    - Offer `Списать N бонусов` and `Не списывать`.
    - Use the same spend-limit rules as dry cleaning, including the configured
      percentage and minimum cash part.
    - Collect one or more payment parts from `Наличные`, `Карта`, and
      `Подарочный сертификат`.
    - Validate that payment parts equal `total_amount - bonuses_used`.
    - Save gift certificate payments in `cleaning_order_payments`, but do not
      create cleaning cashbook income rows for them.
11. Collect expenses in a loop.
    - Categories: `Химия`, `ЗП клинеров`, `ГСМ`, `Прочее`.
    - Save each expense as `cleaning_cashbook(kind='expense', order_id=...)`.
12. Show confirmation in the dry-cleaning style:
    - client, phone, address;
    - total amount;
    - payment parts;
    - bonuses spent/accrued;
    - expense parts.
13. Confirm with `Провести` or cancel with `Отменить`.
14. After commit:
    - answer the cleaner with `Готово ✅ Уборка сохранена`;
    - return `cleaning_main_kb()`;
    - send the cleaning money-flow alert.

## Bonus Rules

For non-`Расчётный` cleaning orders, use the dry-cleaning bonus rules:

- same env percentages for maximum spend and accrual;
- same minimum cash part rule;
- same one-year expiration for positive accruals;
- shared `clients.bonus_balance`;
- shared `bonus_transactions`.

For `Расчётный` cleaning orders:

- no bonus spend;
- no bonus accrual;
- no payment-linking or pending-wire workflow.

## Client Lookup

The cleaner `🔍 Клиент` action mirrors the dry-cleaning master client lookup:

- asks for phone;
- normalizes and searches `clients`;
- displays name, phone, bonus balance, birthday, and status;
- returns to `cleaning_main_kb()`.

It requires `cleaning_view_clients`.

## Cleaner Expense

The cleaner `➖ Добавить расход` action writes only to the cleaning cashbook:

1. Require `cleaning_record_expense`.
2. Ask amount.
3. Ask category: `Химия`, `ЗП клинеров`, `ГСМ`, `Прочее`.
4. Ask comment or `Без комментария`.
5. Show confirmation.
6. On confirm, insert `cleaning_cashbook(kind='expense', order_id=NULL)`.
7. Send a cleaning money-flow alert.
8. Return `cleaning_main_kb()`.

It must not write to dry-cleaning `cashbook_entries`.

## Admin Cleaning Commands

Admin-only cleaning commands should be protected by cleaning permissions:

- reports: `cleaning_view_reports`;
- manual income, withdrawal, dividend: `cleaning_manage_cash`;
- cancel order: `cleaning_cancel_orders`;
- broad balance/report access for admins through the same cleaning permissions.

Cleaner-facing commands remain limited to the cleaner menu scope.

## Notifications

Add separate notification rules:

- `cleaning_order_completed_summary`
- `cleaning_order_completed_wire`
- `cleaning_order_rating_reminder`

Use the same message text, timing, and delivery rules as the existing
dry-cleaning rules. It is acceptable that the text says "Заказ".

Payload should match the dry-cleaning payload shape where applicable:

- `total_sum`
- `used_bonus`
- `amount_due`
- `earned_bonus`
- `bonus_expire_date`
- `bonus_balance`
- `order_id`

For `Расчётный`, send the cleaning wire summary immediately, with no bonus
payload effects and no pending-payment workflow.

The rating reminder should be enqueued for completed cleaning orders as a
separate cleaning event. It must not depend on `orders.rating_requested_at`,
because cleaning orders live in `cleaning_orders`.

## Money-Flow Chat

Keep `CLEANING_MONEY_FLOW_CHAT_ID` optional. If empty, sending is a no-op.

Improve diagnostics so a failed Telegram send logs the configured chat id and
the Telegram error. Production follow-up should:

1. read the production env value;
2. verify whether the chat is a normal group or supergroup;
3. fix the env value if it needs the `-100...` supergroup id;
4. send one test cleaning money-flow message after confirmation.

## Data Boundaries

Cleaning remains financially isolated:

- orders: `cleaning_orders`;
- payment parts: `cleaning_order_payments`;
- cashbook: `cleaning_cashbook`.

Shared tables:

- `clients`;
- `bonus_transactions`;
- `notification_outbox`.

The implementation must not change dry-cleaning payroll, order cashbook,
wire-payment linking, or dry-cleaning notification behavior.

## Testing

Add or update focused tests for:

- `CLEANING_MIN_ORDER_AMOUNT` default and override behavior;
- bonus calculation and spend limits matching dry cleaning;
- `Расчётный` forbids mixed payment and yields zero bonus spend/accrual;
- gift certificate payments are excluded from cleaning cashbook income;
- payment parts must balance to `total_amount - bonuses_used`;
- cleaner manual expense writes `cleaning_cashbook(kind='expense')`;
- notification rule keys exist and use the expected timing/text contract.

Verification should include existing cleaning helper tests and syntax validation
for changed modules.
