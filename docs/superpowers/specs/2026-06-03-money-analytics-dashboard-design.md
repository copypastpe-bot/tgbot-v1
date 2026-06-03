# Money Analytics Dashboard Design

## Purpose

Build a read-only web interface for the owner and partner to analyze financial
data from the production PostgreSQL database without destabilizing the existing
Telegram bot flows.

The first focus is money movement:

- main cash desk;
- cleaning cash desk;
- income, expenses, profit, balances;
- charts and drill-down details.

The dashboard must not edit operational data in the first release.

## Users And Access

The initial users are the owner and one partner.

Access model:

- login and password;
- two configured accounts;
- HTTPS only;
- cookie sessions with `HttpOnly`, `Secure`, and `SameSite=Lax`;
- credentials stored outside code, preferably as password hashes in environment
  variables or a private runtime config file.

The production URL is planned as:

```text
analytics.dastydev.ru
```

DNS requirement:

```text
Type: A
Name: analytics
Value: 91.200.150.68
```

## Architecture

Create a separate analytics web service in this repository, for example under
`analytics/`.

The service runs independently from `bot.py`:

- separate process and systemd unit, for example `telegram-analytics.service`;
- separate localhost port, for example `127.0.0.1:8090`;
- nginx proxies `https://analytics.dastydev.ru` to the analytics service;
- database connection via the existing PostgreSQL `DB_DSN` or a dedicated
  read-only analytics DSN;
- no changes to the bot runtime are required for the dashboard MVP.

Recommended stack:

- Python web service using FastAPI, Starlette, or aiohttp;
- `asyncpg` for PostgreSQL queries;
- server-rendered HTML templates;
- Chart.js for charts;
- no React/Vite build step for the MVP.

This keeps the dashboard close to the existing Python stack while isolating it
from the bot and webhook runtime.

## Navigation

After login, the first screen opens directly on **Main Cash Desk**.

Primary navigation:

- Main Cash Desk;
- Cleaning;
- optionally, later: Customers, Masters, Payroll, Notifications.

The main and cleaning cash desks are separate sections. Their totals are not
mixed in the MVP.

## Main Cash Desk Screen

The main cash desk starts with an overview, not with a ledger table.

Top controls:

- quick periods: today, week, month, year;
- custom date range;
- chart grouping: day, week, month;
- filters for transaction type: income, expense, service/internal.

Main cards:

- income for selected period;
- operating expenses for selected period;
- operating profit;
- current cash balance.

Charts:

- money movement over time;
- income by payment method;
- expenses as a searchable/comment-driven detail view, not strict categories.

Details:

- operation ledger with date, type, method, amount, comment, order id, master id;
- search by comment;
- filters by method and transaction type;
- drill-down to related order, client, and master where available.

## Cleaning Screen

Cleaning has the same overview-first structure, but reads from the cleaning
tables.

Main cards:

- income for selected period;
- expenses for selected period;
- operating profit;
- cleaning cash balance.

Charts:

- money movement over time;
- income by payment method;
- expenses by category.

Details:

- cleaning cashbook ledger;
- cleaning orders list;
- drill-down to cleaning order, client, and foreman.

## Data Sources

Main cash desk:

- `cashbook_entries` as the primary cash movement source;
- `order_payments` and `orders` for payment methods and order linkage;
- `payroll_items` for salary and master-level views;
- `staff` for master names;
- `clients` for customer drill-down;
- `jenya_card_entries` for Jenya card reconciliation when needed.

Cleaning:

- `cleaning_cashbook` as the primary cash movement source;
- `cleaning_order_payments` and `cleaning_orders` for payment methods and order
  linkage;
- `cleaning_foremen` for foreman names;
- `clients` for customer drill-down.

## Calculation Rules

Main cash desk:

- operating profit is income minus operating expenses;
- withdrawals and dividends are internal/capital movements, not operating
  expenses;
- startup/opening balances are excluded from ordinary P&L charts;
- deleted cashbook rows are excluded;
- pending wire payments are shown separately when detectable;
- expenses do not have reliable strict categories in current data.

Main cash desk expense grouping:

- do not infer exact categories as source-of-truth;
- show comments and search;
- show service groups where reliable, such as withdrawal, dividend, linked order,
  linked master, Jenya card mirror;
- optional later enhancement: add explicit expense category capture in the bot or
  a controlled recategorization workflow.

Cleaning:

- keep cleaning totals separate from main cash desk totals;
- operating profit is cleaning income minus cleaning operating expenses;
- `deposit`, `withdrawal`, and `dividend` are excluded from operating profit;
- gift certificate payments are shown separately because they appear in payments
  but do not necessarily represent cash movement;
- `cleaning_cashbook.method` may be used as expense category.

## Read-Only Contract

The MVP dashboard is read-only.

Implementation rules:

- code uses parameterized SQL only;
- endpoints must not execute `INSERT`, `UPDATE`, `DELETE`, `ALTER`, or `DROP`;
- if possible, production should use a PostgreSQL role with read-only access;
- no web UI controls for editing, deleting, importing, or recategorizing data.

## Error Handling

Login errors:

- return a generic invalid credentials message;
- do not reveal which login exists.

Database errors:

- show a compact dashboard error state;
- log full details server-side;
- do not expose DSN, SQL text with secrets, or stack traces in the browser.

Data quality gaps:

- show honest labels such as "no explicit category" for main cash expenses;
- avoid misleading charts when source data cannot support the grouping.

## Deployment Shape

Production target:

- host: Timeweb server `91.200.150.68`;
- service path can be `/opt/telegram-analytics` or a sibling of
  `/opt/telegram-bot`;
- systemd service: `telegram-analytics.service`;
- nginx virtual host for `analytics.dastydev.ru`;
- Let's Encrypt certificate for `analytics.dastydev.ru`;
- app bound only to localhost.

Deploy should follow the existing project rule:

```text
local -> git -> VPS
```

Do not deploy from a dirty worktree.

## Verification

Unit tests:

- main cash P&L excludes withdrawals and dividends;
- deleted cashbook rows are excluded;
- opening balances are excluded from ordinary P&L;
- cleaning `deposit`, `withdrawal`, and `dividend` are excluded from operating
  profit;
- cleaning gift certificate payments are reported separately.

Smoke tests:

- login succeeds for configured user;
- unauthenticated requests redirect to login;
- main cash page renders;
- cleaning page renders;
- period filters change returned totals;
- chart data endpoints return valid JSON if the implementation uses separate
  JSON endpoints.

Production checks:

- verify DNS resolves `analytics.dastydev.ru` to `91.200.150.68`;
- verify nginx config syntax;
- verify HTTPS certificate;
- compare a few dashboard totals against direct SQL queries before declaring the
  dashboard ready.

## Out Of Scope For MVP

- editing transactions from the web UI;
- recategorizing main cash expenses;
- writing new expense categories back to the bot;
- user management UI;
- public registration;
- merging main cash and cleaning into one combined total;
- exports, unless they are trivial after the first dashboard is working.
