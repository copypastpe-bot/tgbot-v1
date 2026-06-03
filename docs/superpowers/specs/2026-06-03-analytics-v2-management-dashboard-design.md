# Analytics v2 Management Dashboard Design

## Context

The first analytics dashboard exposed the cashbook in a browser: period shortcuts, four cash totals, two sparse charts, and a transaction ledger. That is not enough for owner-level decisions because Telegram already shows similar cash movement. The next version must answer business questions:

- How much did we earn by checks?
- How much salary did we pay?
- What percentage of checks went to salaries?
- How much revenue did we lose by accepting bonuses?
- What were the largest expenses?
- Can we group expenses well enough from existing methods and comments?
- How did these values move over time?

## Product Goal

Create an owner/partner management dashboard that starts with actionable financial analytics and keeps the raw ledger as drill-down, not as the main product.

The default screen remains the main cash/business contour, with navigation to cleaning. The owner must be able to choose arbitrary periods and quickly see margin pressure from salaries, bonuses, and expenses.

## Data Sources

Analytics v2 will compute metrics from several tables instead of relying only on cashbook rows:

- `orders`: check amount, live money amount, bonuses spent/earned, payment method, master, created date.
- `order_payments`: payment method breakdown for non-cleaning orders.
- `payroll_items`: base pay, fuel pay, upsell pay, total salary, linked order and master.
- `staff`: master names and roles.
- `cashbook_entries`: non-order expenses, dividends/withdrawals, and cash movement.
- `cleaning_orders`, `cleaning_order_payments`, `cleaning_cashbook`: separate cleaning contour metrics.

The main business dashboard will treat `orders.amount_total` as gross check revenue. Bonus loss percent is:

```text
bonus_spent / amount_total
```

Example: check `5000`, bonuses `1000`, live money `4000` means bonus loss is `20%`.

## Period Controls

The dashboard must support both quick presets and arbitrary dates:

- Today.
- Week.
- Month.
- Year.
- Custom `date_from` and `date_to`.

All period filters use the existing Moscow business-day interpretation from `analytics_app.dates`: selected local dates are converted to UTC bounds before querying.

## Main Dashboard Layout

The first viewport should show decision metrics, not a ledger:

- Gross checks: sum of `orders.amount_total`.
- Live money: sum of `orders.amount_cash` or payment parts, excluding bonus value.
- Bonuses spent: sum of `orders.bonus_spent`.
- Bonus loss percent: `bonuses_spent / gross_checks`.
- Salaries: sum of `payroll_items.total_pay`.
- Salary percent: `salaries / gross_checks`.
- Other expenses: recognized operating expenses from `cashbook_entries`.
- Operating profit estimate: `live_money - salaries - other_expenses`.

The balance card can remain, but it must be visually secondary because cash balance is not the same as profitability.

## Salary Analytics

Add a salary section with:

- Total salary for the selected period.
- Salary as percent of gross checks.
- Breakdown by component: base pay, fuel pay, upsell pay.
- Salary by master.
- Master-level table: master, orders count, gross checks, live money, salary, salary percent.
- Chart: salary by master.
- Chart or line: salary percent over time.

This section answers both "how much did salaries cost?" and "how heavy are salaries relative to checks?".

## Bonus Analytics

Add a bonus section with:

- Bonuses spent.
- Bonuses earned.
- Bonus loss percent from gross checks.
- Live-money gap: `amount_total - amount_cash`.
- Top orders by bonuses spent.
- Chart: bonuses spent over time.
- Chart: bonus loss percent over time.

The dashboard will label spent bonuses as revenue pressure, not as a cash expense, because it reduces collected money rather than creating a cashbook outflow.

## Expense Analytics

Expenses will be grouped heuristically because the current system does not store strict expense categories for the main cashbook. The grouping should be explicit and inspectable.

Initial groups:

- Salary: from `payroll_items`, not cashbook comments.
- Fuel/transport: comments or methods containing `бенз`, `топлив`, `дорог`, `такси`.
- Materials/chemistry: `хим`, `средств`, `расходник`, `перчат`, `пакет`.
- Advertising/marketing: `реклам`, `авито`, `директ`, `таргет`, `лид`.
- Rent/utilities: `аренд`, `офис`, `склад`, `коммун`.
- Telecom/software: `связ`, `телефон`, `интернет`, `софт`, `crm`, `wahelp`.
- Refunds/corrections: `возврат`, `коррект`.
- Dividends/withdrawals: existing `[DIV]`, `DIV`, `[WDR]`, withdrawal comments.
- Other recognized by method.
- Uncategorized.

The UI must show each grouped row with amount, share of expenses, count of operations, and examples/comments. Uncategorized stays visible so the owner can see where comments are too vague.

## Largest Expenses

Add a section that surfaces the largest expense rows for the period:

- Top 10 by amount.
- Optional threshold filter in UI later, but first version can use top 10.
- Each row shows date, amount, detected group, method, comment.
- Dividends/withdrawals are shown separately from operating expenses so they do not distort operational profitability.

## Charts

The first implementation should include these charts:

- Revenue vs salaries vs other expenses by day/week/month bucket.
- Salary percent of gross checks over time.
- Bonus loss percent over time.
- Expense groups as a bar chart.
- Waterfall: gross checks -> bonuses spent -> live money -> salaries -> other expenses -> operating profit estimate.

Chart bucket size depends on period length:

- Up to 45 days: daily buckets.
- 46 to 180 days: weekly buckets.
- More than 180 days: monthly buckets.

## Cleaning Dashboard

Cleaning remains separate from the main business contour. Analytics v2 should apply the same ideas where the data supports it:

- Gross cleaning orders.
- Live money.
- Bonuses/gift certificates where relevant.
- Cleaning expenses by method/category from `cleaning_cashbook`.
- Profit estimate.
- Largest cleaning expenses.

If cleaning has no payroll table equivalent yet, the UI should not fake salary metrics. It should show "salary data unavailable for cleaning" only where needed, or omit the section.

## Raw Ledger

The raw transaction ledger remains available as drill-down:

- It moves below analytics sections.
- It supports search by comment/method.
- It can be filtered by detected expense group.
- It is not the main first-screen product.

## Architecture

Keep the existing separate `analytics_app` service and extend it with well-bounded modules:

- `analytics_app/dates.py`: arbitrary period parsing and bucket selection.
- `analytics_app/queries.py`: DB fetches for orders, payroll, cashbook, and cleaning.
- `analytics_app/expense_groups.py`: deterministic expense classification rules.
- `analytics_app/management.py`: pure metric calculations, ratios, waterfall data, and chart payloads.
- `analytics_app/templates.py`: server-rendered dashboard UI.
- `analytics_app/static/app.js`: Chart.js rendering and table filtering.

Calculations must be pure functions where practical so they can be unit-tested without a real DB.

## Error Handling

- If a table is missing or a query fails, return a clear server log and a simple page-level error instead of silently showing zeros.
- If a ratio denominator is zero, show `0%` and mark the metric as no-data in chart payloads.
- If expense grouping cannot classify a row, classify as `Uncategorized`; never hide it.
- If there are no rows in a period, charts show empty states and metric cards show zeros.

## Testing

Add focused unit tests for:

- Custom period parsing.
- Bonus loss percent calculations.
- Salary percent calculations.
- Expense grouping keyword rules.
- Waterfall calculation.
- Top expense extraction.
- Chart bucket selection.
- Template contains the expected management sections.

Existing analytics tests must continue to pass.

## Deployment

Deploy remains the same service:

- Push committed `main`.
- Pull/reset `/opt/telegram-analytics`.
- Restart `telegram-analytics.service`.
- Verify HTTPS login and authenticated main/cleaning pages.

No database schema change is required for Analytics v2.

## Open Follow-Up

Create a read-only PostgreSQL user for analytics. The current production deployment uses the bot DB credentials, which works but is not ideal for a web-facing dashboard.
