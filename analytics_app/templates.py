from __future__ import annotations

import html
import json
from decimal import Decimal
from typing import Any

from .money import MoneySummary


def money(value: Decimal) -> str:
    return f"{value:,.0f}".replace(",", " ") + " ₽"


def percent(value: Decimal) -> str:
    return f"{(value * Decimal('100')):.0f}%"


def _json_amounts(values: dict[str, Decimal]) -> str:
    return json.dumps(
        {"labels": list(values.keys()), "values": [float(v) for v in values.values()]},
        ensure_ascii=False,
    )


def render_login(*, error: bool) -> str:
    error_html = '<div class="error">Неверный логин или пароль</div>' if error else ""
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Analytics Login</title>
  <link rel="stylesheet" href="/static/app.css">
</head>
<body class="login-page">
  <main class="login-panel">
    <h1>Финансы</h1>
    <form method="post" action="/login">
      {error_html}
      <label>Логин<input name="login" autocomplete="username"></label>
      <label>Пароль<input name="password" type="password" autocomplete="current-password"></label>
      <button type="submit">Войти</button>
    </form>
  </main>
</body>
</html>"""


def _row_get(row: Any, key: str, default: Any = "") -> Any:
    try:
        value = row[key]
    except Exception:
        value = getattr(row, key, default)
    return default if value is None else value


def _ledger_rows(ledger: list[Any]) -> str:
    rows: list[str] = []
    for row in ledger[:200]:
        happened_at = _row_get(row, "happened_at")
        date_text = happened_at.strftime("%d.%m.%Y %H:%M") if happened_at else ""
        kind = str(_row_get(row, "kind"))
        sign = "+" if kind == "income" else "-"
        rows.append(
            "<tr>"
            f"<td>{html.escape(date_text)}</td>"
            f"<td>{html.escape(kind)}</td>"
            f"<td>{html.escape(str(_row_get(row, 'method')))}</td>"
            f"<td>{html.escape(str(_row_get(row, 'comment')))}</td>"
            f"<td class=\"amount\">{sign}{money(Decimal(_row_get(row, 'amount', 0)))}</td>"
            "</tr>"
        )
    if not rows:
        return '<tr><td colspan="5" class="muted">Операций за период нет</td></tr>'
    return "\n".join(rows)


def _top_expense_table(expenses: list[Any]) -> str:
    rows: list[str] = []
    for row in expenses[:10]:
        happened_at = _row_get(row, "happened_at")
        date_text = happened_at.strftime("%d.%m.%Y %H:%M") if happened_at else ""
        rows.append(
            "<tr>"
            f"<td>{html.escape(date_text)}</td>"
            f"<td>{html.escape(str(_row_get(row, 'method')))}</td>"
            f"<td>{html.escape(str(_row_get(row, 'comment')))}</td>"
            f"<td class=\"amount\">{money(Decimal(_row_get(row, 'amount', 0)))}</td>"
            "</tr>"
        )
    if not rows:
        return '<p class="muted">Крупных расходов за период нет</p>'
    return (
        "<table>"
        "<thead><tr><th>Дата</th><th>Метод</th><th>Комментарий</th><th>Сумма</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )


def _master_salary_table(rows: list[Any]) -> str:
    body: list[str] = []
    for row in rows:
        body.append(
            "<tr>"
            f"<td>{html.escape(str(_row_get(row, 'master_name', 'Без мастера')))}</td>"
            f"<td>{int(_row_get(row, 'orders_count', 0))}</td>"
            f"<td class=\"amount\">{money(Decimal(_row_get(row, 'gross_checks', 0)))}</td>"
            f"<td class=\"amount\">{money(Decimal(_row_get(row, 'live_money', 0)))}</td>"
            f"<td class=\"amount\">{money(Decimal(_row_get(row, 'salary', 0)))}</td>"
            f"<td class=\"amount\">{percent(Decimal(_row_get(row, 'salary_percent', 0)))}</td>"
            "</tr>"
        )
    if not body:
        return '<p class="muted">Зарплатных данных за период нет</p>'
    return (
        "<table>"
        "<thead><tr><th>Мастер</th><th>Заказы</th><th>Чеки</th><th>Живые деньги</th><th>Зарплата</th><th>%</th></tr></thead>"
        f"<tbody>{''.join(body)}</tbody>"
        "</table>"
    )


def _expense_group_table(groups: list[Any]) -> str:
    body: list[str] = []
    for group in groups:
        examples = ", ".join(str(item) for item in _row_get(group, "examples", [])[:3])
        body.append(
            "<tr>"
            f"<td>{html.escape(str(_row_get(group, 'name')))}</td>"
            f"<td class=\"amount\">{money(Decimal(_row_get(group, 'amount', 0)))}</td>"
            f"<td class=\"amount\">{percent(Decimal(_row_get(group, 'share', 0)))}</td>"
            f"<td>{int(_row_get(group, 'count', 0))}</td>"
            f"<td>{html.escape(examples)}</td>"
            "</tr>"
        )
    if not body:
        return '<p class="muted">Расходов для группировки за период нет</p>'
    return (
        "<table>"
        "<thead><tr><th>Группа</th><th>Сумма</th><th>Доля</th><th>Операций</th><th>Примеры</th></tr></thead>"
        f"<tbody>{''.join(body)}</tbody>"
        "</table>"
    )


def render_dashboard(
    *,
    title: str,
    active_section: str,
    period_label: str,
    summary: MoneySummary,
    balance: Decimal,
    ledger: list[Any],
    income_by_method: dict[str, Decimal],
    expense_by_group: dict[str, Decimal],
    extra_note: str,
) -> str:
    active_main = "active" if active_section == "main" else ""
    active_cleaning = "active" if active_section == "cleaning" else ""
    chart_data = {
        "income": json.loads(_json_amounts(income_by_method)),
        "expense": json.loads(_json_amounts(expense_by_group)),
    }
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <link rel="stylesheet" href="/static/app.css">
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
  <aside class="sidebar">
    <div class="brand">Финансы</div>
    <a class="{active_main}" href="/">Основная касса</a>
    <a class="{active_cleaning}" href="/cleaning">Клининг</a>
    <a href="/logout">Выйти</a>
  </aside>
  <main class="content">
    <header class="topbar">
      <div>
        <h1>{html.escape(title)}</h1>
        <p>{html.escape(period_label)}</p>
      </div>
      <form class="filters" method="get">
        <button name="period" value="today">Сегодня</button>
        <button name="period" value="week">Неделя</button>
        <button name="period" value="month">Месяц</button>
        <button name="period" value="year">Год</button>
      </form>
    </header>
    <section class="metrics">
      <div><span>Приход</span><strong>{money(summary.income)}</strong></div>
      <div><span>Расход</span><strong>{money(summary.expense)}</strong></div>
      <div><span>Прибыль</span><strong>{money(summary.profit)}</strong></div>
      <div><span>Остаток</span><strong>{money(balance)}</strong></div>
    </section>
    <p class="note">{html.escape(extra_note)}</p>
    <section class="charts">
      <div><h2>Доходы</h2><canvas id="incomeChart"></canvas></div>
      <div><h2>Расходы</h2><canvas id="expenseChart"></canvas></div>
    </section>
    <section>
      <h2>Детализация операций</h2>
      <input class="table-search" id="ledgerSearch" placeholder="Поиск по комментарию">
      <table id="ledgerTable">
        <thead><tr><th>Дата</th><th>Тип</th><th>Метод</th><th>Комментарий</th><th>Сумма</th></tr></thead>
        <tbody>{_ledger_rows(ledger)}</tbody>
      </table>
    </section>
  </main>
  <script id="chart-data" type="application/json">{html.escape(json.dumps(chart_data, ensure_ascii=False))}</script>
  <script src="/static/app.js"></script>
</body>
</html>"""


def render_management_dashboard(
    *,
    title: str,
    active_section: str,
    period_label: str,
    date_from: str,
    date_to: str,
    balance: Decimal,
    dashboard: Any,
    ledger: list[Any],
    extra_note: str,
) -> str:
    active_main = "active" if active_section == "main" else ""
    active_cleaning = "active" if active_section == "cleaning" else ""
    chart_data = dashboard.charts
    note_html = f'<p class="note">{html.escape(extra_note)}</p>' if extra_note else ""
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <link rel="stylesheet" href="/static/app.css">
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
  <aside class="sidebar">
    <div class="brand">Финансы</div>
    <a class="{active_main}" href="/">Основная касса</a>
    <a class="{active_cleaning}" href="/cleaning">Клининг</a>
    <a href="/logout">Выйти</a>
  </aside>
  <main class="content">
    <header class="topbar">
      <div>
        <h1>{html.escape(title)}</h1>
        <p>{html.escape(period_label)}</p>
      </div>
      <form class="filters date-filters" method="get">
        <input type="date" name="date_from" value="{html.escape(date_from)}">
        <input type="date" name="date_to" value="{html.escape(date_to)}">
        <button type="submit">Показать</button>
        <button name="period" value="today">Сегодня</button>
        <button name="period" value="week">Неделя</button>
        <button name="period" value="month">Месяц</button>
        <button name="period" value="year">Год</button>
      </form>
    </header>
    {note_html}
    <section class="dashboard-section">
      <h2>Управленческий обзор</h2>
      <div class="metrics management-metrics">
        <div><span>Чеки</span><strong>{money(dashboard.gross_checks)}</strong></div>
        <div><span>Живые деньги</span><strong>{money(dashboard.live_money)}</strong></div>
        <div><span>Зарплата</span><strong>{money(dashboard.salary_total)}</strong><small>Зарплата от чеков: {percent(dashboard.salary_percent)}</small></div>
        <div><span>Бонусная потеря</span><strong>{money(dashboard.bonuses_spent)}</strong><small>{percent(dashboard.bonus_loss_percent)} от чеков</small></div>
        <div><span>Прочие расходы</span><strong>{money(dashboard.other_expenses)}</strong></div>
        <div><span>Опер. прибыль</span><strong>{money(dashboard.operating_profit)}</strong></div>
        <div><span>Остаток кассы</span><strong>{money(balance)}</strong></div>
      </div>
    </section>
    <section class="charts grid-2">
      <div><h2>Водопад прибыли</h2><canvas id="waterfallChart"></canvas></div>
      <div><h2>Группы расходов</h2><canvas id="expenseGroupsChart"></canvas></div>
      <div><h2>Зарплата по мастерам</h2><canvas id="salaryByMasterChart"></canvas></div>
      <div><h2>Динамика</h2><canvas id="timeSeriesChart"></canvas></div>
    </section>
    <section class="dashboard-section">
      <h2>Зарплаты</h2>
      <p>База: {money(dashboard.salary_base)}. Бензин: {money(dashboard.salary_fuel)}. Допродажи: {money(dashboard.salary_upsell)}.</p>
      {_master_salary_table(dashboard.master_salaries)}
    </section>
    <section class="dashboard-section">
      <h2>Бонусы</h2>
      <p>Списано: {money(dashboard.bonuses_spent)}. Начислено: {money(dashboard.bonuses_earned)}. Потеря: {percent(dashboard.bonus_loss_percent)} от чеков.</p>
    </section>
    <section class="dashboard-section">
      <h2>Группировка расходов</h2>
      {_expense_group_table(dashboard.expense_groups)}
    </section>
    <section class="dashboard-section">
      <h2>Крупные расходы</h2>
      {_top_expense_table(dashboard.top_expenses)}
    </section>
    <section class="dashboard-section">
      <h2>Детализация операций</h2>
      <input class="table-search" id="ledgerSearch" placeholder="Поиск по комментарию или методу">
      <table id="ledgerTable">
        <thead><tr><th>Дата</th><th>Тип</th><th>Метод</th><th>Комментарий</th><th>Сумма</th></tr></thead>
        <tbody>{_ledger_rows(ledger)}</tbody>
      </table>
    </section>
  </main>
  <script id="management-chart-data" type="application/json">{html.escape(json.dumps(chart_data, ensure_ascii=False))}</script>
  <script src="/static/app.js"></script>
</body>
</html>"""
