from __future__ import annotations

import html
import json
from decimal import Decimal
from typing import Any

from .money import MoneySummary


def money(value: Decimal) -> str:
    return f"{value:,.0f}".replace(",", " ") + " ₽"


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
        return default
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
