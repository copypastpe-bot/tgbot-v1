"""Форматтеры алертов в money-flow чат клининга."""

from __future__ import annotations

from decimal import Decimal


def _money(value: Decimal) -> str:
    """1234.5 → '1 234.50', 87540 → '87 540'."""
    quant = value.quantize(Decimal("0.01"))
    if quant == quant.to_integral_value():
        return f"{int(quant):,}".replace(",", " ")
    return f"{quant:,.2f}".replace(",", " ")


def format_order_provided_alert(
    *,
    order_id: int,
    foreman_name: str,
    client_phone: str,
    client_name: str,
    address: str,
    total_amount: Decimal,
    payments: list[tuple[str, Decimal]],
    expenses: list[tuple[str, Decimal]],
    bonuses_used: Decimal,
    bonuses_earned: Decimal,
    profit: Decimal,
    balance_after: Decimal,
) -> str:
    pay_line = ", ".join(f"{_money(a)}₽ {m}" for m, a in payments) or "—"
    exp_line = ", ".join(f"{m} {_money(a)}₽" for m, a in expenses) or "—"
    lines = [
        f"✅ Уборка проведена #{order_id}",
        f"Бригадир: {foreman_name}",
        f"Клиент: {client_phone} ({client_name})",
        f"Адрес: {address}",
        f"Сумма: {_money(total_amount)}₽",
        f"Оплата: {pay_line}",
        f"Расходы: {exp_line}",
        f"Бонусы: списано {_money(bonuses_used)}, начислено {_money(bonuses_earned)}",
        f"Прибыль по заказу: {_money(profit)}₽",
        f"Касса клининга: {_money(balance_after)}₽",
    ]
    return "\n".join(lines)


def format_dividend_alert(
    *, amount: Decimal, recipient: str, balance_after: Decimal
) -> str:
    return (
        "💸 DIV клининг\n"
        f"Сумма: {_money(amount)}₽\n"
        f"Получатель: {recipient}\n"
        f"Остаток кассы клининга: {_money(balance_after)}₽"
    )


def format_cash_op_alert(
    *,
    op_label: str,           # 'Приход', 'Расход', 'Изъятие'
    bucket: str,             # метод/категория
    amount: Decimal,
    comment: str | None,
    balance_after: Decimal,
) -> str:
    lines = [
        f"📒 Касса клининга: {op_label}",
        f"{bucket}: {_money(amount)}₽",
    ]
    if comment:
        lines.append(f"Комментарий: {comment}")
    lines.append(f"Остаток: {_money(balance_after)}₽")
    return "\n".join(lines)


def format_cash_report(*, label: str, report: dict, balance_after: Decimal) -> str:
    lines = [f"📊 Касса клининга — {label}"]
    lines.append(f"Приход: {_money(report['income_total'])}₽")
    for m, v in sorted(report["income_by_method"].items()):
        lines.append(f"  • {m}: {_money(v)}₽")
    if report["gift_total"] > 0:
        lines.append(f"Сертификаты (вне кассы): {_money(report['gift_total'])}₽")
    lines.append(f"Расход: {_money(report['expense_total'])}₽")
    for c, v in sorted(report["expense_by_category"].items()):
        lines.append(f"  • {c}: {_money(v)}₽")
    if report["dividend_total"] > 0:
        lines.append(f"DIV: {_money(report['dividend_total'])}₽")
    if report["withdrawal_total"] > 0:
        lines.append(f"Изъятия: {_money(report['withdrawal_total'])}₽")
    if report["deposit_total"] > 0:
        lines.append(f"Доп. внесения: {_money(report['deposit_total'])}₽")
    lines.append(f"Прибыль (income − expense): {_money(report['profit'])}₽")
    lines.append(f"Баланс сейчас: {_money(balance_after)}₽")
    return "\n".join(lines)


def format_orders_list(*, label: str, orders: list[dict]) -> str:
    if not orders:
        return f"📋 Уборки — {label}\nНет заказов."
    lines = [f"📋 Уборки — {label} ({len(orders)} шт.)"]
    total = Decimal("0")
    for o in orders:
        time_str = o["happened_at"].strftime("%d.%m %H:%M")
        client = o["client_name"] or "Клиент"
        pay = o["pay_summary"] or "—"
        lines.append(
            f"#{o['id']} {time_str} {client} — {o['address']} — "
            f"{_money(o['total_amount'])}₽ ({pay})"
        )
        total += o["total_amount"]
    lines.append(f"Итого: {_money(total)}₽")
    return "\n".join(lines)


def format_cancel_order_alert(
    *,
    order_id: int,
    address: str,
    total_amount: Decimal,
    bonuses_used: int,
    bonuses_earned: int,
    cashbook_rows_deleted: int,
    balance_after: Decimal,
) -> str:
    return "\n".join(
        [
            f"↩️ Отменён заказ уборки #{order_id}",
            f"Адрес: {address}",
            f"Сумма чека была: {_money(total_amount)}₽",
            f"Откатано строк кассы: {cashbook_rows_deleted}",
            f"Возвращено бонусов клиенту: {bonuses_used}",
            f"Снято начисленных бонусов: {bonuses_earned}",
            f"Касса клининга: {_money(balance_after)}₽",
        ]
    )
