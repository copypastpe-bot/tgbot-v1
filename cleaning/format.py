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
