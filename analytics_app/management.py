from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP


ZERO = Decimal("0")


@dataclass(frozen=True)
class ExpenseRow:
    id: int
    happened_at: datetime | None
    amount: Decimal
    method: str
    comment: str


@dataclass(frozen=True)
class OrderMetricRow:
    id: int
    created_at: datetime
    master_id: int | None
    master_name: str
    amount_total: Decimal
    amount_cash: Decimal
    bonus_spent: Decimal
    bonus_earned: Decimal


@dataclass(frozen=True)
class PayrollMetricRow:
    order_id: int
    master_id: int | None
    master_name: str
    base_pay: Decimal
    fuel_pay: Decimal
    upsell_pay: Decimal
    total_pay: Decimal


@dataclass(frozen=True)
class MasterSalarySummary:
    master_id: int | None
    master_name: str
    orders_count: int
    gross_checks: Decimal
    live_money: Decimal
    salary: Decimal
    salary_percent: Decimal


@dataclass(frozen=True)
class ManagementDashboard:
    gross_checks: Decimal
    live_money: Decimal
    bonuses_spent: Decimal
    bonuses_earned: Decimal
    bonus_loss_percent: Decimal
    salary_total: Decimal
    salary_percent: Decimal
    salary_base: Decimal
    salary_fuel: Decimal
    salary_upsell: Decimal
    other_expenses: Decimal
    operating_profit: Decimal
    expense_groups: list[object] = field(default_factory=list)
    top_expenses: list[ExpenseRow] = field(default_factory=list)
    master_salaries: list[MasterSalarySummary] = field(default_factory=list)
    waterfall: dict[str, Decimal] = field(default_factory=dict)
    charts: dict[str, object] = field(default_factory=dict)


def ratio(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator == 0:
        return ZERO
    return (numerator / denominator).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _build_master_salaries(
    orders: list[OrderMetricRow],
    payroll: list[PayrollMetricRow],
    gross_checks: Decimal,
) -> list[MasterSalarySummary]:
    order_bucket: dict[int | None, list[OrderMetricRow]] = {}
    payroll_bucket: dict[int | None, list[PayrollMetricRow]] = {}
    names: dict[int | None, str] = {}
    for row in orders:
        order_bucket.setdefault(row.master_id, []).append(row)
        names[row.master_id] = row.master_name or "Без мастера"
    for row in payroll:
        payroll_bucket.setdefault(row.master_id, []).append(row)
        names[row.master_id] = row.master_name or "Без мастера"

    result: list[MasterSalarySummary] = []
    for master_id in set(order_bucket) | set(payroll_bucket):
        master_orders = order_bucket.get(master_id, [])
        master_payroll = payroll_bucket.get(master_id, [])
        salary = sum((row.total_pay for row in master_payroll), ZERO)
        result.append(
            MasterSalarySummary(
                master_id=master_id,
                master_name=names.get(master_id) or "Без мастера",
                orders_count=len(master_orders),
                gross_checks=sum((row.amount_total for row in master_orders), ZERO),
                live_money=sum((row.amount_cash for row in master_orders), ZERO),
                salary=salary,
                salary_percent=ratio(salary, gross_checks),
            )
        )
    return sorted(result, key=lambda item: item.salary, reverse=True)


def build_management_dashboard(
    *,
    orders: list[OrderMetricRow],
    payroll: list[PayrollMetricRow],
    expenses: list[ExpenseRow],
    group_by: str,
) -> ManagementDashboard:
    from .expense_groups import group_expenses

    gross_checks = sum((row.amount_total for row in orders), ZERO)
    live_money = sum((row.amount_cash for row in orders), ZERO)
    bonuses_spent = sum((row.bonus_spent for row in orders), ZERO)
    bonuses_earned = sum((row.bonus_earned for row in orders), ZERO)
    salary_total = sum((row.total_pay for row in payroll), ZERO)
    salary_base = sum((row.base_pay for row in payroll), ZERO)
    salary_fuel = sum((row.fuel_pay for row in payroll), ZERO)
    salary_upsell = sum((row.upsell_pay for row in payroll), ZERO)
    other_expenses = sum((row.amount for row in expenses), ZERO)
    operating_profit = live_money - salary_total - other_expenses
    waterfall = {
        "gross_checks": gross_checks,
        "bonuses_spent": -bonuses_spent,
        "live_money": live_money,
        "salaries": -salary_total,
        "other_expenses": -other_expenses,
        "operating_profit": operating_profit,
    }

    return ManagementDashboard(
        gross_checks=gross_checks,
        live_money=live_money,
        bonuses_spent=bonuses_spent,
        bonuses_earned=bonuses_earned,
        bonus_loss_percent=ratio(bonuses_spent, gross_checks),
        salary_total=salary_total,
        salary_percent=ratio(salary_total, gross_checks),
        salary_base=salary_base,
        salary_fuel=salary_fuel,
        salary_upsell=salary_upsell,
        other_expenses=other_expenses,
        operating_profit=operating_profit,
        expense_groups=group_expenses(expenses),
        top_expenses=sorted(expenses, key=lambda row: row.amount, reverse=True)[:10],
        master_salaries=_build_master_salaries(orders, payroll, gross_checks),
        waterfall=waterfall,
        charts={},
    )
