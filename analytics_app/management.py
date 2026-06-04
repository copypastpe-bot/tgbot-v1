from __future__ import annotations

from dataclasses import dataclass, field, replace
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
    category: str = ""


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
    cash_income: Decimal = ZERO
    cash_expense: Decimal = ZERO
    cash_profit: Decimal = ZERO
    div_paid: Decimal = ZERO
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
        master_gross = sum((row.amount_total for row in master_orders), ZERO)
        result.append(
            MasterSalarySummary(
                master_id=master_id,
                master_name=names.get(master_id) or "Без мастера",
                orders_count=len(master_orders),
                gross_checks=master_gross,
                live_money=sum((row.amount_cash for row in master_orders), ZERO),
                salary=salary,
                salary_percent=ratio(salary, master_gross),
            )
        )
    return sorted(result, key=lambda item: item.salary, reverse=True)


def _build_charts(dashboard: ManagementDashboard) -> dict[str, object]:
    return {
        "waterfall": {
            "labels": [
                "Доходы",
                "Расходы",
                "Прибыль",
            ],
            "values": [
                float(dashboard.waterfall[key])
                for key in [
                    "cash_income",
                    "cash_expense",
                    "cash_profit",
                ]
            ],
        },
        "expense_groups": {
            "labels": [group.name for group in dashboard.expense_groups],
            "values": [float(group.amount) for group in dashboard.expense_groups],
        },
        "salary_by_master": {
            "labels": [row.master_name for row in dashboard.master_salaries],
            "values": [float(row.salary) for row in dashboard.master_salaries],
        },
        "time_series": {"labels": [], "datasets": []},
    }


def build_management_dashboard(
    *,
    orders: list[OrderMetricRow],
    payroll: list[PayrollMetricRow],
    expenses: list[ExpenseRow],
    group_by: str,
    cash_income: Decimal | None = None,
    cash_expense: Decimal | None = None,
    div_paid: Decimal = ZERO,
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
    expense_total = cash_expense if cash_expense is not None else sum((row.amount for row in expenses), ZERO)
    income_total = cash_income if cash_income is not None else live_money
    cash_profit = income_total - expense_total
    waterfall = {
        "cash_income": income_total,
        "cash_expense": -expense_total,
        "cash_profit": cash_profit,
    }

    dashboard = ManagementDashboard(
        cash_income=income_total,
        cash_expense=expense_total,
        cash_profit=cash_profit,
        div_paid=div_paid,
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
        other_expenses=expense_total,
        operating_profit=cash_profit,
        expense_groups=group_expenses(expenses),
        top_expenses=sorted(expenses, key=lambda row: row.amount, reverse=True)[:10],
        master_salaries=_build_master_salaries(orders, payroll),
        waterfall=waterfall,
        charts={},
    )
    return replace(dashboard, charts=_build_charts(dashboard))
