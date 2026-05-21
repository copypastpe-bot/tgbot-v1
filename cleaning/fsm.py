"""FSM states for cleaning order completion."""

from __future__ import annotations

from aiogram.fsm.state import State, StatesGroup


class CleaningOrderFSM(StatesGroup):
    phone = State()
    name = State()
    address = State()
    amount = State()
    bonus_spend = State()
    pay_method = State()
    pay_amount = State()
    pay_more = State()
    expense_category = State()
    expense_amount = State()
    expense_more = State()
    confirm = State()


class CleaningDividendFSM(StatesGroup):
    amount = State()
    comment = State()
    confirm = State()
