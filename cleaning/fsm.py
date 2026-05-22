"""FSM states for cleaning order completion."""

from __future__ import annotations

from aiogram.fsm.state import State, StatesGroup


class CleaningOrderFSM(StatesGroup):
    phone = State()
    name = State()
    address_choice = State()
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


class CleaningCashAddFSM(StatesGroup):
    method = State()
    amount = State()
    comment = State()
    confirm = State()


class CleaningCashExpenseFSM(StatesGroup):
    category = State()
    amount = State()
    comment = State()
    confirm = State()


class CleaningCashWithdrawalFSM(StatesGroup):
    amount = State()
    comment = State()
    confirm = State()


class CleaningCancelOrderFSM(StatesGroup):
    confirm = State()


class CleaningClientLookupFSM(StatesGroup):
    phone = State()


class CleaningForemanExpenseFSM(StatesGroup):
    amount = State()
    category = State()
    comment = State()
    confirm = State()
