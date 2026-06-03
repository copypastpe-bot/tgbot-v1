from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class ExpenseRow:
    id: int
    happened_at: datetime | None
    amount: Decimal
    method: str
    comment: str
