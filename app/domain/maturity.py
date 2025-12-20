from __future__ import annotations

from datetime import date
from .ytm import days_to_maturity


def is_near_maturity(maturity_date: date, *, near_maturity_days: int) -> bool:
    return days_to_maturity(maturity_date) <= near_maturity_days
