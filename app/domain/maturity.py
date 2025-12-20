from __future__ import annotations

from datetime import date
from .ytm import days_to_maturity


def is_near_maturity(maturity_date: date, *, near_maturity_days: int) -> bool:
    return days_to_maturity(maturity_date) <= near_maturity_days


def maturity_bucket(maturity_date: date, now: date | None = None) -> str:
    """Map maturity to a coarse bucket for peer comparisons."""

    days = days_to_maturity(maturity_date, now=now)
    if days <= 30:
        return "0-30d"
    if days <= 90:
        return "30-90d"
    if days <= 180:
        return "90-180d"
    if days <= 365:
        return "180-365d"
    if days <= 365 * 3:
        return "1-3y"
    return "3y+"
