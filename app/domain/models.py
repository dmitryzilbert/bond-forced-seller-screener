from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional
from pydantic import BaseModel, Field


class Instrument(BaseModel):
    isin: str
    figi: Optional[str] = None
    name: str
    issuer: str | None = None
    nominal: float = 1000.0
    maturity_date: date
    segment: str | None = None


class OrderBookLevel(BaseModel):
    price: float
    lots: int


class OrderBookSnapshot(BaseModel):
    isin: str
    ts: datetime
    bids: List[OrderBookLevel] = Field(default_factory=list)
    asks: List[OrderBookLevel] = Field(default_factory=list)
    nominal: float = 1000.0

    @property
    def best_bid(self) -> float | None:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return self.asks[0].price if self.asks else None

    @property
    def mid_price(self) -> float | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2


class Event(BaseModel):
    isin: str
    ts: datetime
    ytm_mid: float
    ytm_event: float
    delta_ytm_bps: float
    ask_lots_window: float
    ask_notional_window: float
    spread_ytm_bps: float
    score: float
    stress_flag: bool = False
    near_maturity_flag: bool = False
    payload: dict | None = None
