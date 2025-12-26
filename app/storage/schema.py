from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import String, Integer, Float, Date, DateTime, JSON, Boolean, Index
from sqlalchemy.orm import Mapped, mapped_column
from .db import Base


class InstrumentORM(Base):
    __tablename__ = "instruments"
    isin: Mapped[str] = mapped_column(String, primary_key=True)
    instrument_uid: Mapped[str | None]
    figi: Mapped[str | None]
    ticker: Mapped[str | None]
    class_code: Mapped[str | None]
    name: Mapped[str]
    issuer: Mapped[str | None]
    nominal: Mapped[float]
    maturity_date: Mapped[date] = mapped_column(Date)
    segment: Mapped[str | None]
    updated_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    amortization_flag: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    floating_coupon_flag: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    has_call_offer: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    eligible: Mapped[bool] = mapped_column(Boolean, default=False)
    eligible_reason: Mapped[str | None]
    eligibility_checked_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    is_shortlisted: Mapped[bool] = mapped_column(Boolean, default=False)


class OrderbookSnapshotORM(Base):
    __tablename__ = "orderbook_snapshots"
    __table_args__ = (
        Index("ix_orderbook_snapshots_isin_ts", "isin", "ts"),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    isin: Mapped[str]
    ts: Mapped[datetime] = mapped_column(DateTime)
    bids_json: Mapped[dict] = mapped_column(JSON)
    asks_json: Mapped[dict] = mapped_column(JSON)
    best_bid: Mapped[float | None]
    best_ask: Mapped[float | None]
    nominal: Mapped[float] = mapped_column(Float, default=1000.0)


class EventORM(Base):
    __tablename__ = "events"
    __table_args__ = (
        Index("ix_events_isin_ts", "isin", "ts"),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    isin: Mapped[str]
    ts: Mapped[datetime] = mapped_column(DateTime)
    ytm_mid: Mapped[float]
    ytm_event: Mapped[float]
    delta_ytm_bps: Mapped[float]
    ask_lots_window: Mapped[float]
    ask_notional_window: Mapped[float]
    spread_ytm_bps: Mapped[float]
    score: Mapped[float]
    stress_flag: Mapped[bool]
    near_maturity_flag: Mapped[bool]
    payload_json: Mapped[dict | None] = mapped_column(JSON)
