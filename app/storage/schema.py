from __future__ import annotations

from sqlalchemy import Column, String, Integer, Float, Date, DateTime, JSON, BigInteger
from sqlalchemy.orm import Mapped, mapped_column
from .db import Base


class InstrumentORM(Base):
    __tablename__ = "instruments"
    isin: Mapped[str] = mapped_column(String, primary_key=True)
    figi: Mapped[str | None]
    name: Mapped[str]
    issuer: Mapped[str | None]
    nominal: Mapped[float]
    maturity_date: Mapped[Date]
    segment: Mapped[str | None]
    updated_at: Mapped[DateTime]


class OrderbookSnapshotORM(Base):
    __tablename__ = "orderbook_snapshots"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    isin: Mapped[str]
    ts: Mapped[DateTime]
    bids_json: Mapped[dict] = mapped_column(JSON)
    asks_json: Mapped[dict] = mapped_column(JSON)
    best_bid: Mapped[float | None]
    best_ask: Mapped[float | None]


class EventORM(Base):
    __tablename__ = "events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    isin: Mapped[str]
    ts: Mapped[DateTime]
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
