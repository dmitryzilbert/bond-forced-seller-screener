from __future__ import annotations

import logging
from datetime import datetime, time, timezone
from typing import Iterable

from .rest import TInvestRestClient
from .mapping import map_instrument_payload
from ...domain.models import Instrument, OrderBookSnapshot

logger = logging.getLogger(__name__)


class _DisabledStream:
    enabled = False

    async def subscribe(self, instruments: Iterable[Instrument]):  # pragma: no cover - trivial guard
        if False:
            yield instruments


class TInvestClient:
    def __init__(
        self,
        token: str | None,
        account_id: str | None,
        depth: int = 10,
        *,
        rest_transport=None,
        dry_run: bool = False,
        app_env: str = "prod",
        grpc_target_prod: str | None = None,
        grpc_target_sandbox: str | None = None,
        ssl_ca_bundle: str | None = None,
        stream_heartbeat_interval_s: float = 20.0,
    ):
        self.rest = TInvestRestClient(token, transport=rest_transport)
        if grpc_target_prod is None or grpc_target_sandbox is None:
            raise ValueError("gRPC target configuration is required for grpc transport")
        if token:
            from .grpc_stream import TInvestGrpcStream

            self.stream = TInvestGrpcStream(
                token,
                depth=depth,
                app_env=app_env,
                target_prod=grpc_target_prod,
                target_sandbox=grpc_target_sandbox,
                ssl_ca_bundle=ssl_ca_bundle,
                dry_run=dry_run,
                stream_heartbeat_interval_s=stream_heartbeat_interval_s,
            )
        else:
            self.stream = _DisabledStream()
        self.account_id = account_id

    async def list_bonds(self) -> list[Instrument]:
        if not self.rest.enabled:
            logger.info("TInvest REST disabled (no token)")
            return []
        payloads = await self.rest.list_bonds()
        return [map_instrument_payload(p) for p in payloads]

    async def find_instrument(
        self,
        *,
        query: str,
        instrument_kind: str = "BOND",
        timeout: float | None = None,
    ) -> dict[str, str | bool | None] | None:
        if not self.rest.enabled:
            return None
        payload = await self.rest.find_instrument(
            query=query,
            instrument_kind=instrument_kind,
            timeout=timeout,
        )
        if not payload:
            return None
        coupon_type = payload.get("couponType") or payload.get("coupon_type")
        floating_flag = payload.get("floatingCouponFlag") or payload.get("floating_coupon_flag")
        if isinstance(coupon_type, str) and floating_flag is None:
            if coupon_type.strip().upper() in {"FLOAT", "FLOATING", "VARIABLE"}:
                floating_flag = True
            elif coupon_type.strip().upper() in {"FIXED", "CONSTANT"}:
                floating_flag = False
        return {
            "instrument_uid": payload.get("instrumentUid") or payload.get("uid"),
            "figi": payload.get("figi"),
            "ticker": payload.get("ticker"),
            "class_code": payload.get("classCode") or payload.get("class_code"),
            "floating_coupon_flag": floating_flag,
        }

    async def has_future_call_offer(self, instrument: Instrument) -> bool | None:
        if not self.rest.enabled:
            return None
        instrument_id = instrument.figi or instrument.isin
        if not instrument_id:
            return None
        if instrument.maturity_date is None:
            return None
        from_dt = datetime.now(timezone.utc)
        to_dt = datetime.combine(instrument.maturity_date, time.min, tzinfo=timezone.utc)
        events = await self.rest.get_bond_events(instrument_id, from_dt=from_dt, to_dt=to_dt)
        now = datetime.utcnow().date()
        for event in events:
            event_type = event.get("eventType") or event.get("type") or event.get("event_type")
            if not event_type:
                continue
            if str(event_type).upper().endswith("CALL") or str(event_type).upper() == "CALL":
                date_str = (
                    event.get("eventDate")
                    or event.get("callDate")
                    or event.get("date")
                    or event.get("recordDate")
                )
                if date_str:
                    try:
                        event_date = datetime.fromisoformat(str(date_str).replace("Z", "+00:00")).date()
                        if event_date >= now:
                            return True
                    except ValueError:
                        continue
                else:
                    return True
        return False

    async def stream_orderbooks(
        self,
        instruments: Iterable[Instrument],
        *,
        on_subscribed=None,
    ):
        if not self.stream.enabled:
            logger.info("TInvest stream disabled (no token)")
            return
        async for snapshot in self.stream.subscribe(instruments, on_subscribed=on_subscribed):
            yield snapshot

    async def fetch_orderbook_snapshot(
        self,
        instrument: Instrument,
        *,
        depth: int,
        timeout: float | None = None,
    ) -> OrderBookSnapshot | None:
        if not self.stream.enabled:
            return None
        return await self.stream.fetch_orderbook_snapshot(instrument, depth=depth, timeout=timeout)

    async def fetch_orderbook_response(
        self,
        instrument: Instrument,
        *,
        depth: int,
        timeout: float | None = None,
    ):
        if not self.stream.enabled:
            return None
        return await self.stream.fetch_orderbook_response(instrument, depth=depth, timeout=timeout)

    def build_orderbook_snapshot(self, response, instrument: Instrument) -> OrderBookSnapshot | None:
        if not self.stream.enabled:
            return None
        return self.stream.build_snapshot_from_response(response, instrument)
