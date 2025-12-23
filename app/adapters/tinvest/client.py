from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable

from .rest import TInvestRestClient
from .stream import TInvestStream
from .mapping import map_instrument_payload
from ...domain.models import Instrument, OrderBookSnapshot

logger = logging.getLogger(__name__)


class TInvestClient:
    def __init__(
        self,
        token: str | None,
        account_id: str | None,
        depth: int = 10,
        *,
        rest_transport=None,
        stream_connector=None,
        dry_run: bool = False,
        stream_transport: str = "grpc",
        app_env: str = "prod",
        grpc_target_prod: str | None = None,
        grpc_target_sandbox: str | None = None,
        ws_url: str | None = None,
        ws_protocol: str | None = None,
        ssl_ca_bundle: str | None = None,
        ssl_insecure: bool = False,
    ):
        self.rest = TInvestRestClient(token, transport=rest_transport)
        if stream_transport == "grpc":
            if grpc_target_prod is None or grpc_target_sandbox is None:
                raise ValueError("gRPC target configuration is required for grpc transport")
            from .grpc_stream import TInvestGrpcStream

            self.stream = TInvestGrpcStream(
                token,
                depth=depth,
                app_env=app_env,
                target_prod=grpc_target_prod,
                target_sandbox=grpc_target_sandbox,
                ssl_ca_bundle=ssl_ca_bundle,
                dry_run=dry_run,
            )
        else:
            self.stream = TInvestStream(
                token,
                depth=depth,
                connector=stream_connector,
                dry_run=dry_run,
                ws_url=ws_url,
                ws_protocol=ws_protocol,
                ssl_ca_bundle=ssl_ca_bundle,
                ssl_insecure=ssl_insecure,
            )
        self.account_id = account_id

    async def list_bonds(self) -> list[Instrument]:
        if not self.rest.enabled:
            logger.info("TInvest REST disabled (no token)")
            return []
        payloads = await self.rest.list_bonds()
        return [map_instrument_payload(p) for p in payloads]

    async def has_future_call_offer(self, instrument: Instrument) -> bool | None:
        if not self.rest.enabled:
            return None
        instrument_id = instrument.figi or instrument.isin
        if not instrument_id:
            return None
        events = await self.rest.get_bond_events(instrument_id)
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

    async def stream_orderbooks(self, instruments: Iterable[Instrument]):
        if not self.stream.enabled:
            logger.info("TInvest stream disabled (no token)")
            return
        async for snapshot in self.stream.subscribe(instruments):
            yield snapshot
