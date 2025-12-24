from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timezone
from typing import Any, Iterable

import httpx

logger = logging.getLogger(__name__)
_BOND_EVENTS_SEMAPHORE = asyncio.Semaphore(4)


class TInvestRestClient:
    BASE_URL = "https://invest-public-api.tinkoff.ru"

    def __init__(self, token: str | None, *, transport: httpx.AsyncBaseTransport | None = None, base_url: str | None = None):
        self.token = token
        self.enabled = bool(token)
        self.base_url = base_url or self.BASE_URL
        self._client = httpx.AsyncClient(base_url=self.base_url, transport=transport)

    @property
    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    async def list_bonds(self) -> list[dict[str, Any]]:
        """Load bonds from real T-Invest REST API.

        Returns raw instrument payloads so that mapping stays in adapters layer.
        """

        if not self.enabled:
            return []

        payload = {"instrumentStatus": "INSTRUMENT_STATUS_BASE"}
        url = "/rest/tinkoff.public.invest.api.contract.v1.InstrumentsService/Bonds"
        logger.info("Fetching bonds from T-Invest REST")
        resp = await self._client.post(url, headers=self._headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
        instruments = []
        for item in data.get("instruments", []):
            instruments.append(
                {
                    "isin": item.get("isin"),
                    "instrument_uid": item.get("instrumentUid") or item.get("uid"),
                    "figi": item.get("figi"),
                    "ticker": item.get("ticker"),
                    "class_code": item.get("classCode") or item.get("class_code"),
                    "name": item.get("name") or item.get("ticker"),
                    "issuer": item.get("issuerName") or item.get("name"),
                    "nominal": self._parse_money_value(item.get("nominal")) or 0,
                    "maturity_date": self._parse_datetime(item.get("maturityDate")),
                    "segment": item.get("sector"),
                    "amortization_flag": item.get("amortizationFlag"),
                }
            )
        return [i for i in instruments if i.get("isin") and i.get("maturity_date")]

    async def get_bond_events(
        self,
        instrument_id: str,
        *,
        from_dt: datetime | None = None,
        to_dt: datetime | None = None,
    ) -> list[dict[str, Any]]:
        if not self.enabled:
            return []

        url = "/rest/tinkoff.public.invest.api.contract.v1.InstrumentsService/GetBondEvents"
        payload = {
            "instrumentId": instrument_id,
            "type": "EVENT_TYPE_CALL",
        }
        if from_dt is not None:
            payload["from"] = self._format_datetime(from_dt)
        if to_dt is not None:
            payload["to"] = self._format_datetime(to_dt)
        logger.info("Fetching bond events for %s", instrument_id)

        max_attempts = 5
        max_delay = 30.0
        for attempt in range(max_attempts):
            async with _BOND_EVENTS_SEMAPHORE:
                resp = await self._client.post(url, headers=self._headers, json=payload)

            if resp.status_code != httpx.codes.TOO_MANY_REQUESTS:
                resp.raise_for_status()
                data = resp.json()
                return data.get("events", [])

            if attempt == max_attempts - 1:
                resp.raise_for_status()

            retry_after = self._parse_retry_after(resp.headers.get("Retry-After"))
            base_delay = min(2**attempt, max_delay)
            delay = min(base_delay + random.uniform(0, 1), max_delay)
            if retry_after is not None:
                delay = max(delay, retry_after)
            logger.warning(
                "Received 429 for %s, retrying in %.2fs (attempt %d/%d)",
                instrument_id,
                delay,
                attempt + 1,
                max_attempts,
            )
            await asyncio.sleep(delay)
        return []

    async def last_prices(self, instrument_ids: Iterable[str]) -> dict[str, float]:
        """Fetch last traded prices for provided instrument ids (figi/uid/isin)."""

        if not self.enabled:
            return {}

        url = "/rest/tinkoff.public.invest.api.contract.v1.MarketDataService/GetLastPrices"
        payload = {"instrumentId": list(instrument_ids)}
        resp = await self._client.post(url, headers=self._headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
        prices: dict[str, float] = {}
        for item in data.get("lastPrices", []):
            price_value = self._parse_quotation(item.get("price"))
            instrument_id = item.get("instrumentUid") or item.get("instrumentId") or item.get("figi")
            if instrument_id and price_value is not None:
                prices[instrument_id] = price_value
        return prices

    async def close(self) -> None:
        await self._client.aclose()

    def _parse_datetime(self, value: str | None) -> datetime | None:
        if not value:
            return None
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    def _format_datetime(self, value: datetime) -> str:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    def _parse_retry_after(self, value: str | None) -> float | None:
        if not value:
            return None
        try:
            return float(value)
        except ValueError:
            return None

    def _parse_money_value(self, value: Any) -> float | None:
        if not value:
            return None
        units = int(value.get("units", 0))
        nano = int(value.get("nano", 0))
        return units + nano / 1e9

    def _parse_quotation(self, value: Any) -> float | None:
        return self._parse_money_value(value)
