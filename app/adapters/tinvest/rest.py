from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class TInvestRestClient:
    BASE_URL = "https://api-invest.tinkoff.ru"

    def __init__(self, token: str | None):
        self.token = token
        self.enabled = bool(token)
        self._client = httpx.AsyncClient()

    async def list_bonds(self) -> list[dict[str, Any]]:
        if not self.enabled:
            return []
        headers = {"Authorization": f"Bearer {self.token}"}
        # TODO: replace with real endpoint
        logger.info("Fetching bonds from T-Invest REST (stub)")
        resp = await self._client.get(f"{self.BASE_URL}/bonds", headers=headers)
        resp.raise_for_status()
        data = resp.json()
        return data.get("instruments", [])

    async def close(self) -> None:
        await self._client.aclose()
