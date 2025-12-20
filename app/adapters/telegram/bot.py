from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Dict
from aiogram import Bot

from .formatters import format_message
from ...domain.models import Event, Instrument
from ...settings import Settings

logger = logging.getLogger(__name__)


class TelegramBot:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.enabled = bool(settings.telegram_bot_token)
        self.bot = Bot(token=settings.telegram_bot_token) if self.enabled else None
        self._instrument_last_sent: Dict[str, float] = defaultdict(float)
        self._global_lock = asyncio.Lock()
        self._last_global_sent: float | None = None
        self._rate_limit_interval = 1.0

    def _is_eligible(self, event: Event, instrument: Instrument) -> bool:
        payload_eligible = (event.payload or {}).get("eligible")
        if payload_eligible is False:
            return False
        return bool(instrument.eligible)

    def _in_cooldown(self, instrument: Instrument) -> bool:
        cooldown_seconds = self.settings.alert_cooldown_min * 60
        now = asyncio.get_event_loop().time()
        last_sent = self._instrument_last_sent.get(instrument.isin)
        if last_sent and now - last_sent < cooldown_seconds:
            remaining = cooldown_seconds - (now - last_sent)
            logger.info("[TG COOLDOWN] %s %.1fs remaining", instrument.isin, remaining)
            return True
        return False

    async def _wait_global_rate_limit(self):
        async with self._global_lock:
            now = asyncio.get_event_loop().time()
            if self._last_global_sent is None:
                self._last_global_sent = now
                return
            elapsed = now - self._last_global_sent
            if elapsed < self._rate_limit_interval:
                await asyncio.sleep(self._rate_limit_interval - elapsed)
            self._last_global_sent = asyncio.get_event_loop().time()

    async def send_event(self, event: Event, instrument: Instrument):
        if not self._is_eligible(event, instrument):
            logger.info("[TG SKIP] not eligible: %s", instrument.isin)
            return
        if self._in_cooldown(instrument):
            return

        message = format_message(event, instrument, self.settings.public_dashboard_url)
        await self._wait_global_rate_limit()

        if not self.enabled:
            logger.info("[MOCK TG] %s", message)
        else:
            await self.bot.send_message(chat_id=self.settings.telegram_chat_id, text=message, parse_mode="Markdown")

        self._instrument_last_sent[instrument.isin] = asyncio.get_event_loop().time()

    async def close(self):
        if self.bot:
            await self.bot.session.close()
