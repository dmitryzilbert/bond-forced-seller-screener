from __future__ import annotations

import logging
import asyncio
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

    async def send_event(self, event: Event, instrument: Instrument):
        message = format_message(event, instrument, self.settings.public_dashboard_url)
        if not self.enabled:
            logger.info("[MOCK TG] %s", message)
            return
        await self.bot.send_message(chat_id=self.settings.telegram_chat_id, text=message, parse_mode="Markdown")

    async def close(self):
        if self.bot:
            await self.bot.session.close()
