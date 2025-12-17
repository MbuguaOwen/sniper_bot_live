from __future__ import annotations

import aiohttp
import logging

log = logging.getLogger("telegram")


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str) -> None:
        self.bot_token = (bot_token or "").strip().strip('"').strip("'")
        self.chat_id = (chat_id or "").strip().strip('"').strip("'")
        self._warned_disabled = False

    @property
    def enabled(self) -> bool:
        return bool(self.bot_token) and bool(self.chat_id)

    async def send(self, text: str, parse_mode: str = "HTML") -> bool:
        if not self.enabled:
            if not self._warned_disabled:
                log.warning("Telegram disabled (missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID) — alerts will be dropped.")
                self._warned_disabled = True
            return False
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=10) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        log.warning("Telegram send failed: %s %s", resp.status, body[:200])
                        return False
                    return True
        except Exception as e:
            log.warning("Telegram send exception: %s", e)
            return False
