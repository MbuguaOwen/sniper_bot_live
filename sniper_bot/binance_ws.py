from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from typing import Callable, Awaitable, Optional, List

import websockets

log = logging.getLogger("binance_ws")


class BinanceMarketStream:
    def __init__(
        self,
        ws_base: str,
        streams: List[str],
        on_message: Callable[[dict], Awaitable[None]],
        name: str = "stream",
        combined: bool = True,
    ) -> None:
        self.ws_base = ws_base.rstrip("/")
        self.streams = streams
        self.on_message = on_message
        self.name = name
        self._stop = asyncio.Event()
        self._reconnect = asyncio.Event()
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self.combined = combined

    def stop(self) -> None:
        self._stop.set()

    def request_reconnect(self) -> None:
        """Ask the stream loop to reconnect without stopping the process."""
        if self._stop.is_set():
            return
        self._reconnect.set()
        if self._ws and not self._ws.closed:
            try:
                asyncio.get_running_loop().create_task(self._ws.close(code=1012, reason="watchdog_reconnect"))
            except RuntimeError:
                # No running loop; let the natural backoff handle reconnect.
                pass

    def _url(self) -> str:
        if self.combined:
            return f"{self.ws_base}/stream?streams=" + "/".join(self.streams)
        if len(self.streams) != 1:
            raise ValueError("Non-combined mode requires exactly one stream name.")
        return f"{self.ws_base}/ws/{self.streams[0]}"

    async def run_forever(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                url = self._url()
                log.info("[%s] connecting: %s", self.name, url)
                async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                    self._ws = ws
                    self._reconnect.clear()
                    backoff = 1.0
                    log.info("[%s] connected", self.name)
                    async for raw in ws:
                        if self._stop.is_set():
                            break
                        if self._reconnect.is_set():
                            log.info("[%s] reconnect requested", self.name)
                            break
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        await self.on_message(msg)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning("[%s] ws error: %s", self.name, e)

            # jittered reconnect
            self._ws = None
            await asyncio.sleep(backoff + random.random() * 0.25)
            backoff = min(backoff * 1.7, 30.0)

        self._ws = None
        log.info("[%s] stopped", self.name)
