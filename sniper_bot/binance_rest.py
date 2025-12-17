from __future__ import annotations

import aiohttp
import hashlib
import hmac
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib.parse import urlencode

log = logging.getLogger("binance_rest")


@dataclass
class SymbolFilters:
    step_size: float
    min_qty: float
    tick_size: float


class BinanceFuturesRest:
    def __init__(self, api_key: str, api_secret: str, base_url: str) -> None:
        self.api_key = api_key
        self.api_secret = api_secret.encode("utf-8")
        self.base_url = base_url.rstrip("/")
        self._session: Optional[aiohttp.ClientSession] = None
        self._time_offset_ms: int = 0  # local - server

    async def __aenter__(self) -> "BinanceFuturesRest":
        self._session = aiohttp.ClientSession()
        await self.sync_time()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    def _headers(self) -> Dict[str, str]:
        return {"X-MBX-APIKEY": self.api_key}

    def _sign(self, params: Dict[str, Any]) -> str:
        qs = urlencode(params, doseq=True)
        sig = hmac.new(self.api_secret, qs.encode("utf-8"), hashlib.sha256).hexdigest()
        return f"{qs}&signature={sig}"

    def _ts(self) -> int:
        # Binance expects milliseconds
        return int(time.time() * 1000) - self._time_offset_ms

    async def sync_time(self) -> None:
        server = await self._get("/fapi/v1/time")
        server_ms = int(server["serverTime"])
        local_ms = int(time.time() * 1000)
        self._time_offset_ms = local_ms - server_ms
        log.info("Time sync: local-server offset = %+d ms", self._time_offset_ms)

    async def _get(self, path: str, params: Optional[Dict[str, Any]] = None, signed: bool = False) -> Any:
        assert self._session is not None, "Session not started"
        url = f"{self.base_url}{path}"
        if params is None:
            params = {}
        if signed:
            params = dict(params)
            params["timestamp"] = self._ts()
            params.setdefault("recvWindow", 5000)
            qs = self._sign(params)
            url = f"{url}?{qs}"
            async with self._session.get(url, headers=self._headers(), timeout=10) as resp:
                data = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(f"GET {path} failed: {resp.status} {data}")
                return data
        else:
            async with self._session.get(url, params=params, timeout=10) as resp:
                data = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(f"GET {path} failed: {resp.status} {data}")
                return data

    async def _post(self, path: str, params: Dict[str, Any], signed: bool = True) -> Any:
        assert self._session is not None, "Session not started"
        url = f"{self.base_url}{path}"
        if signed:
            p = dict(params)
            p["timestamp"] = self._ts()
            p.setdefault("recvWindow", 5000)
            body = self._sign(p)
            headers = self._headers()
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            async with self._session.post(url, data=body, headers=headers, timeout=10) as resp:
                data = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(f"POST {path} failed: {resp.status} {data}")
                return data
        else:
            async with self._session.post(url, params=params, timeout=10) as resp:
                data = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(f"POST {path} failed: {resp.status} {data}")
                return data

    async def _delete(self, path: str, params: Dict[str, Any], signed: bool = True) -> Any:
        assert self._session is not None, "Session not started"
        url = f"{self.base_url}{path}"
        if signed:
            p = dict(params)
            p["timestamp"] = self._ts()
            p.setdefault("recvWindow", 5000)
            qs = self._sign(p)
            url = f"{url}?{qs}"
            async with self._session.delete(url, headers=self._headers(), timeout=10) as resp:
                data = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(f"DELETE {path} failed: {resp.status} {data}")
                return data
        else:
            async with self._session.delete(url, params=params, timeout=10) as resp:
                data = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(f"DELETE {path} failed: {resp.status} {data}")
                return data

    # ---------- Public market data ----------

    async def exchange_info(self) -> Any:
        return await self._get("/fapi/v1/exchangeInfo")

    async def klines_1m(self, symbol: str, limit: int = 200) -> Any:
        return await self._get("/fapi/v1/klines", params={"symbol": symbol, "interval": "1m", "limit": limit})

    async def mark_price(self, symbol: str) -> float:
        data = await self._get("/fapi/v1/premiumIndex", params={"symbol": symbol})
        return float(data["markPrice"])

    # ---------- Account/trade ----------

    async def set_leverage(self, symbol: str, leverage: int) -> Any:
        return await self._post("/fapi/v1/leverage", params={"symbol": symbol, "leverage": leverage})

    async def place_market_order(self, symbol: str, side: str, quantity: float, reduce_only: bool = False) -> Any:
        p = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": self._fmt(quantity),
            "newOrderRespType": "RESULT",
        }
        if reduce_only:
            p["reduceOnly"] = "true"
        return await self._post("/fapi/v1/order", params=p)

    async def cancel_all_open_orders(self, symbol: str) -> Any:
        return await self._delete("/fapi/v1/allOpenOrders", params={"symbol": symbol})

    # --- New algo conditional order endpoint (required after 2025-12-09 migration) ---
    async def place_stop_market_close_all(self, symbol: str, side: str, trigger_price: float, working_type: str = "CONTRACT_PRICE") -> Any:
        # algoType must be CONDITIONAL, type STOP_MARKET, closePosition=true, and quantity must be omitted.
        p = {
            "algoType": "CONDITIONAL",
            "symbol": symbol,
            "side": side,
            "type": "STOP_MARKET",
            "triggerPrice": self._fmt(trigger_price),
            "closePosition": "true",
            "workingType": working_type,
        }
        return await self._post("/fapi/v1/algoOrder", params=p)

    async def cancel_all_open_algo_orders(self, symbol: str) -> Any:
        # endpoint is /fapi/v1/algoOpenOrders (DELETE). Docs name: Cancel All Algo Open Orders
        return await self._delete("/fapi/v1/algoOpenOrders", params={"symbol": symbol})

    async def position_risk(self, symbol: str) -> Any:
        return await self._get("/fapi/v2/positionRisk", params={"symbol": symbol}, signed=True)

    # ---------- User data stream (listenKey) ----------

    async def start_user_stream(self) -> str:
        """Create a user data listenKey for private stream events."""
        assert self._session is not None, "Session not started"
        url = f"{self.base_url}/fapi/v1/listenKey"
        async with self._session.post(url, headers=self._headers(), timeout=10) as resp:
            data = await resp.json()
            if resp.status != 200:
                raise RuntimeError(f"POST /listenKey failed: {resp.status} {data}")
            return str(data.get("listenKey", ""))

    async def keepalive_user_stream(self, listen_key: str) -> None:
        assert self._session is not None, "Session not started"
        url = f"{self.base_url}/fapi/v1/listenKey"
        async with self._session.put(url, params={"listenKey": listen_key}, headers=self._headers(), timeout=10) as resp:
            if resp.status != 200:
                data = await resp.json()
                raise RuntimeError(f"PUT /listenKey failed: {resp.status} {data}")

    async def close_user_stream(self, listen_key: str) -> None:
        assert self._session is not None, "Session not started"
        url = f"{self.base_url}/fapi/v1/listenKey"
        async with self._session.delete(url, params={"listenKey": listen_key}, headers=self._headers(), timeout=10) as resp:
            if resp.status != 200:
                data = await resp.json()
                raise RuntimeError(f"DELETE /listenKey failed: {resp.status} {data}")

    @staticmethod
    def _fmt(x: float) -> str:
        # Binance accepts decimals as strings. Avoid scientific notation.
        s = f"{x:.16f}"
        s = s.rstrip("0").rstrip(".")
        return s if s else "0"
