from __future__ import annotations

import asyncio
import logging
import math
import time
from dataclasses import dataclass
from typing import Dict, Optional

from .binance_rest import BinanceFuturesRest, SymbolFilters

log = logging.getLogger("trader")


def _round_step(x: float, step: float) -> float:
    if step <= 0:
        return x
    return math.floor(x / step) * step


def _round_tick(x: float, tick: float) -> float:
    if tick <= 0:
        return x
    return round(math.floor(x / tick) * tick, 10)


def parse_symbol_filters(exchange_info: dict) -> Dict[str, SymbolFilters]:
    out: Dict[str, SymbolFilters] = {}
    for s in exchange_info.get("symbols", []):
        sym = s.get("symbol")
        if not sym:
            continue
        step = None
        min_qty = None
        tick = None
        for f in s.get("filters", []):
            t = f.get("filterType")
            if t == "LOT_SIZE":
                step = float(f["stepSize"])
                min_qty = float(f["minQty"])
            elif t == "PRICE_FILTER":
                tick = float(f["tickSize"])
        if step is None or min_qty is None or tick is None:
            continue
        out[sym] = SymbolFilters(step_size=step, min_qty=min_qty, tick_size=tick)
    return out


@dataclass
class PositionState:
    side: str  # "LONG" or "SHORT"
    qty: float
    entry_price: float
    entry_time_ms: int
    time_exit_ms: int
    stop_trigger_price: float
    stop_algo_id: Optional[int] = None
    entry_order_id: Optional[int] = None
    entry_client_order_id: Optional[str] = None
    entry_status: str = "PENDING"  # OPEN_SENT, FILLED, REJECTED, etc.
    exit_reason: Optional[str] = None  # "time_exit" or "stop"
    exit_price: Optional[float] = None
    exit_time_ms: Optional[int] = None
    exit_order_id: Optional[int] = None

    def estimated_fees(self, fee_rate: float) -> float:
        """Approximate taker fees for entry + exit."""
        if fee_rate <= 0:
            return 0.0
        exit_px = self.exit_price if self.exit_price is not None else self.entry_price
        notional_entry = self.qty * self.entry_price
        notional_exit = self.qty * exit_px
        return float((notional_entry + notional_exit) * fee_rate)


class Trader:
    def __init__(
        self,
        rest: BinanceFuturesRest,
        filters: Dict[str, SymbolFilters],
        notional_usdt: float,
        leverage: int,
        stop_loss_pct: float,
        time_exit_seconds: int,
        trading_mode: str = "paper",
        fee_rate: float = 0.0004,
    ) -> None:
        self.rest = rest
        self.filters = filters
        self.notional_usdt = float(notional_usdt)
        self.leverage = int(leverage)
        self.stop_loss_pct = float(stop_loss_pct)
        self.time_exit_seconds = int(time_exit_seconds)
        self.trading_mode = trading_mode.lower().strip()
        self.fee_rate = float(fee_rate)
        self.positions: Dict[str, PositionState] = {}
        self._lock = asyncio.Lock()

    def has_position(self, symbol: str) -> bool:
        return symbol in self.positions

    async def drop_position(self, symbol: str, reason: str = "") -> None:
        """Force-remove internal position state (used when exchange says we're flat)."""
        async with self._lock:
            if symbol not in self.positions:
                return
            if self.trading_mode == "live":
                try:
                    await self.rest.cancel_all_open_algo_orders(symbol)
                except Exception:
                    pass
            self.positions.pop(symbol, None)
            log.info("Dropped position state for %s (%s)", symbol, reason)

    async def ensure_leverage(self, symbol: str) -> None:
        if self.trading_mode != "live":
            return
        try:
            await self.rest.set_leverage(symbol, self.leverage)
        except Exception as e:
            log.warning("set_leverage failed for %s: %s", symbol, e)

    async def open_position(self, symbol: str, direction: str, mark_price: float) -> Dict:
        """direction: 'LONG' or 'SHORT'"""
        async with self._lock:
            if symbol in self.positions:
                return {"status": "skip", "reason": "already_in_position"}

            filt = self.filters.get(symbol)
            if not filt:
                return {"status": "skip", "reason": "missing_symbol_filters"}

            qty = self.notional_usdt / float(mark_price)
            qty = _round_step(qty, filt.step_size)
            if qty < filt.min_qty:
                return {"status": "skip", "reason": f"qty<{filt.min_qty}"}

            if direction == "LONG":
                side = "BUY"
                stop_side = "SELL"
                stop_price = mark_price * (1.0 - self.stop_loss_pct)
            else:
                side = "SELL"
                stop_side = "BUY"
                stop_price = mark_price * (1.0 + self.stop_loss_pct)

            stop_price = _round_tick(stop_price, filt.tick_size)

            now_ms = int(time.time() * 1000)
            pos = PositionState(
                side=direction,
                qty=qty,
                entry_price=float(mark_price),
                entry_time_ms=now_ms,
                time_exit_ms=now_ms + self.time_exit_seconds * 1000,
                stop_trigger_price=float(stop_price),
            )

            if self.trading_mode != "live":
                pos.entry_status = "FILLED"
                self.positions[symbol] = pos
                log.info("[PAPER] OPEN %s %s qty=%.8f mark=%.2f stop=%.2f", symbol, direction, qty, mark_price, stop_price)
                return {
                    "status": "paper_open",
                    "symbol": symbol,
                    "direction": direction,
                    "qty": qty,
                    "mark_price": mark_price,
                    "stop": stop_price,
                    "pos": pos,
                }

            # LIVE
            await self.ensure_leverage(symbol)

            # Cancel existing conditional orders (best-effort)
            try:
                await self.rest.cancel_all_open_algo_orders(symbol)
            except Exception:
                pass

            try:
                order = await self.rest.place_market_order(symbol, side=side, quantity=qty, reduce_only=False)
            except Exception as e:
                log.warning("OPEN %s %s failed: %s", symbol, direction, e)
                return {"status": "error", "reason": "order_failed", "error": str(e)}

            pos.entry_order_id = int(order.get("orderId")) if order.get("orderId") is not None else None
            pos.entry_client_order_id = order.get("clientOrderId")
            pos.entry_status = "OPEN_SENT"

            stop_error = None
            stop_resp = None
            try:
                stop_resp = await self.rest.place_stop_market_close_all(
                    symbol, side=stop_side, trigger_price=stop_price, working_type="MARK_PRICE"
                )
                pos.stop_algo_id = int(stop_resp.get("algoId")) if "algoId" in stop_resp else None
            except Exception as e:
                stop_error = str(e)
                log.warning("STOP placement failed for %s: %s", symbol, e)

            self.positions[symbol] = pos
            log.info(
                "OPEN %s %s qty=%.8f mark=%.2f stop=%.2f algoId=%s",
                symbol,
                direction,
                qty,
                mark_price,
                stop_price,
                pos.stop_algo_id,
            )
            return {
                "status": "live_open_sent",
                "order": order,
                "stop": stop_resp,
                "stop_error": stop_error,
                "pos": pos,
                "qty": qty,
            }

    async def close_if_due(self, symbol: str, now_ms: int, mark_price: float) -> Optional[Dict]:
        async with self._lock:
            pos = self.positions.get(symbol)
            if not pos:
                return None
            if now_ms < pos.time_exit_ms:
                return None

            direction = pos.side
            qty = pos.qty
            pos.exit_reason = "time_exit"
            pos.exit_time_ms = now_ms

            if self.trading_mode != "live":
                self.positions.pop(symbol, None)
                log.info("[PAPER] TIME-EXIT %s %s qty=%.8f mark=%.2f", symbol, direction, qty, mark_price)
                pos.exit_price = float(mark_price)
                return {"status": "paper_close", "symbol": symbol, "direction": direction, "qty": qty, "mark_price": mark_price, "pos": pos}

            # LIVE: cancel outstanding algo stops first (best effort), then reduce-only market.
            try:
                await self.rest.cancel_all_open_algo_orders(symbol)
            except Exception as e:
                log.warning("cancel_all_open_algo_orders failed: %s", e)

            close_side = "SELL" if direction == "LONG" else "BUY"
            try:
                order = await self.rest.place_market_order(symbol, side=close_side, quantity=qty, reduce_only=True)
            except Exception as e:
                log.warning("TIME-EXIT order failed for %s: %s", symbol, e)
                return {"status": "error", "reason": "time_exit_failed", "error": str(e)}

            pos.exit_order_id = int(order.get("orderId")) if order.get("orderId") is not None else None
            filled_px = float(order.get("avgPrice") or order.get("price") or mark_price)
            pos.exit_price = filled_px

            self.positions.pop(symbol, None)
            log.info("TIME-EXIT %s %s qty=%.8f mark=%.2f", symbol, direction, qty, filled_px)
            return {
                "status": "live_close",
                "order": order,
                "symbol": symbol,
                "direction": direction,
                "qty": qty,
                "mark_price": filled_px,
                "pos": pos,
            }

    async def mark_entry_filled(self, symbol: str, price: float, event_time_ms: int, order_id: Optional[int] = None) -> Optional[PositionState]:
        async with self._lock:
            pos = self.positions.get(symbol)
            if not pos:
                return None
            pos.entry_status = "FILLED"
            px = float(price) if price and price > 0 else pos.entry_price
            pos.entry_price = float(px)
            pos.entry_time_ms = int(event_time_ms)
            if order_id is not None:
                pos.entry_order_id = order_id
            return pos

    async def mark_entry_rejected(self, symbol: str, reason: str = "REJECTED") -> Optional[PositionState]:
        async with self._lock:
            pos = self.positions.pop(symbol, None)
            if not pos:
                return None
            pos.entry_status = reason
            return pos

    async def mark_stop_filled(self, symbol: str, price: float, event_time_ms: int) -> Optional[PositionState]:
        async with self._lock:
            pos = self.positions.pop(symbol, None)
            if not pos:
                return None
            pos.exit_reason = "stop"
            px = float(price) if price and price > 0 else pos.stop_trigger_price
            pos.exit_price = float(px)
            pos.exit_time_ms = int(event_time_ms)
            return pos
