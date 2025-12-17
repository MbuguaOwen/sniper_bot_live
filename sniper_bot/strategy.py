from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Optional, Tuple, Dict, Any, List

import numpy as np


IQR_SCALE = 0.7413  # converts IQR to ~std for normal data


class RollingRobustZ:
    def __init__(self, window: int) -> None:
        self.window = int(window)
        self.values: Deque[float] = deque(maxlen=self.window)

    def push(self, x: float) -> None:
        self.values.append(float(x))

    def ready(self) -> bool:
        return len(self.values) >= self.window

    def z(self, x: float) -> float:
        # Robust z = (x - median) / (IQR * 0.7413)
        arr = np.asarray(self.values, dtype=float)
        med = float(np.median(arr))
        q25, q75 = np.percentile(arr, [25, 75])
        iqr = float(q75 - q25)
        denom = iqr * IQR_SCALE
        if denom <= 1e-12:
            return 0.0
        return float((x - med) / denom)


@dataclass
class DonchianState:
    window: int
    highs: Deque[float]
    lows: Deque[float]
    last_closed_minute_ms: int = 0

    @classmethod
    def create(cls, window: int) -> "DonchianState":
        return cls(window=window, highs=deque(maxlen=window), lows=deque(maxlen=window))

    def push_candle(self, high: float, low: float, close_time_ms: int) -> None:
        # Only accept strictly newer closed candles
        if close_time_ms <= self.last_closed_minute_ms:
            return
        self.highs.append(float(high))
        self.lows.append(float(low))
        self.last_closed_minute_ms = int(close_time_ms)

    def ready(self) -> bool:
        return len(self.highs) >= self.window and len(self.lows) >= self.window

    def bands(self) -> tuple[float, float]:
        return (max(self.highs), min(self.lows))


@dataclass
class Bar5s:
    start_ms: int
    open: float
    high: float
    low: float
    close: float
    delta_tick: int
    trades: int
    last_trade_ms: int

    def mid(self) -> float:
        return 0.5 * (self.high + self.low)


class TradeBarAccumulator:
    def __init__(self, bar_ms: int) -> None:
        self.bar_ms = int(bar_ms)
        self.cur_start_ms: Optional[int] = None
        self.open: float = 0.0
        self.high: float = 0.0
        self.low: float = 0.0
        self.close: float = 0.0
        self.delta_tick: int = 0
        self.trades: int = 0
        self.last_trade_ms: int = 0
        self._has_price: bool = False

    def _start_new(self, start_ms: int, price: float, trade_ms: int) -> None:
        self.cur_start_ms = start_ms
        self.open = self.high = self.low = self.close = float(price)
        self.delta_tick = 0
        self.trades = 0
        self.last_trade_ms = int(trade_ms)
        self._has_price = True

    def add_trade(self, trade_time_ms: int, price: float, is_buyer_maker: bool) -> List[Bar5s]:
        """Returns list of finalized bars if this trade advances time."""
        out: List[Bar5s] = []
        start_ms = (int(trade_time_ms) // self.bar_ms) * self.bar_ms

        if self.cur_start_ms is None:
            self._start_new(start_ms, price, trade_time_ms)

        # Fill missing bars if gap
        assert self.cur_start_ms is not None
        while start_ms > self.cur_start_ms:
            # finalize current bar
            out.append(self.finalize())
            # advance by one bar; carry-forward close if we have one
            self._start_new(self.cur_start_ms + self.bar_ms, self.close, self.cur_start_ms + self.bar_ms)

        # Update current bar with this trade
        if not self._has_price:
            self._start_new(start_ms, price, trade_time_ms)

        p = float(price)
        self.high = max(self.high, p)
        self.low = min(self.low, p)
        self.close = p
        self.trades += 1
        self.last_trade_ms = int(trade_time_ms)

        # Tick delta: buyer taker => +1, seller taker => -1
        if is_buyer_maker:
            self.delta_tick -= 1
        else:
            self.delta_tick += 1

        return out

    def finalize(self) -> Bar5s:
        assert self.cur_start_ms is not None
        bar = Bar5s(
            start_ms=self.cur_start_ms,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            delta_tick=int(self.delta_tick),
            trades=int(self.trades),
            last_trade_ms=int(self.last_trade_ms),
        )
        return bar
