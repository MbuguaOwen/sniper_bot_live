from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Any
import os

import yaml


@dataclass(frozen=True)
class EngineConfig:
    symbols: List[str]
    bar_seconds: int
    donchian_window_minutes: int
    roll_window_bars: int
    min_trades_per_bar: int
    cooldown_seconds: int
    opposing_threshold_z: float
    z_cap_abs: float


@dataclass(frozen=True)
class ExecutionConfig:
    notional_usdt_per_trade: float
    leverage: int
    stop_loss_pct: float
    time_exit_seconds: int
    warmup_seconds: int


@dataclass(frozen=True)
class LoggingConfig:
    out_dir: str
    events_csv: str
    orders_csv: str
    trades_csv: str
    level: str


@dataclass(frozen=True)
class MonitorConfig:
    # Periodic liveness/health sampling
    sample_interval_seconds: int

    # Telegram heartbeat (optional)
    heartbeat_interval_seconds: int
    telegram_heartbeat: bool
    telegram_min_gap_seconds: int

    # Staleness detection
    stale_aggtrade_seconds: int
    stale_kline_seconds: int
    stale_repeat_seconds: int
    stale_force_reconnect_seconds: int

    # Health log
    health_csv: str


@dataclass(frozen=True)
class AppConfig:
    engine: EngineConfig
    execution: ExecutionConfig
    logging: LoggingConfig
    monitor: MonitorConfig


def _require(cond: bool, msg: str) -> None:
    if not cond:
        raise ValueError(msg)


def _as_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def load_config(path: str | Path) -> AppConfig:
    path = Path(path)
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}

    _require("engine" in data, "Missing 'engine' section in config.")
    _require("execution" in data, "Missing 'execution' section in config.")
    _require("logging" in data, "Missing 'logging' section in config.")

    eng = data["engine"]
    exe = data["execution"]
    log = data["logging"]
    mon = data.get("monitor", {})

    engine = EngineConfig(
        symbols=list(eng["symbols"]),
        bar_seconds=int(eng["bar_seconds"]),
        donchian_window_minutes=int(eng["donchian_window_minutes"]),
        roll_window_bars=int(eng["roll_window_bars"]),
        min_trades_per_bar=int(eng["min_trades_per_bar"]),
        cooldown_seconds=int(eng["cooldown_seconds"]),
        opposing_threshold_z=float(eng["opposing_threshold_z"]),
        z_cap_abs=float(eng["z_cap_abs"]),
    )

    execution = ExecutionConfig(
        notional_usdt_per_trade=float(exe["notional_usdt_per_trade"]),
        leverage=int(exe["leverage"]),
        stop_loss_pct=float(exe["stop_loss_pct"]),
        time_exit_seconds=int(exe["time_exit_seconds"]),
        warmup_seconds=int(exe.get("warmup_seconds", 0)),
    )

    logging = LoggingConfig(
        out_dir=str(log["out_dir"]),
        events_csv=str(log["events_csv"]),
        orders_csv=str(log["orders_csv"]),
        trades_csv=str(log.get("trades_csv", "trades.csv")),
        level=str(log.get("level", "INFO")).upper(),
    )

    monitor = MonitorConfig(
        sample_interval_seconds=int(mon.get("sample_interval_seconds", 5)),
        heartbeat_interval_seconds=int(mon.get("heartbeat_interval_seconds", 1800)),
        telegram_heartbeat=_as_bool(mon.get("telegram_heartbeat", False), False),
        telegram_min_gap_seconds=int(mon.get("telegram_min_gap_seconds", 300)),
        stale_aggtrade_seconds=int(mon.get("stale_aggtrade_seconds", 60)),
        stale_kline_seconds=int(mon.get("stale_kline_seconds", 180)),
        stale_repeat_seconds=int(mon.get("stale_repeat_seconds", 600)),
        stale_force_reconnect_seconds=int(mon.get("stale_force_reconnect_seconds", 240)),
        health_csv=str(mon.get("health_csv", "health.csv")),
    )

    _require(engine.bar_seconds in (1, 2, 5, 10), "bar_seconds must be one of {1,2,5,10}.")
    _require(engine.donchian_window_minutes >= 20, "donchian_window_minutes must be >= 20.")
    _require(engine.roll_window_bars >= 60, "roll_window_bars must be >= 60.")
    _require(engine.min_trades_per_bar >= 0, "min_trades_per_bar must be >= 0.")
    _require(engine.cooldown_seconds >= 0, "cooldown_seconds must be >= 0.")
    _require(engine.opposing_threshold_z >= 0, "opposing_threshold_z must be >= 0.")
    _require(engine.z_cap_abs >= 0, "z_cap_abs must be >= 0.")

    _require(execution.notional_usdt_per_trade > 0, "notional_usdt_per_trade must be > 0.")
    _require(execution.leverage >= 1, "leverage must be >= 1.")
    _require(execution.stop_loss_pct > 0, "stop_loss_pct must be > 0.")
    _require(execution.time_exit_seconds >= engine.bar_seconds, "time_exit_seconds too small.")
    _require(execution.warmup_seconds >= 0, "warmup_seconds must be >= 0.")

    _require(monitor.sample_interval_seconds >= 1, "monitor.sample_interval_seconds must be >= 1.")
    _require(monitor.heartbeat_interval_seconds >= 60, "monitor.heartbeat_interval_seconds must be >= 60.")
    _require(monitor.telegram_min_gap_seconds >= 30, "monitor.telegram_min_gap_seconds must be >= 30.")
    _require(monitor.stale_aggtrade_seconds > 0, "monitor.stale_aggtrade_seconds must be > 0.")
    _require(monitor.stale_kline_seconds > 0, "monitor.stale_kline_seconds must be > 0.")
    _require(monitor.stale_repeat_seconds > 0, "monitor.stale_repeat_seconds must be > 0.")
    _require(monitor.stale_force_reconnect_seconds >= 30, "monitor.stale_force_reconnect_seconds must be >= 30.")

    return AppConfig(engine=engine, execution=execution, logging=logging, monitor=monitor)


def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    return int(v)


def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    if v is None:
        return default
    return v
