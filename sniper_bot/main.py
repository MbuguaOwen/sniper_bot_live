from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import time
from pathlib import Path
from typing import Dict, Any, Optional, List

from dotenv import load_dotenv

from .config import load_config, env_bool, env_int, env_str
from .logging_utils import setup_logging, CsvLogger
from .binance_rest import BinanceFuturesRest
from .binance_ws import BinanceMarketStream
from .strategy import RollingRobustZ, DonchianState, TradeBarAccumulator
from .trader import Trader, parse_symbol_filters
from .telegram import TelegramNotifier

log = logging.getLogger("sniper_main")


def _ms_to_iso(ms: int) -> str:
    # Avoid pandas dependency for formatting
    import datetime as _dt

    return _dt.datetime.utcfromtimestamp(ms / 1000).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


async def run_startup_self_test(rest: BinanceFuturesRest, ws_base: str, symbols: List[str], timeout_s: int = 120) -> None:
    """Quick connectivity check: REST + at least one aggTrade and closed kline."""
    if not symbols:
        raise RuntimeError("No symbols configured for self-test.")

    sym = symbols[0]
    log.info("Running startup self-test (symbol=%s timeout=%ss)...", sym, timeout_s)
    await rest.exchange_info()
    await rest.mark_price(sym)

    got_agg = asyncio.Event()
    got_kline = asyncio.Event()

    async def on_msg(msg: dict) -> None:
        data = msg.get("data", msg)
        etype = data.get("e")
        if etype == "aggTrade":
            got_agg.set()
        elif etype == "kline":
            k = data.get("k", {})
            if k.get("x"):
                got_kline.set()
        if got_agg.is_set() and got_kline.is_set():
            selftest_ws.stop()

    streams = [f"{sym.lower()}@aggTrade", f"{sym.lower()}@kline_1m"]
    selftest_ws = BinanceMarketStream(ws_base=ws_base, streams=streams, on_message=on_msg, name="selftest")
    task = asyncio.create_task(selftest_ws.run_forever())
    try:
        await asyncio.wait_for(asyncio.gather(got_agg.wait(), got_kline.wait()), timeout=timeout_s)
    except Exception as e:
        selftest_ws.stop()
        await asyncio.gather(task, return_exceptions=True)
        raise RuntimeError(f"Startup self-test failed within {timeout_s}s: {e}")

    selftest_ws.stop()
    await asyncio.gather(task, return_exceptions=True)
    log.info("Startup self-test passed.")


async def main_async(config_path: str, self_test: bool = False, self_test_timeout: int = 120) -> None:
    load_dotenv()

    cfg = load_config(config_path)
    setup_logging(cfg.logging.level)

    is_testnet = env_bool("BINANCE_TESTNET", True)
    trading_mode = env_str("TRADING_MODE", "paper").lower().strip()
    max_open_positions = env_int("MAX_OPEN_POSITIONS", 1)
    self_test_strict = env_bool("SELF_TEST_STRICT", False)

    api_key = env_str("BINANCE_API_KEY", "").strip()
    api_secret = env_str("BINANCE_API_SECRET", "").strip()

    if trading_mode == "live" and (not api_key or not api_secret):
        raise SystemExit(
            "TRADING_MODE=live requires BINANCE_API_KEY and BINANCE_API_SECRET in environment/.env"
        )

    # Endpoints (market streams + REST). You can override in env if needed.
    rest_default = "https://demo-fapi.binance.com" if is_testnet else "https://fapi.binance.com"
    ws_default = "wss://fstream.binancefuture.com" if is_testnet else "wss://fstream.binance.com"
    rest_base = (env_str("BINANCE_REST_BASE", rest_default) or rest_default).strip()
    ws_base = (env_str("BINANCE_WS_BASE", ws_default) or ws_default).strip()

    out_dir = Path(cfg.logging.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    events_csv = CsvLogger(
        path=out_dir / cfg.logging.events_csv,
        fieldnames=[
            "ts_iso",
            "symbol",
            "bar_start_ms",
            "bar_open",
            "bar_high",
            "bar_low",
            "bar_close",
            "delta_tick",
            "delta_z",
            "trades",
            "donchian_high",
            "donchian_low",
            "touch_low",
            "touch_high",
            "signal",
            "reason",
        ],
    )
    orders_csv = CsvLogger(
        path=out_dir / cfg.logging.orders_csv,
        fieldnames=["ts_iso", "symbol", "action", "direction", "qty", "price", "status", "details"],
    )
    trades_csv = CsvLogger(
        path=out_dir / cfg.logging.trades_csv,
        fieldnames=[
            "symbol",
            "direction",
            "qty",
            "entry_time",
            "entry_price",
            "exit_time",
            "exit_price",
            "reason",
            "fees_est",
        ],
    )
    health_csv = CsvLogger(
        path=out_dir / cfg.monitor.health_csv,
        fieldnames=[
            "ts_iso",
            "symbol",
            "agg_age_s",
            "kline_age_s",
            "agg_msgs",
            "kline_msgs",
            "bars_finalized",
            "open_positions",
            "mode",
            "testnet",
        ],
    )

    notifier = TelegramNotifier(
        bot_token=env_str("TELEGRAM_BOT_TOKEN", ""),
        chat_id=env_str("TELEGRAM_CHAT_ID", ""),
    )

    # Telegram throttling (for non-trade noise)
    last_tg_ms = 0

    async def send_tg_info(text: str) -> None:
        nonlocal last_tg_ms
        if not notifier.enabled:
            return
        now_ms = int(time.time() * 1000)
        if now_ms - last_tg_ms < cfg.monitor.telegram_min_gap_seconds * 1000:
            return
        await notifier.send(text)
        last_tg_ms = now_ms

    async def send_tg_trade(text: str) -> None:
        nonlocal last_tg_ms
        if not notifier.enabled:
            return
        await notifier.send(text)
        last_tg_ms = int(time.time() * 1000)

    def log_trade(symbol: str, pos) -> None:
        """Write a compact trade summary row."""
        if pos is None:
            return
        entry_iso = _ms_to_iso(pos.entry_time_ms) if getattr(pos, "entry_time_ms", None) else ""
        exit_iso = _ms_to_iso(pos.exit_time_ms) if getattr(pos, "exit_time_ms", None) else ""
        trades_csv.log(
            {
                "symbol": symbol,
                "direction": getattr(pos, "side", ""),
                "qty": getattr(pos, "qty", ""),
                "entry_time": entry_iso,
                "entry_price": getattr(pos, "entry_price", ""),
                "exit_time": exit_iso,
                "exit_price": getattr(pos, "exit_price", ""),
                "reason": getattr(pos, "exit_reason", ""),
                "fees_est": round(pos.estimated_fees(getattr(trader, "fee_rate", 0.0)), 8)
                if hasattr(pos, "estimated_fees")
                else "",
            }
        )

    # Per-symbol state
    bar_ms = cfg.engine.bar_seconds * 1000
    accum: Dict[str, TradeBarAccumulator] = {s: TradeBarAccumulator(bar_ms) for s in cfg.engine.symbols}
    zcalc: Dict[str, RollingRobustZ] = {s: RollingRobustZ(cfg.engine.roll_window_bars) for s in cfg.engine.symbols}
    donch: Dict[str, DonchianState] = {
        s: DonchianState.create(cfg.engine.donchian_window_minutes) for s in cfg.engine.symbols
    }
    last_signal_ms: Dict[str, int] = {s: 0 for s in cfg.engine.symbols}
    last_pos_sync_ms: Dict[str, int] = {s: 0 for s in cfg.engine.symbols}

    # Feed health stats
    last_agg_ms: Dict[str, int] = {s: 0 for s in cfg.engine.symbols}
    last_kline_ms: Dict[str, int] = {s: 0 for s in cfg.engine.symbols}
    agg_msgs: Dict[str, int] = {s: 0 for s in cfg.engine.symbols}
    kline_msgs: Dict[str, int] = {s: 0 for s in cfg.engine.symbols}
    bars_finalized: Dict[str, int] = {s: 0 for s in cfg.engine.symbols}

    start_ms = int(time.time() * 1000)
    warmup_until_ms = start_ms + cfg.execution.warmup_seconds * 1000

    async with BinanceFuturesRest(api_key=api_key, api_secret=api_secret, base_url=rest_base) as rest:
        if self_test:
            try:
                await run_startup_self_test(rest, ws_base, cfg.engine.symbols, timeout_s=self_test_timeout)
            except Exception as e:
                if self_test_strict:
                    log.error("Startup self-test failed: %s", e)
                    raise SystemExit(1)
                log.warning("Startup self-test failed (continuing because SELF_TEST_STRICT=0): %s", e)

        exinfo = await rest.exchange_info()
        filters = parse_symbol_filters(exinfo)
        trader = Trader(
            rest=rest,
            filters=filters,
            notional_usdt=cfg.execution.notional_usdt_per_trade,
            leverage=cfg.execution.leverage,
            stop_loss_pct=cfg.execution.stop_loss_pct,
            time_exit_seconds=cfg.execution.time_exit_seconds,
            trading_mode=trading_mode,
        )

        # Bootstrap Donchian window from REST klines
        for sym in cfg.engine.symbols:
            try:
                kl = await rest.klines_1m(sym, limit=max(200, cfg.engine.donchian_window_minutes + 5))
                now_ms = int(time.time() * 1000)
                # kline format: [openTime, open, high, low, close, volume, closeTime, ...]
                for row in kl:
                    high = float(row[2])
                    low = float(row[3])
                    close_time = int(row[6])
                    # only closed candles
                    if close_time <= now_ms - 1000:
                        donch[sym].push_candle(high, low, close_time)
                        last_kline_ms[sym] = max(last_kline_ms[sym], close_time)
                log.info("Bootstrapped %s donchian candles: %d/%d", sym, len(donch[sym].highs), donch[sym].window)
            except Exception as e:
                log.warning("Bootstrap klines failed for %s: %s", sym, e)

        # User data stream for fills/stops (live mode only)
        user_listen_key: Optional[str] = None
        user_ws: Optional[BinanceMarketStream] = None
        user_stream_ok = False

        async def handle_user_stream(msg: dict) -> None:
            nonlocal user_stream_ok
            data = msg.get("data", msg)
            etype = data.get("e")
            if etype == "listenKeyExpired":
                user_stream_ok = False
                await send_tg_info("User data listenKey expired; restart bot to refresh key.")
                return
            if etype != "ORDER_TRADE_UPDATE":
                return
            o = data.get("o", {})
            sym = o.get("s")
            if not sym or sym not in accum:
                return

            status = o.get("X")
            order_type = o.get("ot")
            event_time = int(data.get("E", int(time.time() * 1000)))
            order_id_raw = o.get("i")
            order_id = int(order_id_raw) if order_id_raw is not None else None

            price = None
            for key in ("ap", "L", "p", "sp"):
                try:
                    v = o.get(key)
                    if v is None:
                        continue
                    f = float(v)
                    if f > 0:
                        price = f
                        break
                except Exception:
                    continue

            # Entry fills
            if status == "FILLED" and order_type in ("MARKET", "LIMIT"):
                pos_filled = await trader.mark_entry_filled(sym, price or 0.0, event_time, order_id=order_id)
                if pos_filled:
                    orders_csv.log(
                        {
                            "ts_iso": _ms_to_iso(event_time),
                            "symbol": sym,
                            "action": "OPEN_FILLED",
                            "direction": pos_filled.side,
                            "qty": pos_filled.qty,
                            "price": pos_filled.entry_price,
                            "status": status,
                            "details": f"order_id={order_id}",
                        }
                    )
                    await send_tg_trade(
                        f"FILLED {sym} {pos_filled.side} qty={pos_filled.qty} avg~{pos_filled.entry_price:.4f}"
                    )
                return

            if status in ("REJECTED", "EXPIRED", "CANCELED") and order_type in ("MARKET", "LIMIT"):
                pos_rej = await trader.mark_entry_rejected(sym, reason=status)
                if pos_rej:
                    orders_csv.log(
                        {
                            "ts_iso": _ms_to_iso(event_time),
                            "symbol": sym,
                            "action": "OPEN_REJECTED",
                            "direction": pos_rej.side,
                            "qty": pos_rej.qty,
                            "price": price or pos_rej.entry_price,
                            "status": status,
                            "details": f"order_id={order_id}",
                        }
                    )
                    await send_tg_trade(f"REJECTED {sym} {pos_rej.side} qty={pos_rej.qty} status={status}")
                return

            # Stop-loss triggered
            if status == "FILLED" and order_type == "STOP_MARKET":
                pos_stop = await trader.mark_stop_filled(sym, price or 0.0, event_time)
                if pos_stop:
                    orders_csv.log(
                        {
                            "ts_iso": _ms_to_iso(event_time),
                            "symbol": sym,
                            "action": "STOP_FILLED",
                            "direction": pos_stop.side,
                            "qty": pos_stop.qty,
                            "price": price or pos_stop.stop_trigger_price,
                            "status": status,
                            "details": f"order_id={order_id}",
                        }
                    )
                    log_trade(sym, pos_stop)
                    await send_tg_trade(
                        f"STOP FILLED {sym} {pos_stop.side} qty={pos_stop.qty} px~{(price or pos_stop.stop_trigger_price):.4f}"
                    )

        async def start_user_stream() -> None:
            nonlocal user_listen_key, user_ws, user_stream_ok
            if trading_mode != "live":
                return
            try:
                user_listen_key = await rest.start_user_stream()
                user_stream_ok = True
                log.info("Obtained user listenKey.")
            except Exception as e:
                user_stream_ok = False
                log.warning("User stream start failed: %s", e)
                await send_tg_info("User data stream unavailable; fill/stop confirmations disabled.")
                return

            user_ws = BinanceMarketStream(
                ws_base=ws_base,
                streams=[user_listen_key],
                on_message=handle_user_stream,
                name="user",
                combined=False,
            )

        async def user_keepalive() -> None:
            while trading_mode == "live" and not stop_event.is_set():
                await asyncio.sleep(1200)
                if stop_event.is_set() or not user_listen_key:
                    continue
                try:
                    await rest.keepalive_user_stream(user_listen_key)
                except Exception as e:
                    log.warning("User stream keepalive failed: %s", e)
                    user_stream_ok = False
                    await send_tg_info("User data stream keepalive failed; fill confirmations may stop.")
        async def handle_aggtrade(msg: dict) -> None:
            data = msg.get("data", msg)
            if data.get("e") != "aggTrade":
                return
            sym = data.get("s")
            if sym not in accum:
                return

            try:
                trade_ms = int(data["T"])
                price = float(data["p"])
                is_buyer_maker = bool(data["m"])
            except Exception:
                return

            last_agg_ms[sym] = trade_ms
            agg_msgs[sym] += 1

            # Build 5s bars
            finalized = accum[sym].add_trade(trade_ms, price, is_buyer_maker)
            for bar in finalized:
                bars_finalized[sym] += 1

                # Update rolling robust Z with delta_tick
                zcalc[sym].push(bar.delta_tick)
                dz = zcalc[sym].z(bar.delta_tick) if zcalc[sym].ready() else 0.0

                # Compute donchian bands if ready
                d_high = d_low = float("nan")
                touch_low = touch_high = False
                if donch[sym].ready():
                    d_high, d_low = donch[sym].bands()
                    touch_low = (bar.low <= d_low) or (bar.close <= d_low)
                    touch_high = (bar.high >= d_high) or (bar.close >= d_high)

                # Strategy decision
                signal_s = ""
                reason = ""
                now_ms = int(time.time() * 1000)

                # Sync position state (if SL hit on-exchange, we may still think we're in a position)
                if trading_mode == "live" and trader.has_position(sym) and (now_ms - last_pos_sync_ms[sym] > 60000):
                    last_pos_sync_ms[sym] = now_ms
                    try:
                        pr = await rest.position_risk(sym)
                        if isinstance(pr, list) and pr:
                            amt = float(pr[0].get("positionAmt", "0"))
                        else:
                            amt = float(pr.get("positionAmt", "0"))
                        if abs(amt) < 1e-12:
                            await trader.drop_position(sym, reason="exchange_flat")
                            orders_csv.log(
                                {
                                    "ts_iso": _ms_to_iso(now_ms),
                                    "symbol": sym,
                                    "action": "SYNC_FLAT",
                                    "direction": "",
                                    "qty": "",
                                    "price": bar.close,
                                    "status": "live_flat",
                                    "details": "exchange positionAmt ~ 0",
                                }
                            )
                            await send_tg_trade(f"{sym} detected flat on-exchange (cleared local position state).")
                    except Exception as e:
                        log.warning("position sync failed for %s: %s", sym, e)

                # Time exit check (per symbol)
                if trader.has_position(sym):
                    close_res = await trader.close_if_due(sym, now_ms=now_ms, mark_price=bar.close)
                    if close_res is not None:
                        exit_px = close_res.get("mark_price", bar.close)
                        pos_closed = close_res.get("pos")
                        orders_csv.log(
                            {
                                "ts_iso": _ms_to_iso(now_ms),
                                "symbol": sym,
                                "action": "TIME_EXIT",
                                "direction": close_res.get("direction", ""),
                                "qty": close_res.get("qty", ""),
                                "price": exit_px,
                                "status": close_res.get("status", ""),
                                "details": str(close_res.get("order", ""))[:400],
                            }
                        )
                        if pos_closed:
                            log_trade(sym, pos_closed)
                        await send_tg_trade(
                            f"TIME EXIT {sym} {close_res.get('direction')} qty={close_res.get('qty')} price~{float(exit_px):.2f}"
                        )

                # Entry checks
                can_trade = now_ms >= warmup_until_ms
                if not can_trade:
                    reason = "warmup"
                elif not donch[sym].ready():
                    reason = "donch_not_ready"
                elif bar.trades < cfg.engine.min_trades_per_bar:
                    reason = "min_trades"
                elif abs(dz) > cfg.engine.z_cap_abs:
                    reason = "z_cap"
                elif (now_ms - last_signal_ms[sym]) < cfg.engine.cooldown_seconds * 1000:
                    reason = "cooldown"
                elif len(trader.positions) >= max_open_positions and not trader.has_position(sym):
                    reason = "max_positions"
                elif trader.has_position(sym):
                    reason = "in_position"
                else:
                    thr = cfg.engine.opposing_threshold_z
                    if touch_low and dz > thr:
                        signal_s = "LONG"
                        reason = "touch_low+opposing"
                    elif touch_high and dz < -thr:
                        signal_s = "SHORT"
                        reason = "touch_high+opposing"
                    else:
                        reason = "no_signal"

                # Log bar + decision
                events_csv.log(
                    {
                        "ts_iso": _ms_to_iso(bar.start_ms),
                        "symbol": sym,
                        "bar_start_ms": bar.start_ms,
                        "bar_open": bar.open,
                        "bar_high": bar.high,
                        "bar_low": bar.low,
                        "bar_close": bar.close,
                        "delta_tick": bar.delta_tick,
                        "delta_z": dz,
                        "trades": bar.trades,
                        "donchian_high": d_high,
                        "donchian_low": d_low,
                        "touch_low": int(touch_low),
                        "touch_high": int(touch_high),
                        "signal": signal_s,
                        "reason": reason,
                    }
                )

                # Execute
                if signal_s and can_trade:
                    last_signal_ms[sym] = now_ms
                    try:
                        mark = await rest.mark_price(sym)
                    except Exception:
                        mark = bar.close

                    res = await trader.open_position(sym, direction=signal_s, mark_price=mark)
                    pos_open = res.get("pos")
                    qty_open = res.get("qty", getattr(pos_open, "qty", ""))
                    status = res.get("status", "")

                    orders_csv.log(
                        {
                            "ts_iso": _ms_to_iso(now_ms),
                            "symbol": sym,
                            "action": "OPEN",
                            "direction": signal_s,
                            "qty": qty_open,
                            "price": mark,
                            "status": status,
                            "details": str({k: v for k, v in res.items() if k not in ("pos",)})[:400],
                        }
                    )

                    if status == "skip":
                        continue

                    if status == "error":
                        await send_tg_info(f"OPEN FAILED {sym} {signal_s} error={res.get('error')}")
                        continue

                    if trading_mode != "live":
                        await send_tg_trade(f"PAPER OPEN {sym} {signal_s} qty={qty_open} price~{mark:.2f}")
                    else:
                        stop_note = "stop_ok" if not res.get("stop_error") else "stop_fail"
                        await send_tg_trade(
                            f"OPEN_SENT {sym} {signal_s} qty={qty_open} z={dz:.2f} trades={bar.trades} price~{mark:.2f} {stop_note}"
                        )
                        if res.get("stop_error"):
                            await send_tg_info(f"Stop placement failed for {sym}: {res.get('stop_error')}")

        async def handle_kline(msg: dict) -> None:
            data = msg.get("data", msg)
            if data.get("e") != "kline":
                return
            sym = data.get("s")
            if sym not in donch:
                return
            k = data.get("k", {})
            # closed candle
            if not k.get("x", False):
                return
            try:
                high = float(k["h"])
                low = float(k["l"])
                close_time = int(k["T"])  # close time ms
            except Exception:
                return
            kline_msgs[sym] += 1
            last_kline_ms[sym] = close_time
            donch[sym].push_candle(high, low, close_time)

        async def on_ws_message(msg: dict) -> None:
            # combined stream wrapper: {"stream":"...","data":{...}}
            data = msg.get("data", msg)
            etype = data.get("e")
            if etype == "aggTrade":
                await handle_aggtrade(msg)
            elif etype == "kline":
                await handle_kline(msg)

        # Streams
        streams_agg = [f"{s.lower()}@aggTrade" for s in cfg.engine.symbols]
        streams_kline = [f"{s.lower()}@kline_1m" for s in cfg.engine.symbols]

        agg_ws = BinanceMarketStream(ws_base=ws_base, streams=streams_agg, on_message=on_ws_message, name="aggTrade")
        kl_ws = BinanceMarketStream(ws_base=ws_base, streams=streams_kline, on_message=on_ws_message, name="kline_1m")

        # graceful shutdown
        stop_event = asyncio.Event()

        def _stop(*_args: Any) -> None:
            log.info("Shutdown signal received.")
            agg_ws.stop()
            kl_ws.stop()
            if user_ws:
                user_ws.stop()
            stop_event.set()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _stop)
            except NotImplementedError:
                pass

        if trading_mode == "live":
            await start_user_stream()

        async def health_monitor() -> None:
            stale_state: Dict[str, bool] = {s: False for s in cfg.engine.symbols}
            stale_since: Dict[str, Optional[int]] = {s: None for s in cfg.engine.symbols}
            reconnect_sent: Dict[str, bool] = {s: False for s in cfg.engine.symbols}
            last_stale_send: Dict[str, int] = {s: 0 for s in cfg.engine.symbols}
            last_hb_send = 0

            # Startup banner (one-time)
            await send_tg_info(
                "\n".join(
                    [
                        "Sniper Bot ONLINE",
                        f"mode={trading_mode} testnet={is_testnet}",
                        f"symbols={','.join(cfg.engine.symbols)}",
                        f"donch=1m({cfg.engine.donchian_window_minutes}) bar={cfg.engine.bar_seconds}s",
                        f"thr={cfg.engine.opposing_threshold_z} z_cap={cfg.engine.z_cap_abs} min_trades={cfg.engine.min_trades_per_bar}",
                        f"notional={cfg.execution.notional_usdt_per_trade} lev={cfg.execution.leverage} SL={cfg.execution.stop_loss_pct*100:.3f}% exit={cfg.execution.time_exit_seconds}s",
                    ]
                )
            )

            while not stop_event.is_set():
                now_ms = int(time.time() * 1000)
                open_pos = len(trader.positions)

                for sym in cfg.engine.symbols:
                    agg_age_s = ""
                    kline_age_s = ""
                    if last_agg_ms[sym] > 0:
                        agg_age_s = round((now_ms - last_agg_ms[sym]) / 1000.0, 3)
                    if last_kline_ms[sym] > 0:
                        kline_age_s = round((now_ms - last_kline_ms[sym]) / 1000.0, 3)

                    health_csv.log(
                        {
                            "ts_iso": _ms_to_iso(now_ms),
                            "symbol": sym,
                            "agg_age_s": agg_age_s,
                            "kline_age_s": kline_age_s,
                            "agg_msgs": agg_msgs[sym],
                            "kline_msgs": kline_msgs[sym],
                            "bars_finalized": bars_finalized[sym],
                            "open_positions": open_pos,
                            "mode": trading_mode,
                            "testnet": int(is_testnet),
                        }
                    )

                    # Staleness alerts (start checking after 60s uptime)
                    if now_ms - start_ms < 60000:
                        continue

                    agg_stale = (last_agg_ms[sym] == 0 and (now_ms - start_ms) > cfg.monitor.stale_aggtrade_seconds * 1000) or (
                        last_agg_ms[sym] > 0 and (now_ms - last_agg_ms[sym]) > cfg.monitor.stale_aggtrade_seconds * 1000
                    )
                    kline_stale = (last_kline_ms[sym] == 0 and (now_ms - start_ms) > cfg.monitor.stale_kline_seconds * 1000) or (
                        last_kline_ms[sym] > 0 and (now_ms - last_kline_ms[sym]) > cfg.monitor.stale_kline_seconds * 1000
                    )

                    is_stale = bool(agg_stale or kline_stale)

                    if is_stale and not stale_state[sym]:
                        stale_state[sym] = True
                        stale_since[sym] = now_ms
                        reconnect_sent[sym] = False
                        last_stale_send[sym] = now_ms
                        await send_tg_trade(
                            f"FEED STALE {sym} agg_age={agg_age_s}s kline_age={kline_age_s}s (will keep trying)"
                        )
                    elif (not is_stale) and stale_state[sym]:
                        stale_state[sym] = False
                        stale_since[sym] = None
                        reconnect_sent[sym] = False
                        await send_tg_trade(f"FEED RECOVERED {sym} agg_age={agg_age_s}s kline_age={kline_age_s}s")
                    elif is_stale and (now_ms - last_stale_send[sym]) > cfg.monitor.stale_repeat_seconds * 1000:
                        last_stale_send[sym] = now_ms
                        await send_tg_info(
                            f"still stale {sym} agg_age={agg_age_s}s kline_age={kline_age_s}s (msgs agg={agg_msgs[sym]} kline={kline_msgs[sym]})"
                        )

                    if (
                        is_stale
                        and stale_since[sym]
                        and not reconnect_sent[sym]
                        and (now_ms - stale_since[sym]) > cfg.monitor.stale_force_reconnect_seconds * 1000
                    ):
                        reconnect_sent[sym] = True
                        if agg_stale:
                            agg_ws.request_reconnect()
                        if kline_stale:
                            kl_ws.request_reconnect()
                        await send_tg_info(
                            f"Watchdog reconnect {sym} agg_stale={agg_stale} kline_stale={kline_stale} agg_age={agg_age_s}s kline_age={kline_age_s}s"
                        )

                # Heartbeat (optional)
                if cfg.monitor.telegram_heartbeat and notifier.enabled:
                    if now_ms - last_hb_send > cfg.monitor.heartbeat_interval_seconds * 1000:
                        last_hb_send = now_ms
                        # Don't spam: send_tg_info will enforce min gap
                        total_bars = sum(bars_finalized.values())
                        await send_tg_info(
                            f"HEARTBEAT mode={trading_mode} testnet={is_testnet} open_pos={len(trader.positions)} total_bars={total_bars}"
                        )

                await asyncio.sleep(cfg.monitor.sample_interval_seconds)

        tasks = [
            asyncio.create_task(agg_ws.run_forever()),
            asyncio.create_task(kl_ws.run_forever()),
            asyncio.create_task(health_monitor()),
        ]
        if user_ws:
            tasks.append(asyncio.create_task(user_ws.run_forever()))
        if user_listen_key:
            tasks.append(asyncio.create_task(user_keepalive()))

        log.info(
            "Started sniper bot. mode=%s testnet=%s symbols=%s rest=%s ws=%s",
            trading_mode,
            is_testnet,
            cfg.engine.symbols,
            rest_base,
            ws_base,
        )
        if trading_mode != "live":
            log.warning("TRADING_MODE=%s (no orders will be sent)", trading_mode)

        await stop_event.wait()

        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        if user_listen_key:
            try:
                await rest.close_user_stream(user_listen_key)
            except Exception:
                pass

        log.info("Exited cleanly.")


def cli() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Path to YAML config")
    ap.add_argument("--self-test", action="store_true", help="Run startup connectivity self-test before trading")
    ap.add_argument("--self-test-timeout", type=int, default=120, help="Seconds to wait for WS self-test data (default 120)")
    args = ap.parse_args()
    asyncio.run(main_async(args.config, self_test=args.self_test, self_test_timeout=args.self_test_timeout))


if __name__ == "__main__":
    cli()
