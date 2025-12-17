# Sniper Bot Live (Donchian Touch + Opposing Flow)

Minimal live-ready implementation of the Experiment D "Sniper" rule:
- Context: Donchian High/Low from closed 1m klines
- Trigger: 5s robust Z on tick delta showing opposing aggression
- Filters: min trades per 5s bar, Z-cap kill switch, per-symbol cooldown
- Execution: market entry, hard stop (algo close-all), time-exit after 5 minutes

## Quick start (paper mode)
```bash
cd repo_final
cp .env.example .env           # leave keys blank for paper
python -m venv .venv
source .venv/bin/activate      # on Windows: .venv\Scripts\activate
pip install -r requirements.txt

python -m sniper_bot.main --config configs/sniper.yaml --self-test
```
- TRADING_MODE defaults to `paper`; no keys required.
- If websockets are blocked in your environment, self-test will warn and continue (set `SELF_TEST_STRICT=1` to fail-fast).

## Configs
- `configs/sniper.yaml` – safe defaults for paper mode.
- `configs/sniper.full.yaml` – every key spelled out with comments.
All config keys are validated; monitor section is optional (defaults applied when omitted).

## Logs
- `logs/events.csv` – finalized 5s bars, Donchian bands, touch flags, signal + reason
- `logs/orders.csv` – opens, exits, sync events, API payload snippets
- `logs/trades.csv` – compact trade summary (entry/exit times, prices, qty, reason, estimated fees)
- `logs/health.csv` – per-symbol stream health (aggTrade/kline ages, counters, open positions)

## Telegram alerts (optional)
- Startup banner, trade lifecycle (`OPEN_SENT`/`FILLED`/`REJECTED`/`TIME EXIT`/`STOP FILLED`)
- Feed health: `FEED STALE` / `FEED RECOVERED` with slow repeats while stale
- Optional heartbeat (throttled by `monitor.telegram_min_gap_seconds`)
Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` in `.env` to enable.

## Self-test
`python -m sniper_bot.main --config configs/sniper.yaml --self-test --self-test-timeout 120`
- REST: `exchangeInfo` + `markPrice` for the first symbol
- WS: waits for at least one `aggTrade` and one closed `kline_1m`
- Fails fast when `SELF_TEST_STRICT=1`; otherwise logs a warning and continues if the network blocks websockets.

## Deployment (systemd example)
- Copy repo to `/opt/sniper_bot_live`
- Create venv, install deps, place `.env` in `/opt/sniper_bot_live/.env`
- Copy `scripts/systemd/sniper-bot.service` to `/etc/systemd/system/sniper-bot.service`
```bash
sudo systemctl daemon-reload
sudo systemctl enable sniper-bot
sudo systemctl start sniper-bot
sudo journalctl -u sniper-bot -f
```

## Endpoint overrides
Override when your network requires custom domains:
- `BINANCE_REST_BASE=...`
- `BINANCE_WS_BASE=...`
