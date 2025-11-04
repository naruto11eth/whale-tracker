# HyperLiquid Whale Tracker

Command–line tool for monitoring large trades (default ≥ $10 M notional) on the HyperLiquid perp markets. The script seeds itself with the most recent fills, subscribes to the public websocket for live updates, and prints a table summarizing each qualifying trade along with real‑time market context (mark, mid, impact prices, funding, and instantaneous PnL vs. mark).

## Requirements

- Python 3.9+
- [`hyperliquid-python-sdk`](https://github.com/hyperliquid-dex/hyperliquid-python-sdk)

Example setup:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install hyperliquid-python-sdk
```

## Usage

```bash
python3 whale_tracker.py [options]
```

Key options:

- `--coins BTC ETH …` Coins to monitor (default `BTC ETH`).
- `--threshold 15000000` USD notional threshold (default `10000000`).
- `--lookback-hours 12` Retention horizon for the cached trades.
- `--show-users` Include taker/maker addresses in the output table.
- `--storage-file trade_cache.json` On‑disk cache (default same name).
- `--sentiment COIN` Print long/short flow mix for a coin and exit.
- `--sentiment-lookback 6` Lookback (in hours) used for sentiment mode.
- `--no-startup-summary` Skip the initial snapshot of cached trades.

Example:

```bash
python3 whale_tracker.py --coins BTC ETH --threshold 20000000 --show-users
```

Run the tracker continuously (e.g. via tmux/systemd) to accumulate trades beyond the REST endpoint’s last-10 limit. The cache rolls automatically and persists to `trade_cache.json` between runs.

### Sentiment Mode

To inspect the recent directional flow for a single coin:

```bash
python3 whale_tracker.py --sentiment ETH --sentiment-lookback 12
```

The script reports trade counts and USD notionals by direction within the lookback window (using cached data plus the latest `recentTrades` batch if necessary).

## Backfilling Trades

The public `/info` API only exposes the most recent few trades per coin. For historical backfills, download HyperLiquid’s trade dumps from `s3://hl-mainnet-node-data/node_trades` (AWS CLI, requester pays) and merge them into `trade_cache.json` before launching the tracker.

## Notes

- Output uses UTC timestamps.
- Funding impact is a simple “current rate × notional” estimate; positive values represent a benefit to longs.
- The script subscribes to `activeAssetCtx` for mark/impact data; the HyperLiquid websocket must remain reachable for full functionality.
