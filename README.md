# HyperLiquid Whale Tracker

Command–line tool for monitoring large trades (default ≥ $10 M notional) on the HyperLiquid perp markets. The script seeds itself with the most recent fills, subscribes to the public websocket for live updates, and prints a table summarizing each qualifying trade along with real‑time market context (mark, mid, impact prices, funding, and instantaneous PnL vs. mark).

## Requirements

- Python 3.9+
- [`hyperliquid-python-sdk`](https://github.com/hyperliquid-dex/hyperliquid-python-sdk)
- [`requests`](https://pypi.org/project/requests/)

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

- `--coins BTC ETH …` List of coins to monitor. Pass `all` to auto-discover every listed perp.
- `--coins-file coins.txt` Optional text file (see `coins.txt`) with additional symbols, one per line or whitespace separated.
- `--env-file secrets.env` Load a custom `.env` file (defaults to `.env` when present).
- `--threshold 15000000` USD notional threshold (default `10000000`).
- `--lookback-hours 12` Retention horizon for the cached trades.
- `--hide-users` Skip printing taker/maker addresses (default is to include them).
- `--telegram-token / --telegram-chat-id` Send whale alerts to a Telegram bot/chat.
- `--pnl-period day|week|month` Annotate alerts with the taker’s portfolio PnL for that timeframe.
- `--storage-file custom_cache.json` Explicit cache file path. When omitted a timestamped `trade_cache_YYYYmmdd_HHMMSS.json` is created automatically each run.
- `--sentiment COIN` Print long/short flow mix for a coin and exit.
- `--sentiment-lookback 6` Lookback (in hours) used for sentiment mode.
- `--no-startup-summary` Skip the initial snapshot of cached trades.

Example:

```bash
python3 whale_tracker.py --coins BTC ETH --threshold 20000000 --show-users
```

Run the tracker continuously (e.g. via tmux/systemd) to accumulate trades beyond the REST endpoint’s last-10 limit. Each run writes to its own cache file unless you pass `--storage-file` to reuse a prior dataset.

> Auto alerts now start **off**; send `/turn_on_auto_watcher` after the bot joins a chat if you want prints to resume immediately.

### Common CLI Commands

- **Quick local watch (BTC/ETH, $200k+)**
  ```bash
  python3 whale_tracker.py --coins BTC ETH --threshold 200000 \
      --cache-dir cache --pnl-period day
  ```

- **Monitor every coin with full feed + weekly PnL column**
  ```bash
  python3 whale_tracker.py --coins all --threshold 0 \
      --cache-dir cache --pnl-period week
  ```

- **Telegram deployment using a coins file**
  ```bash
  python3 whale_tracker.py --coins-file coins.txt --threshold 150000 \
      --telegram-token "$TELEGRAM_BOT_TOKEN" \
      --telegram-chat-id "$TELEGRAM_CHAT_ID" \
      --cache-dir cache --pnl-period all
  ```

- **Sentiment-only mode**
  ```bash
  python3 whale_tracker.py --sentiment SOL --sentiment-lookback 6
  ```

- **Custom cache reuse**
  ```bash
  python3 whale_tracker.py --coins BTC ETH \
      --storage-file cache/btc_eth_cache.json --threshold 500000
  ```



### Monitoring All Coins

You can launch the tracker for every listed perp by discovering the universe at runtime:

```bash
python3 whale_tracker.py --coins $(python3 - <<'PY'
from hyperliquid.info import Info
meta = Info(skip_ws=True).meta()
coins = [asset["name"] for asset in meta["universe"] if not asset.get("isDelisted")]
print(" ".join(coins))
PY
) --threshold 0
```

This will subscribe to every coin’s trade and asset-context feed, so expect a high data rate.

### Coins File

Use the provided `coins.txt` (or create your own) to store a reusable list of symbols:

```bash
python3 whale_tracker.py --coins-file coins.txt --threshold 15000000
```

### Telegram Alerts

Provide your bot token and chat ID to receive push notifications for every whale trade:

```bash
python3 whale_tracker.py --coins BTC ETH \
    --threshold 15000000 \
    --telegram-token YOUR_BOT_TOKEN \
    --telegram-chat-id YOUR_CHAT_ID
```

Create a bot via [@BotFather](https://core.telegram.org/bots#6-botfather). DM the bot once, then call `https://api.telegram.org/bot<token>/getUpdates` to discover your chat ID.

You can also keep credentials in a `.env` file (automatically loaded) so you don’t pass them on the CLI:

```env
TELEGRAM_BOT_TOKEN=123456:ABCDEF
TELEGRAM_CHAT_ID=987654321
TELEGRAM_THREAD_ID=42  # optional, for forum topics
```

Use `--env-file custom.env` to load an alternative secrets file.

If you want alerts in a specific forum topic inside a supergroup, capture the topic’s `message_thread_id` from `getUpdates` and pass `--telegram-thread-id 42` (or set `TELEGRAM_THREAD_ID` in `.env`).

### Telegram Commands

Once the bot is in a chat you can control it without restarting the tracker:

- `/turn_on_auto_watcher` / `/turn_off_auto_watcher` – toggle real-time alert blasting while continuing to record trades.
- `/last_10_trades`, `/last_20_trades`, `/last_50_trades` – dump the most recent cached whale alerts.
- `/coin_watcher BTC` – subscribe to a new coin on the fly.
- `/market_sentiment ETH` – show the cached sentiment summary for any coin.
- `/pnl_of_address 0xabc… day|week|month|all` – pull the portfolio PnL summary for any account over the requested window.
- `/pnl_period day|week|month|all|off` – change the global PnL overlay without restarting the bot (use `off` to hide it).
- `/positions 0xabc… [limit]` – show current positions plus recent fills (limit defaults to 5).

When auto alerts are off the tracker still records trades (up to 500) so you can pull them on demand with the `/last_*` commands.

All commands respect the optional `TELEGRAM_THREAD_ID`; send them inside the same topic/channel where the bot posts alerts.

### Sentiment Mode

To inspect the recent directional flow for a single coin:

```bash
python3 whale_tracker.py --sentiment ETH --sentiment-lookback 12
```

The script reports trade counts and USD notionals by direction within the lookback window (using cached data plus the latest `recentTrades` batch if necessary).

## Backfilling Trades

The public `/info` API only exposes the most recent few trades per coin. For historical backfills, download HyperLiquid’s trade dumps from `s3://hl-mainnet-node-data/node_trades` (AWS CLI, requester pays) and merge them into your cache file before launching the tracker.

## Notes

- Output uses UTC timestamps.
- Funding impact is a simple “current rate × notional” estimate; positive values represent a benefit to longs.
- The script subscribes to `activeAssetCtx` for mark/impact data; the HyperLiquid websocket must remain reachable for full functionality.
