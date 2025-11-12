#!/usr/bin/env python3
"""Entry point and orchestration for the whale tracker."""
from __future__ import annotations

import argparse
import os
import signal
import sys
import threading
import time
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from hyperliquid.info import Info

from .cache import Trade, TradeCache, format_notional, load_cache, save_cache
from .constants import (
    DEFAULT_CACHE_DIR,
    DEFAULT_LOOKBACK_HOURS,
    DEFAULT_SAVE_INTERVAL_SECONDS,
    DEFAULT_STORAGE_PATH,
    DEFAULT_THRESHOLD_USD,
    VALID_PNL_PERIODS,
)
from .hyperliquid_client import (
    AssetContext,
    compute_funding_effect,
    compute_pnl_vs_mark,
    fetch_recent,
)
from .pnl import fetch_portfolio_pnl, query_portfolio_pnl
from .telegram_bot import (
    TelegramCommandHandler,
    TelegramNotifier,
    TrackerState,
    compute_sentiment_summary,
)
from .utils import load_env_file, to_decimal

# Ensure .env is loaded before argparse consumes defaults.
load_env_file(".env")


def read_coins_from_file(path: str) -> List[str]:
    file_path = Path(path)
    if not file_path.exists():
        raise SystemExit(f"Coin list file not found: {path}")
    tokens: List[str] = []
    with open(file_path, "r", encoding="utf-8") as fh:
        for line in fh:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            tokens.extend(stripped.split())
    return tokens


def discover_all_coins() -> List[str]:
    info = Info(skip_ws=True)
    meta = info.meta()
    coins: List[str] = []
    for asset in meta.get("universe", []):
        name = asset.get("name")
        if not name or asset.get("isDelisted"):
            continue
        if name not in coins:
            coins.append(name)
    return coins


def resolve_coins(args: argparse.Namespace) -> List[str]:
    resolved: List[str] = []
    seen: Set[str] = set()

    def push(items: Iterable[str]) -> None:
        for coin in items:
            upper = coin.upper()
            if upper in seen:
                continue
            seen.add(upper)
            resolved.append(upper)

    cli_coins = args.coins or []
    if len(cli_coins) == 1 and cli_coins[0].lower() == "all":
        push(discover_all_coins())
    else:
        push(cli_coins)

    if args.coins_file:
        push(read_coins_from_file(args.coins_file))

    if not resolved:
        raise SystemExit("No coins resolved. Provide --coins, --coins all, or --coins-file.")

    return resolved


def print_table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> None:
    col_widths = [len(h) for h in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            col_widths[idx] = max(col_widths[idx], len(cell))

    def fmt_row(row: Sequence[str]) -> str:
        return " | ".join(cell.ljust(col_widths[idx]) for idx, cell in enumerate(row))

    divider = "-+-".join("-" * w for w in col_widths)
    print(fmt_row(headers))
    print(divider)
    for row in rows:
        print(fmt_row(row))
    print()


def build_alert_text(
    trade: Trade,
    mark_px: Optional[Decimal],
    mid_px: Optional[Decimal],
    pnl_vs_mark: Optional[Decimal],
    funding_rate: Optional[Decimal],
    funding_effect: Optional[Decimal],
    *,
    include_users: bool,
    period_pnl: Optional[Decimal],
    pnl_period: Optional[str],
) -> str:
    direction = trade.direction.upper()
    parts = [
        f"<b>{trade.coin}</b> {direction}",
        "",
        f"Size: {trade.size.normalize()} @ {trade.price.normalize()} ({format_notional(trade.notional_usd)})",
    ]
    if mark_px is not None:
        parts.append(f"Mark: {mark_px.normalize()}")
    if mid_px is not None:
        parts.append(f"Mid: {mid_px.normalize()}")
    if pnl_vs_mark is not None:
        parts.append(f"PnL vs mark: {format_notional(pnl_vs_mark)}")
    if funding_rate is not None:
        rate_pct = funding_rate * Decimal(100)
        parts.append(f"Funding: {rate_pct:.4f}%")
    if funding_effect is not None:
        parts.append(f"Funding impact: {format_notional(funding_effect)}")
    if include_users:
        parts.append(f"Taker: {trade.users[0]}\nMaker: {trade.users[1]}")
    if period_pnl is not None and pnl_period:
        parts.append(f"{pnl_period.capitalize()} PnL: {format_notional(period_pnl)}")
    parts.append("")
    parts.append(f"Trade: https://app.hyperliquid.xyz/trade/{trade.coin}")
    return "\n".join(parts)


def resolve_storage_path(user_path: Optional[str], cache_dir: str) -> str:
    if user_path:
        return user_path
    os.makedirs(cache_dir, exist_ok=True)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    return os.path.join(cache_dir, f"trade_cache_{timestamp}.json")


def run_tracker(args: argparse.Namespace) -> None:
    lookback_seconds = int(args.lookback_hours * 3600)
    threshold = Decimal(str(args.threshold))
    coins = resolve_coins(args)
    print(f"Monitoring {len(coins)} coin(s)")
    cache = load_cache(args.storage_file, lookback_seconds)

    tracker_state = TrackerState()
    tracker_state.set_pnl_period(args.pnl_period)

    notifier: Optional[TelegramNotifier] = None
    if args.telegram_token and args.telegram_chat_id:
        notifier = TelegramNotifier(
            args.telegram_token,
            args.telegram_chat_id,
            args.telegram_thread_id,
        )
    elif args.telegram_token or args.telegram_chat_id:
        print(
            "Both --telegram-token and --telegram-chat-id are required for Telegram alerts.",
            file=sys.stderr,
        )

    stop_event = threading.Event()
    trade_queue: "Queue[Trade]" = Queue()
    reported: Dict[str, Set[str]] = defaultdict(set)

    def handle_signal(signum, _frame):
        print(f"Received signal {signum}, shutting down...")
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    info = Info()
    ctx_lock = threading.Lock()
    asset_ctx: Dict[str, AssetContext] = {}

    def set_ctx(coin: str, ctx_payload: Dict) -> None:
        ctx = AssetContext(
            coin=coin,
            mark_px=to_decimal(ctx_payload.get("markPx")),
            mid_px=to_decimal(ctx_payload.get("midPx")),
            impact_bid=None
            if ctx_payload.get("impactPxs") is None
            else to_decimal(ctx_payload["impactPxs"][0]),
            impact_ask=None
            if ctx_payload.get("impactPxs") is None
            else to_decimal(ctx_payload["impactPxs"][1]),
            funding_rate=to_decimal(ctx_payload.get("funding")),
        )
        with ctx_lock:
            asset_ctx[coin.upper()] = ctx

    try:
        meta, ctxs = info.meta_and_asset_ctxs()
        for asset_info, ctx_payload in zip(meta.get("universe", []), ctxs):
            coin = asset_info.get("name")
            if coin and ctx_payload:
                set_ctx(coin, ctx_payload)
    except Exception as exc:
        print(f"Warning: failed to prime asset contexts: {exc}", file=sys.stderr)

    def on_ws_trades(msg):
        for item in msg["data"]:
            trade_queue.put(Trade.from_raw(item["coin"], item))

    def on_ws_asset_ctx(msg):
        data = msg.get("data", {})
        coin = data.get("coin")
        ctx_payload = data.get("ctx")
        if coin and ctx_payload:
            set_ctx(coin, ctx_payload)

    subscribed_coins: Set[str] = set()
    subscription_lock = threading.Lock()

    def subscribe_coin(coin: str) -> Tuple[bool, Optional[str]]:
        normalized = coin.upper()
        with subscription_lock:
            if normalized in subscribed_coins:
                return False, f"Already watching {normalized}."
            try:
                for trade in fetch_recent(info, normalized):
                    cache.add(trade)
                info.subscribe({"type": "trades", "coin": normalized}, on_ws_trades)
                info.subscribe({"type": "activeAssetCtx", "coin": normalized}, on_ws_asset_ctx)
                subscribed_coins.add(normalized)
                return True, f"Started watching {normalized}."
            except Exception as exc:
                return False, f"Failed to watch {normalized}: {exc}"

    for coin in coins:
        _, message = subscribe_coin(coin)
        if message:
            print(message)

    if args.print_startup_summary:
        now_ms = int(time.time() * 1000)
        big_trades = sorted(
            [t for t in cache.all_recent(now_ms) if t.notional_usd >= threshold],
            key=lambda t: t.time_ms,
        )
        if big_trades:
            print("Existing large trades within lookback window:")
            headers = ["Time (UTC)", "Coin", "Dir", "Size", "Price", "Notional"]
            rows = [
                [
                    time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(t.time_ms / 1000)),
                    t.coin,
                    t.direction.upper(),
                    str(t.size.normalize()),
                    str(t.price.normalize()),
                    format_notional(t.notional_usd),
                ]
                for t in big_trades
            ]
            print_table(headers, rows)
        else:
            print("No large trades cached within lookback window.")

    def sentiment_callback(symbol: str, cache_ref: TradeCache = cache) -> str:
        return compute_sentiment_summary(symbol, cache_ref, args.sentiment_lookback)

    if notifier and args.telegram_token and args.telegram_chat_id:
        cmd_handler = TelegramCommandHandler(
            args.telegram_token,
            args.telegram_chat_id,
            args.telegram_thread_id,
            tracker_state,
            notifier,
            subscribe_coin,
            sentiment_callback,
            lambda address, period: query_portfolio_pnl(address, period),
            cache,
            args.sentiment_lookback,
            stop_event,
        )
        cmd_handler.start()

    pnl_cache: Dict[Tuple[str, str], Tuple[Optional[Decimal], float]] = {}
    pnl_cache_ttl = 300
    last_save = time.time()

    try:
        while not stop_event.is_set():
            try:
                trade = trade_queue.get(timeout=0.5)
            except Empty:
                if time.time() - last_save >= args.save_interval:
                    save_cache(args.storage_file, cache)
                    last_save = time.time()
                continue

            trade_queue.task_done()
            if not cache.add(trade):
                continue

            key = f"{trade.tid}:{trade.time_ms}"
            if trade.notional_usd >= threshold:
                cutoff = cache.cutoff(None)
                if trade.time_ms >= cutoff and key not in reported[trade.coin]:
                    reported[trade.coin].add(key)
                    with ctx_lock:
                        ctx = asset_ctx.get(trade.coin.upper())
                    mark_px = ctx.mark_px if ctx else None
                    mid_px = ctx.mid_px if ctx else None
                    pnl = compute_pnl_vs_mark(trade, mark_px)
                    funding_effect = compute_funding_effect(trade, ctx.funding_rate if ctx else None)
                    period_pnl_value: Optional[Decimal] = None
                    period_setting = tracker_state.get_pnl_period()
                    if period_setting:
                        cache_key = (trade.users[0], period_setting)
                        cached = pnl_cache.get(cache_key)
                        now = time.time()
                        if cached and now - cached[1] < pnl_cache_ttl:
                            period_pnl_value = cached[0]
                        else:
                            data, error = fetch_portfolio_pnl(trade.users[0], period_setting)
                            if data:
                                period_pnl_value = data["pnl"]
                                pnl_cache[cache_key] = (period_pnl_value, now)
                            else:
                                pnl_cache[cache_key] = (None, now)
                    headers = [
                        "Time (UTC)",
                        "Coin",
                        "Dir",
                        "Size",
                        "Trade Px",
                        "Notional",
                        "Mark Px",
                        "Mid Px",
                        "Impact Bid",
                        "Impact Ask",
                        "PnL vs Mark",
                        "Funding Rate",
                        "Funding Impact",
                    ]
                    row = [
                        time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(trade.time_ms / 1000)),
                        trade.coin,
                        trade.direction.upper(),
                        str(trade.size.normalize()),
                        str(trade.price.normalize()),
                        format_notional(trade.notional_usd),
                        str(mark_px.normalize()) if mark_px is not None else "N/A",
                        str(mid_px.normalize()) if mid_px is not None else "N/A",
                        str(ctx.impact_bid.normalize()) if ctx and ctx.impact_bid is not None else "N/A",
                        str(ctx.impact_ask.normalize()) if ctx and ctx.impact_ask is not None else "N/A",
                        format_notional(pnl) if pnl is not None else "N/A",
                        str(ctx.funding_rate) if ctx and ctx.funding_rate is not None else "N/A",
                        format_notional(funding_effect) if funding_effect is not None else "N/A",
                    ]
                    if not args.hide_users:
                        headers += ["Taker", "Maker"]
                        row += [trade.users[0], trade.users[1]]
                    if period_setting:
                        headers += [f"{period_setting.capitalize()} PnL"]
                        row.append(format_notional(period_pnl_value) if period_pnl_value is not None else "N/A")
                    print_table(headers, [row])

                    alert_text = build_alert_text(
                        trade,
                        mark_px,
                        mid_px,
                        pnl,
                        ctx.funding_rate if ctx else None,
                        funding_effect,
                        include_users=not args.hide_users,
                        period_pnl=period_pnl_value,
                        pnl_period=period_setting,
                    )
                    tracker_state.record_trade(alert_text)

                    if notifier and tracker_state.is_auto_watch():
                        notifier.send(alert_text)

        while not trade_queue.empty():
            try:
                trade_queue.get_nowait()
                trade_queue.task_done()
            except Empty:
                break
    finally:
        stop_event.set()
        save_cache(args.storage_file, cache)
        try:
            info.disconnect_websocket()
        except RuntimeError:
            pass


def run_sentiment(args: argparse.Namespace) -> None:
    if not args.sentiment:
        print("Please provide --sentiment COIN")
        return
    cache = load_cache(args.storage_file, int(args.sentiment_lookback * 3600))
    summary = compute_sentiment_summary(args.sentiment, cache, args.sentiment_lookback)
    print(summary)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="HyperLiquid whale tracker")
    parser.add_argument(
        "--coins",
        nargs="*",
        default=["BTC", "ETH"],
        help="List of coins to monitor (default: %(default)s)",
    )
    parser.add_argument(
        "--coins-file",
        help="Optional text file with additional coin symbols (one per line or space-separated).",
    )
    parser.add_argument(
        "--env-file",
        help="Optional .env file to load (defaults to .env if present).",
    )
    parser.add_argument(
        "--threshold",
        type=Decimal,
        default=DEFAULT_THRESHOLD_USD,
        help="Notional USD threshold for highlighting trades (default: %(default)s)",
    )
    parser.add_argument(
        "--lookback-hours",
        type=float,
        default=DEFAULT_LOOKBACK_HOURS,
        help="Hours to retain trades in memory and cache (default: %(default)s)",
    )
    parser.add_argument(
        "--storage-file",
        default=DEFAULT_STORAGE_PATH,
        help="Optional cache file path (default: auto-generated trade_cache_TIMESTAMP.json)",
    )
    parser.add_argument(
        "--cache-dir",
        default=DEFAULT_CACHE_DIR,
        help="Directory where auto-generated cache files should be written (default: %(default)s)",
    )
    parser.add_argument(
        "--save-interval",
        type=int,
        default=DEFAULT_SAVE_INTERVAL_SECONDS,
        help="Seconds between cache flushes to disk (default: %(default)s)",
    )
    parser.add_argument(
        "--hide-users",
        action="store_true",
        help="Hide taker/maker addresses in alerts.",
    )
    parser.add_argument(
        "--pnl-period",
        choices=sorted(VALID_PNL_PERIODS),
        help="When set, include the taker's PnL for the given period in alerts.",
    )
    parser.add_argument(
        "--telegram-token",
        default=os.getenv("TELEGRAM_BOT_TOKEN"),
        help="Telegram bot token for alerts (defaults to TELEGRAM_BOT_TOKEN env).",
    )
    parser.add_argument(
        "--telegram-chat-id",
        default=os.getenv("TELEGRAM_CHAT_ID"),
        help="Telegram chat ID for alerts (defaults to TELEGRAM_CHAT_ID env).",
    )
    parser.add_argument(
        "--telegram-thread-id",
        default=os.getenv("TELEGRAM_THREAD_ID"),
        help="Optional Telegram topic/thread ID for forum groups (defaults to TELEGRAM_THREAD_ID env).",
    )
    parser.add_argument(
        "--sentiment",
        help="Print sentiment for the specified coin and exit.",
    )
    parser.add_argument(
        "--sentiment-lookback",
        type=float,
        default=DEFAULT_LOOKBACK_HOURS,
        help="Lookback window in hours for sentiment mode (default: %(default)s).",
    )
    parser.add_argument(
        "--no-startup-summary",
        action="store_false",
        dest="print_startup_summary",
        help="Skip listing cached large trades on startup.",
    )
    parser.set_defaults(print_startup_summary=True)
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    if args.env_file:
        load_env_file(args.env_file)
        if not args.telegram_token:
            args.telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        if not args.telegram_chat_id:
            args.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not args.telegram_thread_id:
            args.telegram_thread_id = os.getenv("TELEGRAM_THREAD_ID")

    thread_id_value: Optional[int] = None
    if args.telegram_thread_id:
        try:
            thread_id_value = int(str(args.telegram_thread_id).strip())
        except ValueError as exc:
            raise SystemExit(
                f"Invalid telegram thread id '{args.telegram_thread_id}': {exc}"
            )
    args.telegram_thread_id = thread_id_value

    args.storage_file = resolve_storage_path(args.storage_file, args.cache_dir)
    print(f"Using cache file: {args.storage_file}")

    if args.sentiment:
        run_sentiment(args)
    else:
        run_tracker(args)


if __name__ == "__main__":  # pragma: no cover
    main()
