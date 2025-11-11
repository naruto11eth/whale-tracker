#!/usr/bin/env python3
"""
HyperLiquid whale tracker.

Continuously watches trades for selected coins and highlights fills whose USD
notional crosses a configurable threshold (default: 10M) within a configurable
lookback window (default: 12h).

Requirements:
    pip install hyperliquid-python-sdk
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from queue import Empty, Queue
from typing import Deque, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import requests

from hyperliquid.info import Info


DEFAULT_THRESHOLD_USD = Decimal("10000000")  # $10M
DEFAULT_LOOKBACK_HOURS = 12
DEFAULT_SAVE_INTERVAL_SECONDS = 60
DEFAULT_STORAGE_PATH: Optional[str] = None


def load_env_file(path: str) -> None:
    file_path = Path(path)
    if not file_path.exists():
        return
    try:
        with open(file_path, "r", encoding="utf-8") as fh:
            for raw_line in fh:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                if not key:
                    continue
                value = value.strip().strip('"').strip("'")
                os.environ[key] = value
    except OSError as exc:  # pragma: no cover - filesystem guard
        print(f"Warning: could not read env file {path}: {exc}", file=sys.stderr)


# Load default .env early so argparse defaults can read the values.
load_env_file(".env")


def to_decimal(value: Optional[str]) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        return Decimal(value)
    except (InvalidOperation, TypeError):
        return None


@dataclass(frozen=True)
class Trade:
    coin: str
    tid: str
    time_ms: int
    price: Decimal
    size: Decimal
    side: str
    hash: str
    users: Tuple[str, str]

    @property
    def notional_usd(self) -> Decimal:
        return self.price * self.size

    @property
    def direction(self) -> str:
        # HyperLiquid denotes side "B" as buy (taker lifting ask), "A" as sell.
        return "long" if self.side.upper() == "B" else "short"

    @staticmethod
    def from_raw(coin: str, raw: Dict) -> "Trade":
        return Trade(
            coin=coin,
            tid=str(raw["tid"]),
            time_ms=int(raw["time"]),
            price=Decimal(raw["px"]),
            size=Decimal(raw["sz"]),
            side=str(raw["side"]),
            hash=str(raw.get("hash", "")),
            users=tuple(raw.get("users", ("", ""))),  # maker, taker addresses
        )

    def to_json(self) -> Dict:
        return {
            "coin": self.coin,
            "tid": self.tid,
            "time": self.time_ms,
            "px": str(self.price),
            "sz": str(self.size),
            "side": self.side,
            "hash": self.hash,
            "users": list(self.users),
        }


class TradeCache:
    """Stores trades per coin within a moving time window."""

    def __init__(self, lookback_seconds: int):
        self._lookback = lookback_seconds
        self._lock = threading.Lock()
        self._store: Dict[str, Dict[str, Trade]] = defaultdict(dict)

    def add(self, trade: Trade) -> bool:
        """Add trade; returns True if the trade was new."""
        with self._lock:
            trades = self._store[trade.coin]
            existing = trades.get(trade.tid)
            if existing and existing.time_ms >= trade.time_ms:
                return False
            trades[trade.tid] = trade
            self._prune_locked(trade.coin)
            return True

    def recent(self, coin: str, now_ms: Optional[int] = None) -> List[Trade]:
        with self._lock:
            trades = list(self._store.get(coin, {}).values())
        cutoff = self._cutoff(now_ms)
        return [t for t in trades if t.time_ms >= cutoff]

    def all_recent(self, now_ms: Optional[int] = None) -> List[Trade]:
        cutoff = self._cutoff(now_ms)
        with self._lock:
            all_trades = []
            for trades in self._store.values():
                all_trades.extend(trades.values())
        return [t for t in all_trades if t.time_ms >= cutoff]

    def cutoff(self, now_ms: Optional[int] = None) -> int:
        return self._cutoff(now_ms)

    def _cutoff(self, now_ms: Optional[int]) -> int:
        if now_ms is None:
            now_ms = int(time.time() * 1000)
        return now_ms - self._lookback * 1000

    def _prune_locked(self, coin: str, now_ms: Optional[int] = None) -> None:
        cutoff = self._cutoff(now_ms)
        trades = self._store.get(coin)
        if not trades:
            return
        stale = [tid for tid, trade in trades.items() if trade.time_ms < cutoff]
        for tid in stale:
            trades.pop(tid, None)

    def prune_all(self, now_ms: Optional[int] = None) -> None:
        with self._lock:
            for coin in list(self._store.keys()):
                self._prune_locked(coin, now_ms)
                if not self._store[coin]:
                    del self._store[coin]

    def to_json(self) -> Dict:
        with self._lock:
            return {
                "trades": {
                    coin: [trade.to_json() for trade in trades.values()]
                    for coin, trades in self._store.items()
                }
            }

    def load_json(self, payload: Dict) -> None:
        trades = payload.get("trades", {})
        with self._lock:
            for coin, items in trades.items():
                for item in items:
                    try:
                        trade = Trade.from_raw(coin, item)
                    except Exception:
                        continue
                    self._store[coin][trade.tid] = trade


def load_cache(path: str, lookback_seconds: int) -> TradeCache:
    cache = TradeCache(lookback_seconds)
    if not path or not os.path.exists(path):
        return cache
    try:
        with open(path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
        cache.load_json(payload)
        cache.prune_all()
        print(f"Loaded {len(cache.all_recent())} cached trades from {path}")
    except Exception as exc:  # pragma: no cover - defensive
        print(f"Warning: could not load cache {path}: {exc}", file=sys.stderr)
    return cache


def save_cache(path: str, cache: TradeCache) -> None:
    if not path:
        return
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as fh:
        json.dump(cache.to_json(), fh)
    os.replace(tmp_path, path)


def format_notional(value: Decimal) -> str:
    return f"${value:,.2f}"


def summarize_trade(trade: Trade) -> str:
    ts = time.strftime(
        "%Y-%m-%d %H:%M:%S",
        time.gmtime(trade.time_ms / 1000),
    )
    return (
        f"[{ts} UTC] {trade.coin}: {trade.direction.upper()} "
        f"{trade.size.normalize()} @ {trade.price.normalize()} "
        f"({format_notional(trade.notional_usd)}) "
        f"taker={trade.users[0]} maker={trade.users[1]}"
    )


def fetch_recent(info: Info, coin: str) -> List[Trade]:
    try:
        raw = info.post("/info", {"type": "recentTrades", "coin": coin})
        return [Trade.from_raw(coin, item) for item in raw]
    except Exception as exc:  # pragma: no cover - network failure fallback
        print(f"Failed to pull recent trades for {coin}: {exc}", file=sys.stderr)
        return []


@dataclass
class AssetContext:
    coin: str
    mark_px: Optional[Decimal]
    mid_px: Optional[Decimal]
    impact_bid: Optional[Decimal]
    impact_ask: Optional[Decimal]
    funding_rate: Optional[Decimal]


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


def compute_pnl_vs_mark(trade: Trade, mark_px: Optional[Decimal]) -> Optional[Decimal]:
    if mark_px is None:
        return None
    if trade.direction == "long":
        return (mark_px - trade.price) * trade.size
    return (trade.price - mark_px) * trade.size


def compute_funding_effect(
    trade: Trade, funding_rate: Optional[Decimal]
) -> Optional[Decimal]:
    if funding_rate is None:
        return None
    effect = trade.notional_usd * funding_rate
    if trade.direction == "short":
        effect = -effect
    return effect


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str, thread_id: Optional[int] = None) -> None:
        self.token = token
        self.chat_id = chat_id
        self.thread_id = thread_id
        self.endpoint = f"https://api.telegram.org/bot{token}/sendMessage"

    def send(self, text: str) -> None:
        try:
            payload = {
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": "HTML",
            }
            if self.thread_id is not None:
                payload["message_thread_id"] = self.thread_id
            resp = requests.post(
                self.endpoint,
                json=payload,
                timeout=10,
            )
            if not resp.ok:
                print(
                    f"Telegram notify failed: {resp.status_code} {resp.text}",
                    file=sys.stderr,
                )
        except requests.RequestException as exc:  # pragma: no cover - network
            print(f"Telegram notify error: {exc}", file=sys.stderr)


def build_alert_text(
    trade: Trade,
    mark_px: Optional[Decimal],
    mid_px: Optional[Decimal],
    pnl_vs_mark: Optional[Decimal],
    funding_rate: Optional[Decimal],
    funding_effect: Optional[Decimal],
    include_users: bool = False,
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
        parts.append(f"Taker: {trade.users[0]}  |  Maker: {trade.users[1]}")
    parts.append("")
    parts.append(f"Trade: https://app.hyperliquid.xyz/trade/{trade.coin}")
    return "\n".join(parts)


class TrackerState:
    def __init__(self, history_limit: int = 500) -> None:
        self._history: Deque[str] = deque(maxlen=history_limit)
        self._auto_watch = True
        self._lock = threading.Lock()

    def set_auto_watch(self, enabled: bool) -> None:
        with self._lock:
            self._auto_watch = enabled

    def is_auto_watch(self) -> bool:
        with self._lock:
            return self._auto_watch

    def record_trade(self, message: str) -> None:
        with self._lock:
            self._history.appendleft(message)

    def recent_trades(self, count: int) -> List[str]:
        with self._lock:
            return list(self._history)[:count]


class TelegramCommandHandler:
    def __init__(
        self,
        token: str,
        chat_id: str,
        thread_id: Optional[int],
        state: TrackerState,
        notifier: TelegramNotifier,
        subscribe_fn,
        sentiment_fn,
        cache: TradeCache,
        lookback_hours: float,
        stop_event: threading.Event,
    ) -> None:
        self.token = token
        self.chat_id = str(chat_id)
        self.thread_id = thread_id
        self.state = state
        self.notifier = notifier
        self.subscribe_fn = subscribe_fn
        self.sentiment_fn = sentiment_fn
        self.cache = cache
        self.lookback_hours = lookback_hours
        self.stop_event = stop_event
        self.endpoint = f"https://api.telegram.org/bot{token}/getUpdates"
        self._thread: Optional[threading.Thread] = None
        self._offset: Optional[int] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()

    def _poll_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                params: Dict[str, object] = {"timeout": 30}
                if self._offset is not None:
                    params["offset"] = self._offset
                resp = requests.get(self.endpoint, params=params, timeout=35)
                if not resp.ok:
                    time.sleep(5)
                    continue
                data = resp.json()
                results = data.get("result", [])
                for item in results:
                    self._offset = item.get("update_id", 0) + 1
                    message = item.get("message") or item.get("edited_message")
                    if not message:
                        continue
                    chat = message.get("chat", {})
                    if str(chat.get("id")) != self.chat_id:
                        continue
                    thread_id = message.get("message_thread_id")
                    if self.thread_id is not None and thread_id != self.thread_id:
                        continue
                    text = message.get("text")
                    if not text or not text.startswith("/"):
                        continue
                    self._handle_command(text.strip())
            except requests.RequestException:
                time.sleep(5)
            except Exception as exc:  # pragma: no cover - defensive
                print(f"Telegram command handler error: {exc}", file=sys.stderr)
                time.sleep(5)

    def _handle_command(self, text: str) -> None:
        parts = text.split()
        if not parts:
            return
        command = parts[0].split("@")[0].lower()
        args = parts[1:]

        if command in {"/turn_on_auto_watcher", "/turnonauto", "/auto_on"}:
            self.state.set_auto_watch(True)
            self.notifier.send("Auto watcher enabled.")
        elif command in {"/turn_off_auto_watcher", "/turnoffauto", "/auto_off"}:
            self.state.set_auto_watch(False)
            self.notifier.send("Auto watcher disabled. Trades will still be recorded.")
        elif command in {"/last_10_trades", "/last10"}:
            self._send_recent_trades(10)
        elif command in {"/last_20_trades", "/last20"}:
            self._send_recent_trades(20)
        elif command in {"/last_50_trades", "/last50"}:
            self._send_recent_trades(50)
        elif command == "/market_sentiment":
            if not args:
                self.notifier.send("Usage: /market_sentiment COIN")
                return
            coin = args[0].upper()
            summary = self.sentiment_fn(coin, self.cache)
            self.notifier.send(summary)
        elif command == "/coin_watcher":
            if not args:
                self.notifier.send("Usage: /coin_watcher COIN")
                return
            coin = args[0].upper()
            added, message = self.subscribe_fn(coin)
            self.notifier.send(message if message else f"Watching {coin}.")
        else:
            if command.startswith("/last_") and command.endswith("_trades"):
                try:
                    count = int(command.split("_")[1])
                    self._send_recent_trades(count)
                    return
                except ValueError:
                    pass
            self.notifier.send("Unknown command.")

    def _send_recent_trades(self, count: int) -> None:
        entries = self.state.recent_trades(count)
        if not entries:
            self.notifier.send("No trades recorded yet.")
            return
        chunks = []
        for msg in entries:
            chunks.append(msg)
        self.notifier.send("\n\n".join(chunks))


def compute_sentiment_summary(coin: str, cache: TradeCache, lookback_hours: float) -> str:
    normalized = coin.upper()
    trades = cache.recent(normalized)
    if len(trades) < 20:
        try:
            info = Info(skip_ws=True)
            for trade in fetch_recent(info, normalized):
                cache.add(trade)
            trades = cache.recent(normalized)
        except Exception as exc:  # pragma: no cover - network fallback
            print(f"Sentiment fetch failed for {normalized}: {exc}", file=sys.stderr)

    if not trades:
        return f"No trades available for {normalized} in the current lookback window."

    total = len(trades)
    longs = [t for t in trades if t.direction == "long"]
    shorts = [t for t in trades if t.direction == "short"]

    long_notional = sum((t.notional_usd for t in longs), Decimal("0"))
    short_notional = sum((t.notional_usd for t in shorts), Decimal("0"))

    long_pct = (len(longs) / total * 100) if total else 0
    short_pct = (len(shorts) / total * 100) if total else 0

    lines = [
        f"Sentiment for {normalized}",
        f"Window: last {lookback_hours:g}h of cached data",
        f"Total trades: {total}",
        f"Longs: {len(longs)} ({long_pct:.2f}%) | Volume: {format_notional(long_notional)}",
        f"Shorts: {len(shorts)} ({short_pct:.2f}%) | Volume: {format_notional(short_notional)}",
    ]
    return "\n".join(lines)


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
        if not name:
            continue
        if asset.get("isDelisted"):
            continue
        if name not in coins:
            coins.append(name)
    return coins


def resolve_coins(args: argparse.Namespace) -> List[str]:
    resolved: List[str] = []
    seen: Set[str] = set()

    def push(items: Iterable[str]) -> None:
        for coin in items:
            normalized = coin.strip()
            if not normalized:
                continue
            key = normalized.upper()
            if key in seen:
                continue
            seen.add(key)
            resolved.append(normalized)

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


def resolve_storage_path(user_path: Optional[str]) -> str:
    if user_path:
        return user_path
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    return f"trade_cache_{timestamp}.json"


def run_tracker(args: argparse.Namespace) -> None:
    lookback_seconds = int(args.lookback_hours * 3600)
    threshold = Decimal(str(args.threshold))
    coins = resolve_coins(args)
    print(f"Monitoring {len(coins)} coin(s)")
    cache = load_cache(args.storage_file, lookback_seconds)

    tracker_state = TrackerState()

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
            except Exception as exc:  # pragma: no cover - defensive
                return False, f"Failed to watch {normalized}: {exc}"

    for coin in coins:
        added, message = subscribe_coin(coin)
        if message:
            print(message)

    cache.prune_all()
    now_ms = int(time.time() * 1000)
    for trade in cache.all_recent(now_ms):
        if trade.notional_usd >= threshold:
            reported[trade.coin].add(f"{trade.tid}:{trade.time_ms}")

    if args.print_startup_summary:
        big_trades = sorted(
            [t for t in cache.all_recent(now_ms) if t.notional_usd >= threshold],
            key=lambda t: t.time_ms,
        )
        if big_trades:
            print("Existing large trades within lookback window:")
            headers = [
                "Time (UTC)",
                "Coin",
                "Dir",
                "Size",
                "Price",
                "Notional",
            ]
            rows = [
                [
                    time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(trade.time_ms / 1000)),
                    trade.coin,
                    trade.direction.upper(),
                    str(trade.size.normalize()),
                    str(trade.price.normalize()),
                    format_notional(trade.notional_usd),
                ]
                for trade in big_trades
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
            cache,
            args.sentiment_lookback,
            stop_event,
        )
        cmd_handler.start()

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
                    funding_effect = compute_funding_effect(
                        trade, ctx.funding_rate if ctx else None
                    )
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
                        time.strftime(
                            "%Y-%m-%d %H:%M:%S", time.gmtime(trade.time_ms / 1000)
                        ),
                        trade.coin,
                        trade.direction.upper(),
                        str(trade.size.normalize()),
                        str(trade.price.normalize()),
                        format_notional(trade.notional_usd),
                        str(mark_px.normalize()) if mark_px is not None else "N/A",
                        str(mid_px.normalize()) if mid_px is not None else "N/A",
                        str(ctx.impact_bid.normalize())
                        if ctx and ctx.impact_bid is not None
                        else "N/A",
                        str(ctx.impact_ask.normalize())
                        if ctx and ctx.impact_ask is not None
                        else "N/A",
                        format_notional(pnl) if pnl is not None else "N/A",
                        str(ctx.funding_rate)
                        if ctx and ctx.funding_rate is not None
                        else "N/A",
                        format_notional(funding_effect)
                        if funding_effect is not None
                        else "N/A",
                    ]
                    if args.show_users:
                        headers += ["Taker", "Maker"]
                        row += [trade.users[0], trade.users[1]]
                    print_table(headers, [row])

                    alert_text = build_alert_text(
                        trade,
                        mark_px,
                        mid_px,
                        pnl,
                        ctx.funding_rate if ctx else None,
                        funding_effect,
                        include_users=args.show_users,
                    )
                    tracker_state.record_trade(alert_text)

                    if notifier and tracker_state.is_auto_watch():
                        notifier.send(alert_text)

        # ensure queue is drained before exit
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


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Monitor HyperLiquid trades above a notional threshold."
    )
    parser.add_argument(
        "--coins",
        nargs="+",
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
        "--save-interval",
        type=int,
        default=DEFAULT_SAVE_INTERVAL_SECONDS,
        help="Seconds between cache flushes to disk (default: %(default)s)",
    )
    parser.add_argument(
        "--show-users",
        action="store_true",
        help="Display taker/maker addresses in alerts.",
    )
    parser.add_argument(
        "--telegram-token",
        default=os.getenv("TELEGRAM_BOT_TOKEN"),
        help="Telegram bot token for alerts (defaults to TELEGRAM_BOT_TOKEN env).",
    )
    parser.add_argument(
        "--telegram-chat-id",
        default=os.getenv("TELEGRAM_CHAT_ID"),
        help="Telegram chat ID (user or group) for alerts (defaults to TELEGRAM_CHAT_ID env).",
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
    return parser.parse_args(argv)


if __name__ == "__main__":
    cli_args = parse_args()
    if cli_args.env_file:
        load_env_file(cli_args.env_file)
        if not cli_args.telegram_token:
            cli_args.telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        if not cli_args.telegram_chat_id:
            cli_args.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not cli_args.telegram_thread_id:
            cli_args.telegram_thread_id = os.getenv("TELEGRAM_THREAD_ID")

    thread_id_value: Optional[int] = None
    if cli_args.telegram_thread_id:
        try:
            thread_id_value = int(str(cli_args.telegram_thread_id).strip())
        except ValueError as exc:  # pragma: no cover - CLI guard
            raise SystemExit(
                f"Invalid telegram thread id '{cli_args.telegram_thread_id}': {exc}"
            )
    cli_args.telegram_thread_id = thread_id_value

    cli_args.storage_file = resolve_storage_path(cli_args.storage_file)
    print(f"Using cache file: {cli_args.storage_file}")
    try:
        if cli_args.sentiment:
            run_sentiment(cli_args)
        else:
            run_tracker(cli_args)
    except KeyboardInterrupt:
        pass
