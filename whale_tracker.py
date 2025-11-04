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
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from queue import Empty, Queue
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from hyperliquid.info import Info


DEFAULT_THRESHOLD_USD = Decimal("10000000")  # $10M
DEFAULT_LOOKBACK_HOURS = 12
DEFAULT_SAVE_INTERVAL_SECONDS = 60
DEFAULT_STORAGE_PATH = "trade_cache.json"


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
