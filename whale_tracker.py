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
