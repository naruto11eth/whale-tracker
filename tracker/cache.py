"""Trade cache primitives."""
from __future__ import annotations

import json
import os
import time
from collections import defaultdict
import threading
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional



@dataclass(frozen=True)
class Trade:
    coin: str
    tid: str
    time_ms: int
    price: Decimal
    size: Decimal
    side: str
    hash: str
    users: tuple[str, str]

    @property
    def notional_usd(self) -> Decimal:
        return self.price * self.size

    @property
    def direction(self) -> str:
        return "long" if self.side.upper() == "B" else "short"

    @staticmethod
    def from_raw(coin: str, raw: Dict[str, Any]) -> "Trade":
        return Trade(
            coin=coin,
            tid=str(raw["tid"]),
            time_ms=int(raw["time"]),
            price=Decimal(raw["px"]),
            size=Decimal(raw["sz"]),
            side=str(raw["side"]),
            hash=str(raw.get("hash", "")),
            users=tuple(raw.get("users", ("", ""))),
        )

    def to_json(self) -> Dict[str, Any]:
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

    def _cutoff(self, now_ms: Optional[int]) -> int:
        if now_ms is None:
            now_ms = int(time.time() * 1000)
        return now_ms - self._lookback * 1000

    def add(self, trade: Trade) -> bool:
        with self._lock:
            trades = self._store[trade.coin]
            existing = trades.get(trade.tid)
            if existing and existing.time_ms >= trade.time_ms:
                return False
            trades[trade.tid] = trade
            self._prune_locked(trade.coin)
            return True

    def _prune_locked(self, coin: str, now_ms: Optional[int] = None) -> None:
        cutoff = self._cutoff(now_ms)
        trades = self._store.get(coin)
        if not trades:
            return
        stale = [tid for tid, trade in trades.items() if trade.time_ms < cutoff]
        for tid in stale:
            trades.pop(tid, None)

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

    def prune_all(self, now_ms: Optional[int] = None) -> None:
        with self._lock:
            for coin in list(self._store.keys()):
                self._prune_locked(coin, now_ms)
                if not self._store[coin]:
                    del self._store[coin]

    def cutoff(self, now_ms: Optional[int] = None) -> int:
        return self._cutoff(now_ms)

    def to_json(self) -> Dict[str, Any]:
        return {
            "trades": {
                coin: [trade.to_json() for trade in trades.values()]
                for coin, trades in self._store.items()
            }
        }

    def load_json(self, payload: Dict[str, Any]) -> None:
        trades = payload.get("trades", {})
        for coin, items in trades.items():
            for item in items:
                try:
                    trade = Trade.from_raw(coin, item)
                except Exception:
                    continue
                self._store[coin][trade.tid] = trade


def load_cache(path: Optional[str], lookback_seconds: int) -> TradeCache:
    cache = TradeCache(lookback_seconds)
    if not path or not os.path.exists(path):
        return cache
    try:
        with open(path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
        cache.load_json(payload)
        cache.prune_all()
        print(f"Loaded {len(cache.all_recent())} cached trades from {path}")
    except Exception as exc:  # pragma: no cover
        print(f"Warning: could not load cache {path}: {exc}")
    return cache


def save_cache(path: Optional[str], cache: TradeCache) -> None:
    if not path:
        return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as fh:
        json.dump(cache.to_json(), fh)
    os.replace(tmp_path, path)


def format_notional(value: Decimal) -> str:
    return f"${value:,.2f}"


def summarize_trade(trade: Trade) -> str:
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(trade.time_ms / 1000))
    return (
        f"[{ts} UTC] {trade.coin}: {trade.direction.upper()} "
        f"{trade.size.normalize()} @ {trade.price.normalize()} "
        f"({format_notional(trade.notional_usd)}) "
        f"taker={trade.users[0]} maker={trade.users[1]}"
    )
