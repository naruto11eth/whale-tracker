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


