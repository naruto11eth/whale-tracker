"""HyperLiquid helpers (rest + websocket context)."""
from __future__ import annotations

import sys
import threading
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, List, Optional

from hyperliquid.info import Info
from hyperliquid.utils.error import ClientError, ServerError

from .cache import Trade
from .constants import (
    FETCH_SKIP_DURATION_SECONDS,
    FETCH_SKIP_FAILURE_THRESHOLD,
)

_fetch_failure_lock = threading.Lock()
_fetch_failure_counts: Dict[str, int] = {}
_fetch_skip_until: Dict[str, float] = {}


def _remaining_skip_seconds(coin: str) -> float:
    normalized = coin.upper()
    with _fetch_failure_lock:
        until = _fetch_skip_until.get(normalized)
        if until is None:
            return 0.0
        now = time.time()
        if now >= until:
            _fetch_skip_until.pop(normalized, None)
            return 0.0
        return until - now


def _record_fetch_failure(coin: str, reason: str) -> None:
    normalized = coin.upper()
    with _fetch_failure_lock:
        failures = _fetch_failure_counts.get(normalized, 0) + 1
        _fetch_failure_counts[normalized] = failures
        if failures >= FETCH_SKIP_FAILURE_THRESHOLD:
            _fetch_skip_until[normalized] = time.time() + FETCH_SKIP_DURATION_SECONDS
            _fetch_failure_counts[normalized] = 0
            print(
                f"Suspending {normalized} fetches for {FETCH_SKIP_DURATION_SECONDS}s "
                f"after repeated API errors ({reason}).",
                file=sys.stderr,
            )


def _record_fetch_success(coin: str) -> None:
    normalized = coin.upper()
    with _fetch_failure_lock:
        _fetch_failure_counts.pop(normalized, None)
        _fetch_skip_until.pop(normalized, None)


