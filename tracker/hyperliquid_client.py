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


def fetch_recent(
    info: Info,
    coin: str,
    *,
    max_retries: int = 5,
    backoff_seconds: float = 0.5,
) -> List[Trade]:
    remaining = _remaining_skip_seconds(coin)
    if remaining > 0:
        print(
            f"Skipping {coin.upper()} fetch; cooling down for {int(remaining)}s due to repeated API errors.",
            file=sys.stderr,
        )
        return []

    delay = backoff_seconds
    for attempt in range(1, max_retries + 1):
        try:
            raw = info.post("/info", {"type": "recentTrades", "coin": coin})
            trades = [Trade.from_raw(coin, item) for item in raw]
            _record_fetch_success(coin)
            return trades
        except ClientError as exc:
            if exc.status_code == 429 and attempt < max_retries:
                print(
                    f"Rate limited while fetching {coin} trades (attempt {attempt}/{max_retries}). "
                    f"Sleeping {delay:.2f}s.",
                    file=sys.stderr,
                )
                time.sleep(delay)
                delay = min(delay * 2, 10)
                continue
            print(
                f"Failed to pull recent trades for {coin}: HTTP {exc.status_code} {exc.error_message}",
                file=sys.stderr,
            )
            break
        except ServerError as exc:
            if exc.status_code == 500:
                _record_fetch_failure(coin, f"HTTP {exc.status_code}")
                if attempt < max_retries:
                    print(
                        f"Server error while fetching {coin} trades (attempt {attempt}/{max_retries}). "
                        f"Sleeping {delay:.2f}s.",
                        file=sys.stderr,
                    )
                    time.sleep(delay)
                    delay = min(delay * 2, 10)
                    continue
            print(
                f"Failed to pull recent trades for {coin}: HTTP {exc.status_code} {exc.message}",
                file=sys.stderr,
            )
            break
        except Exception as exc:  # pragma: no cover
            if attempt < max_retries:
                print(
                    f"Fetch attempt {attempt}/{max_retries} for {coin} failed ({exc}); retrying in {delay:.2f}s.",
                    file=sys.stderr,
                )
                time.sleep(delay)
                delay = min(delay * 2, 10)
                continue
            print(f"Failed to pull recent trades for {coin}: {exc}", file=sys.stderr)
            break
    return []


@dataclass
class AssetContext:
    coin: str
    mark_px: Optional[Decimal]
    mid_px: Optional[Decimal]
    impact_bid: Optional[Decimal]
    impact_ask: Optional[Decimal]
    funding_rate: Optional[Decimal]


def compute_pnl_vs_mark(trade: Trade, mark_px: Optional[Decimal]) -> Optional[Decimal]:
    if mark_px is None:
        return None
    if trade.direction == "long":
        return (mark_px - trade.price) * trade.size
    return (trade.price - mark_px) * trade.size


def compute_funding_effect(trade: Trade, funding_rate: Optional[Decimal]) -> Optional[Decimal]:
    if funding_rate is None:
        return None
    effect = trade.notional_usd * funding_rate
    if trade.direction == "short":
        effect = -effect
    return effect
