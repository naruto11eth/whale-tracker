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



