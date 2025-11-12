"""Helpers for fetching and formatting user positions."""
from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal
from typing import List, Optional

from hyperliquid.info import Info

from .cache import format_notional


@dataclass
class Position:
    coin: str
    size: Decimal
    direction: str
    entry_px: Optional[Decimal]
    liquidation_px: Optional[Decimal]
    leverage: Optional[str]
    unrealized_pnl: Optional[Decimal]


@dataclass
class Fill:
    coin: str
    side: str
    size: Decimal
    price: Decimal
    timestamp_ms: int
    closed_pnl: Optional[Decimal]

    @property
    def direction(self) -> str:
        return "long" if self.side.upper() == "B" else "short"


def _decimal_or_none(value: Optional[str]) -> Optional[Decimal]:
    if value in (None, "", "null"):
        return None
    try:
        return Decimal(value)
    except Exception:
        return None


def _fmt_decimal(value: Optional[Decimal]) -> str:
    if value is None:
        return "-"
    try:
        return str(value.normalize())
    except Exception:
        return str(value)


def fetch_open_positions(address: str) -> List[Position]:
    info = Info(skip_ws=True)
    state = info.user_state(address)
    positions: List[Position] = []
    for entry in state.get("assetPositions", []):
        pos = entry.get("position", {})
        coin = pos.get("coin")
        if not coin:
            continue
        size_raw = pos.get("szi")
        try:
            size = Decimal(size_raw)
        except Exception:
            continue
        if size == 0:
            continue
        direction = "long" if size > 0 else "short"
        entry_px = _decimal_or_none(pos.get("entryPx"))
        liq_px = _decimal_or_none(pos.get("liquidationPx"))
        leverage_info = pos.get("leverage") or {}
        lev_type = leverage_info.get("type")
        lev_value = leverage_info.get("value")
        leverage = None
        if lev_type and lev_value is not None:
            leverage = f"{lev_type}:{lev_value}"
        unrealized = _decimal_or_none(pos.get("unrealizedPnl"))
        positions.append(
            Position(
                coin=coin,
                size=abs(size),
                direction=direction,
                entry_px=entry_px,
                liquidation_px=liq_px,
                leverage=leverage,
                unrealized_pnl=unrealized,
            )
        )
    positions.sort(key=lambda p: p.size, reverse=True)
    return positions

