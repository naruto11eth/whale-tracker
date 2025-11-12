"""Helpers for fetching and formatting user positions."""
from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal
from typing import List, Optional

from hyperliquid.info import Info


@dataclass
class Position:
    coin: str
    size: Decimal
    direction: str
    entry_px: Optional[Decimal]
    liquidation_px: Optional[Decimal]
    leverage: Optional[str]


@dataclass
class Fill:
    coin: str
    side: str
    size: Decimal
    price: Decimal
    timestamp_ms: int

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
        positions.append(
            Position(
                coin=coin,
                size=abs(size),
                direction=direction,
                entry_px=entry_px,
                liquidation_px=liq_px,
                leverage=leverage,
            )
        )
    positions.sort(key=lambda p: p.size, reverse=True)
    return positions


def fetch_recent_fills(address: str, limit: int = 5) -> List[Fill]:
    info = Info(skip_ws=True)
    fills_raw = info.user_fills(address)
    fills: List[Fill] = []
    for item in fills_raw:
        try:
            fills.append(
                Fill(
                    coin=item.get("coin", ""),
                    side=item.get("side", ""),
                    size=Decimal(item.get("sz", "0")),
                    price=Decimal(item.get("px", "0")),
                    timestamp_ms=int(item.get("time", 0)),
                )
            )
        except Exception:
            continue
    fills.sort(key=lambda f: f.timestamp_ms, reverse=True)
    return fills[:limit]


def format_positions_report(address: str, history_limit: int = 5) -> str:
    try:
        open_positions = fetch_open_positions(address)
    except Exception as exc:
        return f"Failed to fetch positions for {address}: {exc}"

    try:
        history = fetch_recent_fills(address, history_limit)
    except Exception:
        history = []

    lines: List[str] = [f"Positions for {address}"]
    if open_positions:
        lines.append("\n<b>Open Positions</b>")
        lines.append("Coin | Dir | Size | Entry Px | Liq Px | Leverage")
        for pos in open_positions:
            lines.append(
                f"{pos.coin} | {pos.direction.upper()} | {pos.size.normalize()} | "
                f"{_fmt_decimal(pos.entry_px)} | {_fmt_decimal(pos.liquidation_px)} | {pos.leverage or '-'}"
            )
    else:
        lines.append("\nNo open perp positions.")

    if history:
        lines.append("\n<b>Recent Fills</b>")
        for fill in history:
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(fill.timestamp_ms / 1000))
            lines.append(
                f"[{ts} UTC] {fill.coin} {fill.direction.upper()} "
                f"{fill.size.normalize()} @ {fill.price.normalize()}"
            )
    else:
        lines.append("\nNo recent fills available.")

    return "\n".join(lines)
