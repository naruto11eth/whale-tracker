"""PnL helpers."""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence

from hyperliquid.info import Info

from .constants import PERIOD_ALIAS_LOOKUP, VALID_PNL_PERIODS, canonical_period_key


def _extract_portfolio_entries(payload: Any) -> List[Any]:
    if isinstance(payload, dict):
        entries = payload.get("portfolio")
        if entries is None:
            entries = payload.get("result")
        if entries is None:
            entries = payload.get("data")
        if entries is None:
            entries = list(payload.items())
    else:
        entries = payload
    if isinstance(entries, dict):
        return list(entries.items())
    if isinstance(entries, list):
        return entries
    return []


