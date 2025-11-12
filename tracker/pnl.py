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


def fetch_portfolio_pnl(address: str, period: str):
    period = period.lower()
    if period not in VALID_PNL_PERIODS:
        allowed = ", ".join(sorted(VALID_PNL_PERIODS))
        return None, f"Period must be one of: {allowed}"

    try:
        info = Info(skip_ws=True)
        portfolio = info.portfolio(address)
    except Exception as exc:  # pragma: no cover
        return None, f"Failed to fetch portfolio for {address}: {exc}"

    entries = _extract_portfolio_entries(portfolio)
    segments: Dict[str, Any] = {}
    for entry in entries:
        if isinstance(entry, (list, tuple)) and len(entry) >= 2:
            key = canonical_period_key(str(entry[0]))
            if key:
                segments[key] = entry[1]
        elif isinstance(entry, dict):
            key = entry.get("period") or entry.get("name")
            if key:
                canonical = canonical_period_key(str(key))
                if canonical:
                    segments[canonical] = entry

    canonical_period = canonical_period_key(period)
    search_keys = {canonical_period}
    search_keys.update(PERIOD_ALIAS_LOOKUP.get(canonical_period, set()))
    data = None
    for candidate in search_keys:
        data = segments.get(candidate)
        if data:
            break

    if not data:
        return None, f"No portfolio data for period '{period}'."

    pnl_history = data.get("pnlHistory")
    if not pnl_history:
        return None, f"PnL history unavailable for period '{period}'."

    try:
        start = Decimal(pnl_history[0][1])
        end = Decimal(pnl_history[-1][1])
    except Exception:
        return None, "Unexpected PnL format returned by API."

    pnl = end - start
    return {"start": start, "end": end, "pnl": pnl, "period": period}, None


def query_portfolio_pnl(address: str, period: str) -> str:
    data, error = fetch_portfolio_pnl(address, period)
    if error:
        return error
    return "\n".join(
        [
            f"PnL for {address}",
            f"Period: {data['period']}",
            f"Start: ${data['start']:,.2f}",
            f"End: ${data['end']:,.2f}",
            f"Net PnL: ${data['pnl']:,.2f}",
        ]
    )
