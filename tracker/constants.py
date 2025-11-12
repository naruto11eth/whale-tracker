"""Shared constants for the whale tracker."""
from __future__ import annotations

from decimal import Decimal
from typing import Dict, Set

DEFAULT_THRESHOLD_USD = Decimal("10000000")  # $10M
DEFAULT_LOOKBACK_HOURS = 12
DEFAULT_SAVE_INTERVAL_SECONDS = 60
DEFAULT_STORAGE_PATH = None
DEFAULT_CACHE_DIR = "cache"

VALID_PNL_PERIODS = {"day", "week", "month", "all"}

_PERIOD_ALIAS_GROUPS = {
    "day": {"day", "1day", "1d", "daily"},
    "week": {"week", "1week", "1w", "weekly"},
    "month": {"month", "1month", "1m", "monthly"},
    "all": {"all", "alltime", "all_time", "lifetime", "overall", "total"},
}

FETCH_SKIP_FAILURE_THRESHOLD = 3
FETCH_SKIP_DURATION_SECONDS = 300


def canonical_period_key(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalnum())


PERIOD_ALIAS_LOOKUP: Dict[str, Set[str]] = {}
for key, aliases in _PERIOD_ALIAS_GROUPS.items():
    canonical_key = canonical_period_key(key)
    PERIOD_ALIAS_LOOKUP[canonical_key] = {canonical_period_key(alias) for alias in aliases}
    PERIOD_ALIAS_LOOKUP[canonical_key].add(canonical_key)
