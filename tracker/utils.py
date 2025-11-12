"""Utility helpers for whale tracker."""
from __future__ import annotations

import os
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Optional


def load_env_file(path: str) -> None:
    file_path = Path(path)
    if not file_path.exists():
        return
    try:
        with open(file_path, "r", encoding="utf-8") as fh:
            for raw_line in fh:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                if not key:
                    continue
                value = value.strip().strip('"').strip("'")
                os.environ[key] = value
    except OSError as exc:  # pragma: no cover - filesystem guard
        print(f"Warning: could not read env file {path}: {exc}", file=sys.stderr)


def to_decimal(value: Optional[str]) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        return Decimal(value)
    except (InvalidOperation, TypeError):
        return None
