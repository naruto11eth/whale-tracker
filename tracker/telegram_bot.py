"""Telegram integration for whale tracker."""
from __future__ import annotations

import threading
import time
from collections import deque
from decimal import Decimal
from typing import Any, Callable, Deque, Dict, List, Optional, Sequence

import requests
from hyperliquid.info import Info

from .cache import Trade, TradeCache, format_notional
from .constants import VALID_PNL_PERIODS
from .hyperliquid_client import fetch_recent


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str, thread_id: Optional[int] = None) -> None:
        self.token = token
        self.chat_id = chat_id
        self.thread_id = thread_id
        self.endpoint = f"https://api.telegram.org/bot{token}/sendMessage"

    def send(self, text: str) -> None:
        try:
            payload = {
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": "HTML",
            }
            if self.thread_id is not None:
                payload["message_thread_id"] = self.thread_id
            resp = requests.post(self.endpoint, json=payload, timeout=15)
            resp.raise_for_status()
        except Exception as exc:  # pragma: no cover
            print(f"Failed to send Telegram message: {exc}")


class TrackerState:
    def __init__(self, history_limit: int = 500) -> None:
        self._history: Deque[str] = deque(maxlen=history_limit)
        self._auto_watch = True
        self._pnl_period: Optional[str] = None
        self._lock = threading.Lock()

    def set_auto_watch(self, enabled: bool) -> None:
        with self._lock:
            self._auto_watch = enabled

    def is_auto_watch(self) -> bool:
        with self._lock:
            return self._auto_watch

    def record_trade(self, message: str) -> None:
        with self._lock:
            self._history.appendleft(message)

    def recent_trades(self, count: int) -> List[str]:
        with self._lock:
            return list(self._history)[:count]

    def set_pnl_period(self, period: Optional[str]) -> None:
        normalized = period.lower() if period else None
        if normalized is not None and normalized not in VALID_PNL_PERIODS:
            allowed = ", ".join(sorted(VALID_PNL_PERIODS))
            raise ValueError(f"PnL period must be one of: {allowed}")
        with self._lock:
            self._pnl_period = normalized

    def get_pnl_period(self) -> Optional[str]:
        with self._lock:
            return self._pnl_period


class TelegramCommandHandler:
    def __init__(
        self,
        token: str,
        chat_id: str,
        thread_id: Optional[int],
        state: TrackerState,
        notifier: TelegramNotifier,
        subscribe_fn: Callable[[str], tuple[bool, Optional[str]]],
        sentiment_fn: Callable[[str], str],
        pnl_fn: Callable[[str, str], str],
        cache: TradeCache,
        lookback_hours: float,
        stop_event: threading.Event,
    ) -> None:
        self.token = token
        self.chat_id = str(chat_id)
        self.thread_id = thread_id
        self.state = state
        self.notifier = notifier
        self.subscribe_fn = subscribe_fn
        self.sentiment_fn = sentiment_fn
        self.pnl_fn = pnl_fn
        self.cache = cache
        self.lookback_hours = lookback_hours
        self.stop_event = stop_event
        self.endpoint = f"https://api.telegram.org/bot{token}/getUpdates"
        self._thread: Optional[threading.Thread] = None
        self._offset: Optional[int] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()

    def _poll_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                params: Dict[str, Any] = {"timeout": 30}
                if self._offset is not None:
                    params["offset"] = self._offset
                resp = requests.get(self.endpoint, params=params, timeout=35)
                if not resp.ok:
                    time.sleep(5)
                    continue
                data = resp.json()
                results = data.get("result", [])
                for item in results:
                    if not isinstance(item, dict):
                        continue
                    self._offset = item.get("update_id", 0) + 1
                    message = item.get("message") or item.get("edited_message")
                    if not isinstance(message, dict):
                        continue
                    chat = message.get("chat") or {}
                    if str(chat.get("id")) != self.chat_id:
                        continue
                    text = message.get("text") or message.get("caption")
                    if not text:
                        continue
                    self._handle_command(text.strip())
            except Exception:  # pragma: no cover
                time.sleep(5)
