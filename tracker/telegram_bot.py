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

    def send(
        self,
        text: str,
        buttons: Optional[List[List[Dict[str, Any]]]] = None,
    ) -> None:
        try:
            payload = {
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": "HTML",
            }
            if self.thread_id is not None:
                payload["message_thread_id"] = self.thread_id
            if buttons:
                payload["reply_markup"] = {"inline_keyboard": buttons}
            resp = requests.post(self.endpoint, json=payload, timeout=15)
            resp.raise_for_status()
        except Exception as exc:  # pragma: no cover
            print(f"Failed to send Telegram message: {exc}")


class TrackerState:
    def __init__(self, history_limit: int = 500) -> None:
        self._history: Deque[str] = deque(maxlen=history_limit)
        self._auto_watch = False
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
        positions_fn: Callable[[str, int], str],
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
        self.positions_fn = positions_fn
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

    def _handle_command(self, text: str) -> None:
        parts = text.split()
        if not parts:
            return
        command = parts[0].split("@")[0].lower()
        args = parts[1:]

        if command in {"/turn_on_auto_watcher", "/turnonauto", "/auto_on"}:
            self.state.set_auto_watch(True)
            self.notifier.send("Auto watcher enabled.")
        elif command in {"/turn_off_auto_watcher", "/turnoffauto", "/auto_off"}:
            self.state.set_auto_watch(False)
            self.notifier.send("Auto watcher disabled. Trades will still be recorded.")
        elif command in {"/last_10_trades", "/last10"}:
            self._send_recent_trades(10)
        elif command in {"/last_20_trades", "/last20"}:
            self._send_recent_trades(20)
        elif command in {"/last_50_trades", "/last50"}:
            self._send_recent_trades(50)
        elif command == "/market_sentiment":
            if not args:
                self.notifier.send("Usage: /market_sentiment COIN")
                return
            coin = args[0].upper()
            summary = self.sentiment_fn(coin)
            self.notifier.send(summary)
        elif command == "/coin_watcher":
            if not args:
                self.notifier.send("Usage: /coin_watcher COIN")
                return
            coin = args[0].upper()
            added, message = self.subscribe_fn(coin)
            self.notifier.send(message if message else f"Watching {coin}.")
        elif command == "/pnl_of_address":
            if not args:
                self.notifier.send("Usage: /pnl_of_address ADDRESS [day|week|month|all]")
                return
            address = args[0]
            period = args[1].lower() if len(args) > 1 else "day"
            self.notifier.send(self.pnl_fn(address, period))
        elif command in {"/pnl_period", "/set_pnl_period", "/pnlperiod"}:
            self._handle_pnl_period_command(args)
        elif command in {"/positions", "/position"}:
            if not args:
                self.notifier.send("Usage: /positions ADDRESS [limit]")
                return
            address = args[0]
            limit = 5
            if len(args) > 1:
                try:
                    limit = max(1, min(20, int(args[1])))
                except ValueError:
                    pass
            report = self.positions_fn(address, limit)
            print(report)
            self.notifier.send(report)
        elif command in {"/help", "/commands"}:
            self._send_help()
        else:
            if command.startswith("/last_") and command.endswith("_trades"):
                try:
                    count = int(command.split("_")[1])
                    self._send_recent_trades(count)
                    return
                except ValueError:
                    pass
            self.notifier.send("Unknown command.")

    def _send_recent_trades(self, count: int) -> None:
        entries = self.state.recent_trades(count)
        if not entries:
            self.notifier.send("No trades recorded yet.")
            return
        self.notifier.send("\n\n".join(entries))

    def _handle_pnl_period_command(self, args: Sequence[str]) -> None:
        if not args:
            current = self.state.get_pnl_period() or "disabled"
            self.notifier.send(
                "Usage: /pnl_period day|week|month|all|off\n"
                f"Current: {current}"
            )
            return
        choice = args[0].lower()
        if choice in VALID_PNL_PERIODS:
            self.state.set_pnl_period(choice)
            self.notifier.send(f"PnL period set to {choice}.")
        elif choice in {"off", "none", "disable", "disabled"}:
            self.state.set_pnl_period(None)
            self.notifier.send("PnL overlay disabled.")
        else:
            allowed = ", ".join(sorted(VALID_PNL_PERIODS))
            self.notifier.send(f"Invalid period. Choose one of: {allowed}, or 'off'.")


    def _send_help(self) -> None:
        commands = [
            "/turn_on_auto_watcher – resume live alerts",
            "/turn_off_auto_watcher – pause live alerts (still records)",
            "/last_10_trades, /last_20_trades, /last_50_trades – show cached alerts",
            "/coin_watcher COIN – add a new coin to the watch list",
            "/market_sentiment COIN – show cached sentiment",
            "/pnl_of_address ADDRESS [period] – address PnL (day/week/month/all)",
            "/pnl_period period|off – change default PnL overlay",
            "/positions ADDRESS [limit] – open positions + recent fills",
        ]
        self.notifier.send("Commands:\n" + "\n".join(commands))


def compute_sentiment_summary(coin: str, cache: TradeCache, lookback_hours: float) -> str:
    normalized = coin.upper()
    trades = cache.recent(normalized)
    if len(trades) < 20:
        try:
            info = Info(skip_ws=True)
            for trade in fetch_recent(info, normalized):
                cache.add(trade)
            trades = cache.recent(normalized)
        except Exception as exc:  # pragma: no cover
            return f"Sentiment fetch failed for {normalized}: {exc}"

    if not trades:
        return f"No trades available for {normalized} in the current lookback window."

    total = len(trades)
    longs = [t for t in trades if t.direction == "long"]
    shorts = [t for t in trades if t.direction == "short"]

    long_notional = sum((t.notional_usd for t in longs), Decimal("0"))
    short_notional = sum((t.notional_usd for t in shorts), Decimal("0"))

    long_pct = (len(longs) / total * 100) if total else 0
    short_pct = (len(shorts) / total * 100) if total else 0

    lines = [
        f"Sentiment for {normalized}",
        f"Window: last {lookback_hours:g}h of cached data",
        f"Total trades: {total}",
        f"Longs: {len(longs)} ({long_pct:.2f}%) | Volume: {format_notional(long_notional)}",
        f"Shorts: {len(shorts)} ({short_pct:.2f}%) | Volume: {format_notional(short_notional)}",
    ]
    return "\n".join(lines)
