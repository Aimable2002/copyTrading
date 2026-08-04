from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Literal

from .base_agent import BaseAgent

logger = logging.getLogger("terminal_agent")

TradeEventType = Literal[
    "opened", "closed", "modified", "partial_closed",
    "pending_placed", "pending_cancelled", "pending_triggered",
]

TradeEventCallback = Callable[[str, TradeEventType, str, dict], None]


@dataclass
class ReconciliationResult:

    matched: set[str] = field(default_factory=set)
    closed_while_offline: set[str] = field(default_factory=set)
    new_while_offline: set[str] = field(default_factory=set)
    live_orders: dict[str, dict] = field(default_factory=dict)

_ORDER_EVENT_POLL_SECONDS = 0.03
_MODIFICATION_POLL_SECONDS = 0.03


class TerminalAgent(BaseAgent):
    def __init__(self, account_id: str, terminal_path: str, login: int, password: str, server: str,
                 on_trade_event: TradeEventCallback, max_orders: int = 200, max_lot_size: float = 100.0,
                 verbose: bool = True):
        super().__init__(account_id, terminal_path, login, password, server,
                          max_orders=max_orders, max_lot_size=max_lot_size, verbose=verbose)
        self._on_trade_event = on_trade_event
        self._last_orders: dict[str, dict] = {}
        self._last_pending_orders: dict[str, dict] = {}
        self._last_full_snapshot: dict[str, dict] = {}
        self._order_event_thread: threading.Thread | None = None
        self._modification_thread: threading.Thread | None = None
        self._poll_threads_running = False

    def reconcile(self, expected_open_tickets: set[str]) -> ReconciliationResult:
        live = dict(self.terminal.open_orders)
        live_tickets = set(live.keys())

        matched = live_tickets & expected_open_tickets
        closed_while_offline = expected_open_tickets - live_tickets
        new_while_offline = live_tickets - expected_open_tickets

        self._last_orders = dict(live)
        self._last_full_snapshot = dict(live)
        self._last_pending_orders = dict(self.terminal.pending_orders)

        if closed_while_offline:
            logger.warning(
                "[%s] Reconciliation: %d ticket(s) expected open are no longer in the terminal "
                "(closed while this process was down): %s",
                self.account_id, len(closed_while_offline), sorted(closed_while_offline),
            )
        if new_while_offline:
            logger.warning(
                "[%s] Reconciliation: %d ticket(s) are open in the terminal but weren't tracked "
                "(opened while this process was down, or first-ever run): %s",
                self.account_id, len(new_while_offline), sorted(new_while_offline),
            )
        logger.info(
            "[%s] Reconciliation complete: %d matched, %d closed-while-offline, %d new-while-offline",
            self.account_id, len(matched), len(closed_while_offline), len(new_while_offline),
        )

        return ReconciliationResult(
            matched=matched,
            closed_while_offline=closed_while_offline,
            new_while_offline=new_while_offline,
            live_orders=live,
        )

    def start(self) -> None:
        super().start()
        self._poll_threads_running = True
        self._order_event_thread = threading.Thread(target=self._order_event_poll_loop, daemon=True)
        self._order_event_thread.start()
        self._modification_thread = threading.Thread(target=self._modification_poll_loop, daemon=True)
        self._modification_thread.start()

    def stop(self) -> None:
        self._poll_threads_running = False
        super().stop()

    def _order_event_poll_loop(self) -> None:
        while self._poll_threads_running:
            time.sleep(_ORDER_EVENT_POLL_SECONDS)
            try:
                self.on_order_event()
            except Exception:  
                pass

    def on_order_event(self) -> None:
        current = dict(self.terminal.open_orders)
        current_pending = dict(self.terminal.pending_orders)

        vanished_pending = set(self._last_pending_orders) - set(current_pending)
        triggered = vanished_pending & set(current)  # same ticket, now a position
        cancelled = vanished_pending - triggered

        for ticket in triggered:
            self._on_trade_event(self.account_id, "pending_triggered", ticket, current[ticket])

        for ticket in cancelled:
            self._on_trade_event(self.account_id, "pending_cancelled", ticket, self._last_pending_orders[ticket])

        for ticket, order in current_pending.items():
            if ticket not in self._last_pending_orders:
                self._on_trade_event(self.account_id, "pending_placed", ticket, order)

        for ticket, order in current.items():
            if ticket not in self._last_orders and ticket not in triggered:
                self._on_trade_event(self.account_id, "opened", ticket, order)

        for ticket, order in self._last_orders.items():
            if ticket not in current:
                self._on_trade_event(self.account_id, "closed", ticket, order)

        self._last_orders = current
        self._last_pending_orders = current_pending

    def _modification_poll_loop(self) -> None:
        while self._poll_threads_running:
            time.sleep(_MODIFICATION_POLL_SECONDS)
            try:
                self._check_for_modifications()
            except Exception: 
                pass

    def _check_for_modifications(self) -> None:
        current = dict(self.terminal.open_orders)

        for ticket, order in current.items():
            previous = self._last_full_snapshot.get(ticket)
            if previous is None:
                continue 

            old_lots = previous.get("lots", 0)
            new_lots = order.get("lots", 0)
            if new_lots < old_lots:
                event_order = dict(order)
                event_order["previous_lots"] = old_lots
                self._on_trade_event(self.account_id, "partial_closed", ticket, event_order)
            elif order.get("SL") != previous.get("SL") or order.get("TP") != previous.get("TP"):
                self._on_trade_event(self.account_id, "modified", ticket, order)

        self._last_full_snapshot = current