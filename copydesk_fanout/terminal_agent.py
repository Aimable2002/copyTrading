from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Literal

from .base_agent import BaseAgent

logger = logging.getLogger("terminal_agent")

TradeEventType = Literal["opened", "closed", "modified", "partial_closed"]

# Signature: on_trade_event(account_id, event_type, ticket, order_dict)
TradeEventCallback = Callable[[str, TradeEventType, str, dict], None]


@dataclass
class ReconciliationResult:
    """Output of TerminalAgent.reconcile() - what the terminal's real state
    looked like versus what we expected, at the moment reconciliation ran."""

    matched: set[str] = field(default_factory=set)
    closed_while_offline: set[str] = field(default_factory=set)
    new_while_offline: set[str] = field(default_factory=set)
    live_orders: dict[str, dict] = field(default_factory=dict)  

_MODIFICATION_POLL_SECONDS = 0.03


class TerminalAgent(BaseAgent):
    def __init__(self, account_id: str, metatrader_dir_path: str, on_trade_event: TradeEventCallback, verbose: bool = True):
        super().__init__(account_id, metatrader_dir_path, verbose=verbose)
        self._on_trade_event = on_trade_event
        self._last_orders: dict[str, dict] = {}
        self._last_full_snapshot: dict[str, dict] = {}
        self._modification_thread: threading.Thread | None = None
        self._modification_thread_running = False

    def reconcile(self, expected_open_tickets: set[str]) -> ReconciliationResult:
        self.dwx.load_orders()
        live = dict(self.dwx.open_orders)
        live_tickets = set(live.keys())

        matched = live_tickets & expected_open_tickets
        closed_while_offline = expected_open_tickets - live_tickets
        new_while_offline = live_tickets - expected_open_tickets

        self._last_orders = dict(live)
        self._last_full_snapshot = dict(live)

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
        self._modification_thread_running = True
        self._modification_thread = threading.Thread(target=self._modification_poll_loop, daemon=True)
        self._modification_thread.start()

    def stop(self) -> None:
        self._modification_thread_running = False
        super().stop()

    def on_order_event(self) -> None:
        current = dict(self.dwx.open_orders)
        print("DEBUG ON_ORDER_EVENT current :", current)

        for ticket, order in current.items():
            if ticket not in self._last_orders:
                self._on_trade_event(self.account_id, "opened", ticket, order)

        for ticket, order in self._last_orders.items():
            if ticket not in current:
                self._on_trade_event(self.account_id, "closed", ticket, order)

        self._last_orders = current

    def _modification_poll_loop(self) -> None:
        while self._modification_thread_running:
            time.sleep(_MODIFICATION_POLL_SECONDS)
            try:
                self._check_for_modifications()
            except Exception:  # noqa: BLE001 - a poll-loop crash should not kill the whole agent
                pass

    def _check_for_modifications(self) -> None:
        current = dict(self.dwx.open_orders)

        for ticket, order in current.items():
            previous = self._last_full_snapshot.get(ticket)
            if previous is None:
                continue  # newly opened - on_order_event already handles this, nothing to compare against yet

            old_lots = previous.get("lots", 0)
            new_lots = order.get("lots", 0)
            if new_lots < old_lots:
                event_order = dict(order)
                event_order["previous_lots"] = old_lots
                self._on_trade_event(self.account_id, "partial_closed", ticket, event_order)
            elif order.get("SL") != previous.get("SL") or order.get("TP") != previous.get("TP"):
                self._on_trade_event(self.account_id, "modified", ticket, order)

        self._last_full_snapshot = current