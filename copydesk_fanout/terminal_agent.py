from __future__ import annotations

import threading
import time
from typing import Callable, Literal

from .base_agent import BaseAgent

TradeEventType = Literal["opened", "closed", "modified", "partial_closed"]

# Signature: on_trade_event(account_id, event_type, ticket, order_dict)
TradeEventCallback = Callable[[str, TradeEventType, str, dict], None]

# self.dwx.open_orders is kept fresh continuously by dwx_client's own
# check_open_orders loop (confirmed in its source: it reassigns open_orders
# unconditionally on every changed read, then separately decides whether to
# fire on_order_event - the assignment isn't gated on that decision). So
# this loop is just re-reading an already-current in-memory dict, not doing
# its own file I/O - there's no real cost to matching the EA's own ~25ms
# write cadence rather than polling slower than the data actually changes.
_MODIFICATION_POLL_SECONDS = 0.03


class TerminalAgent(BaseAgent):
    """
    Watches one MT5 terminal via DWX Connect and reports opened/closed/
    modified/partial_closed events.

    Important honesty about the mechanism: NOTHING here is a real push from
    MT5. The EA writes its state to a file on a 25ms timer (MILLISECOND_TIMER
    in DWX_Server_MT5.mq5); dwx_client polls that file every ~5ms and updates
    self.open_orders unconditionally on every changed read. Its on_order_event
    callback is just dwx_client's own name for "the poll loop noticed a
    ticket was added or removed" - not an event MT5 pushed to us. Confirmed
    directly in dwx_client.check_open_orders: self.open_orders is reassigned
    BEFORE the decision to fire on_order_event is even made.

    Because of that, self.dwx.open_orders is already fresh at ~25ms
    resolution regardless of whether on_order_event fires. This class runs
    a second loop at a matching ~30ms interval - not a separate slower
    "polling fallback", just reading that same already-current dict on a
    cadence that doesn't add its own lag - specifically to catch SL/TP
    changes and lot-size reductions on existing tickets, since dwx_client's
    own diff only checks for keys being added/removed, never for values
    changing within an existing key.
    """

    def __init__(self, account_id: str, metatrader_dir_path: str, on_trade_event: TradeEventCallback, verbose: bool = True):
        super().__init__(account_id, metatrader_dir_path, verbose=verbose)
        self._on_trade_event = on_trade_event
        self._last_orders: dict[str, dict] = {}
        # Separate snapshot for the modification-poll thread, decoupled from
        # _last_orders (which on_order_event updates) so the two detection
        # paths don't interfere with each other.
        self._last_full_snapshot: dict[str, dict] = {}
        self._modification_thread: threading.Thread | None = None
        self._modification_thread_running = False

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

