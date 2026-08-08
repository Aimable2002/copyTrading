"""
CTraderMasterAgent - a second producer of the same trade-event shape
core.terminal_agent.TerminalAgent already produces from MT5. FanoutCore,
trade_history.get_account_trade_history(), sizing.py etc. never need to know
which one they're talking to - see core/base_agent.py and core/terminal_agent.py
for the exact contract this mirrors:

  - .account_id
  - .start() / .stop()
  - .is_connected
  - .balance
  - .reconcile(expected_open_tickets) -> ReconciliationResult
  - .fetch_historic_trades(lookback_days) -> dict[ticket, deal]
  - calls on_trade_event(account_id, event_type, ticket, order_dict) on live events

IMPORTANT - read before wiring this into fanout for real money:
cTrader's execution-event model doesn't map 1:1 onto the seven TradeEventType
strings this codebase uses (those were designed around MT5's polling model in
terminal_agent.py). The mapping below is a reasoned best effort based on the
Open API's documented message shapes, but it has NOT been validated against a
live cTrader account's actual event stream. Per the plan: before this touches a
real follower, run it standalone against one subscribed demo/master account,
log every raw ProtoOAExecutionEvent next to what _translate_event() decides,
and confirm they agree before connecting on_trade_event to the real fanout.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any

from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAAccountAuthReq,
    ProtoOADealListReq,
    ProtoOAExecutionEvent,
    ProtoOAReconcileReq,
    ProtoOATraderReq,
)
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import (
    ProtoOAExecutionType,
    ProtoOAOrderType,
    ProtoOATradeSide,
)

from ..core.terminal_agent import ReconciliationResult, TradeEventCallback
from . import symbol_map, token_store
from .proto_client import get_connection

logger = logging.getLogger("ctrader.master_agent")

_BALANCE_REFRESH_SECONDS = 30.0
_MONEY_DIGITS_DEFAULT = 2  # ProtoOATrader.moneyDigits; balance is an integer scaled by 10**moneyDigits


def _side_to_str(trade_side: int) -> str:
    return "buy" if trade_side == ProtoOATradeSide.BUY else "sell"


class CTraderMasterAgent:
    def __init__(
        self, *, account_id: str, on_trade_event: TradeEventCallback, supabase_client: Any,
    ) -> None:
        self.account_id = account_id
        self._on_trade_event = on_trade_event
        self._supabase_client = supabase_client

        self._ctid: int | None = None
        self._symbols: symbol_map.SymbolCache | None = None
        self._connected = False

        self._balance: float | None = None
        self._balance_lock = threading.Lock()
        self._balance_thread: threading.Thread | None = None
        self._balance_thread_running = False

        # positionId -> last-seen {volume_units, stop_loss, take_profit} snapshot,
        # used to detect partial-close / SL-TP-modify the same way
        # terminal_agent._check_for_modifications() does for MT5, just event-driven
        # instead of polled.
        self._position_snapshots: dict[int, dict] = {}
        # ticket (str positionId) -> full order dict, same shape as MT5's
        # Mt5Terminal.open_orders - exposed via the open_positions property for
        # live_state_publisher.py and anything else that needs current state
        # rather than just modification-detection.
        self._open_positions: dict[str, dict] = {}
        # orderId -> True for pending (unfilled, non-market) orders we've seen
        # ORDER_ACCEPTED for, so a later ORDER_FILLED on the same orderId can be
        # reported as "pending_triggered" rather than "opened".
        self._pending_order_ids: set[int] = set()

    # -- lifecycle ---------------------------------------------------------- #

    def start(self) -> None:
        if self._connected:
            # Already authenticated - e.g. main.py starts cTrader master agents
            # eagerly (right after construction, before fanout.reconcile_all())
            # so self._ctid is populated in time for reconciliation, and then
            # calls start() again later along with every other agent. Guard
            # against redoing the account auth handshake in that second call.
            logger.debug("[%s] start() called again - already connected, skipping", self.account_id)
            return

        self._ctid = token_store.get_ctid_trading_account_id(self.account_id, self._supabase_client)
        self._symbols = symbol_map.SymbolCache(self._ctid)

        conn = get_connection()
        access_token = token_store.get_valid_access_token(self.account_id, self._supabase_client)
        conn.send_and_wait(ProtoOAAccountAuthReq(ctidTraderAccountId=self._ctid, accessToken=access_token))
        conn.subscribe(self._ctid, self._on_message)
        self._connected = True
        logger.info("[%s] cTrader account authenticated (ctid %s)", self.account_id, self._ctid)

        self._refresh_balance()
        self._balance_thread_running = True
        self._balance_thread = threading.Thread(target=self._balance_poll_loop, daemon=True)
        self._balance_thread.start()

    def stop(self) -> None:
        self._balance_thread_running = False
        if self._ctid is not None:
            get_connection().unsubscribe(self._ctid)
        self._connected = False

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def balance(self) -> float | None:
        with self._balance_lock:
            return self._balance

    @property
    def equity(self) -> float | None:
        # Real equity = balance + unrealized P&L of open positions. cTrader's
        # ProtoOATrader has no equity field, only balance - computing this
        # properly needs live floating P&L (a spot-price subscription per open
        # symbol), which isn't built yet. Returning None (unknown) rather than
        # defaulting to balance, since that would silently hide any open risk.
        return None

    @property
    def open_positions(self) -> dict[str, dict]:
        """ticket -> order dict, same shape as MT5's Mt5Terminal.open_orders."""
        return dict(self._open_positions)

    def _balance_poll_loop(self) -> None:
        while self._balance_thread_running:
            time.sleep(_BALANCE_REFRESH_SECONDS)
            try:
                self._refresh_balance()
            except Exception:
                logger.exception("[%s] Balance refresh failed", self.account_id)

    def _refresh_balance(self) -> None:
        response = get_connection().send_and_wait(ProtoOATraderReq(ctidTraderAccountId=self._ctid))
        trader = response.trader
        digits = trader.moneyDigits or _MONEY_DIGITS_DEFAULT
        with self._balance_lock:
            self._balance = trader.balance / (10 ** digits)

    # -- reconciliation / history -------------------------------------------- #

    def reconcile(self, expected_open_tickets: set[str]) -> ReconciliationResult:
        response = get_connection().send_and_wait(ProtoOAReconcileReq(ctidTraderAccountId=self._ctid))
        live: dict[str, dict] = {}
        for position in response.position:
            ticket = str(position.positionId)
            live[ticket] = self._position_to_order_dict(position)
            self._position_snapshots[position.positionId] = {
                "volume": position.tradeData.volume,
                "stop_loss": position.stopLoss,
                "take_profit": position.takeProfit,
            }
        # Full resync, not incremental - anything not in `live` closed while
        # we were offline and must not linger in open_positions.
        self._open_positions = dict(live)
        live_tickets = set(live.keys())

        matched = live_tickets & expected_open_tickets
        closed_while_offline = expected_open_tickets - live_tickets
        new_while_offline = live_tickets - expected_open_tickets

        logger.info(
            "[%s] Reconciliation complete: %d matched, %d closed-while-offline, %d new-while-offline",
            self.account_id, len(matched), len(closed_while_offline), len(new_while_offline),
        )
        return ReconciliationResult(
            matched=matched, closed_while_offline=closed_while_offline,
            new_while_offline=new_while_offline, live_orders=live,
        )

    def fetch_historic_trades(self, lookback_days: int = 30, timeout: float = 10.0) -> dict[str, dict]:
        now_ms = int(time.time() * 1000)
        from_ms = now_ms - lookback_days * 24 * 60 * 60 * 1000
        response = get_connection().send_and_wait(
            ProtoOADealListReq(
                ctidTraderAccountId=self._ctid, fromTimestamp=from_ms, toTimestamp=now_ms, maxRows=1000,
            ),
            timeout=timeout,
        )
        trades: dict[str, dict] = {}
        for deal in response.deal:
            symbol_name = self._symbols.name_for(deal.symbolId)
            lot_size = self._symbols.lot_size_for(deal.symbolId)
            trades[str(deal.dealId)] = {
                "symbol": symbol_name,
                "lots": round(symbol_map.volume_to_lots(deal.filledVolume, lot_size), 2),
                "type": _side_to_str(deal.tradeSide),
                # entry: 0="in"/1="out" in trade_history.py's ENTRY_MAP - cTrader
                # doesn't label deals this way directly; closePositionDetail being
                # set is the closing-deal signal (best-effort, not yet validated
                # against a live deal list - see module docstring).
                "entry": 1 if deal.HasField("closePositionDetail") else 0,
                # MT5's side (core/mt5_terminal.py's deal_to_historic_trade_dict)
                # emits an ISO8601 string via datetime.fromtimestamp(...).isoformat()
                # - the frontend's Deal.deal_time: string contract and
                # lib/trades.ts's parseDealTime() both expect that shape, not a
                # raw epoch integer. cTrader's executionTimestamp is epoch
                # milliseconds, so it must be converted here, not passed through.
                "deal_time": datetime.fromtimestamp(
                    deal.executionTimestamp / 1000, tz=timezone.utc
                ).isoformat(),
                "deal_price": deal.executionPrice,
                "pnl": (
                    deal.closePositionDetail.grossProfit
                    if deal.HasField("closePositionDetail") else 0.0
                ),
                "commission": deal.commission,
                "swap": deal.closePositionDetail.swap if deal.HasField("closePositionDetail") else 0.0,
                "comment": "",
            }
        return trades

    # -- live event handling -------------------------------------------------- #

    def _position_to_order_dict(self, position) -> dict:
        symbol_name = self._symbols.name_for(position.tradeData.symbolId)
        lot_size = self._symbols.lot_size_for(position.tradeData.symbolId)
        return {
            "symbol": symbol_name,
            "lots": symbol_map.volume_to_lots(position.tradeData.volume, lot_size),
            "type": _side_to_str(position.tradeData.tradeSide),
            "SL": position.stopLoss,
            "TP": position.takeProfit,
            "open_price": position.price,
            "comment": position.tradeData.comment,
        }

    def _on_message(self, payload) -> None:
        if not isinstance(payload, ProtoOAExecutionEvent):
            return
        try:
            self._translate_event(payload)
        except Exception:
            logger.exception("[%s] Failed to translate cTrader execution event", self.account_id)

    def _translate_event(self, event: ProtoOAExecutionEvent) -> None:
        exec_type = event.executionType

        if event.HasField("position") and event.position.positionId in self._position_snapshots:
            self._maybe_report_sltp_modification(event.position)

        if exec_type in (
            ProtoOAExecutionType.ORDER_REJECTED,
            ProtoOAExecutionType.ORDER_CANCEL_REJECTED,
            ProtoOAExecutionType.SWAP,
            ProtoOAExecutionType.DEPOSIT_WITHDRAW,
            ProtoOAExecutionType.BONUS_DEPOSIT_WITHDRAW,
        ):
            return  # not a trade-copy-relevant event

        order = event.order if event.HasField("order") else None

        if exec_type == ProtoOAExecutionType.ORDER_ACCEPTED:
            # A pending (non-market) order was accepted but not yet filled.
            if order is not None and order.orderType != ProtoOAOrderType.MARKET:
                self._pending_order_ids.add(order.orderId)
                ticket = str(order.orderId)
                self._on_trade_event(self.account_id, "pending_placed", ticket, self._order_to_dict(order))
            return

        if exec_type in (ProtoOAExecutionType.ORDER_CANCELLED, ProtoOAExecutionType.ORDER_EXPIRED):
            if order is not None and order.orderId in self._pending_order_ids:
                self._pending_order_ids.discard(order.orderId)
                ticket = str(order.orderId)
                self._on_trade_event(self.account_id, "pending_cancelled", ticket, self._order_to_dict(order))
            return

        if exec_type in (ProtoOAExecutionType.ORDER_FILLED, ProtoOAExecutionType.ORDER_PARTIAL_FILL):
            self._handle_fill(event, is_partial=(exec_type == ProtoOAExecutionType.ORDER_PARTIAL_FILL))
            return

        # ORDER_REPLACED (pending-order amendment before fill): no clear
        # equivalent in this codebase's TradeEventType set today - logged, not
        # forwarded. Revisit if a strategy you subscribe to actually amends
        # pending orders before they fill.
        if exec_type == ProtoOAExecutionType.ORDER_REPLACED:
            logger.info("[%s] ORDER_REPLACED received, not forwarded (no mapping yet)", self.account_id)
            return

    def _handle_fill(self, event: ProtoOAExecutionEvent, *, is_partial: bool) -> None:
        if not event.HasField("position"):
            logger.warning("[%s] Fill event with no position field, skipping", self.account_id)
            return
        position = event.position
        order = event.order if event.HasField("order") else None
        position_id = position.positionId
        ticket = str(position_id)
        was_pending = order is not None and order.orderId in self._pending_order_ids
        if was_pending:
            self._pending_order_ids.discard(order.orderId)

        previous = self._position_snapshots.get(position_id)
        order_dict = self._position_to_order_dict(position)

        is_closing = order is not None and order.closingOrder

        if is_closing:
            if previous is not None and previous["volume"] > position.tradeData.volume > 0:
                # Position still has volume left after this fill -> partial close.
                lot_size = self._symbols.lot_size_for(position.tradeData.symbolId)
                order_dict["previous_lots"] = symbol_map.volume_to_lots(previous["volume"], lot_size)
                self._on_trade_event(self.account_id, "partial_closed", ticket, order_dict)
                self._position_snapshots[position_id] = {
                    "volume": position.tradeData.volume,
                    "stop_loss": position.stopLoss,
                    "take_profit": position.takeProfit,
                }
                self._open_positions[ticket] = order_dict
            else:
                # Fully closed - report using the pre-close snapshot's size/side
                # if we have it (position.tradeData.volume is likely 0 by now).
                self._on_trade_event(self.account_id, "closed", ticket, order_dict)
                self._position_snapshots.pop(position_id, None)
                self._open_positions.pop(ticket, None)
            return

        if is_partial:
            # Partial fill of the OPENING order (position still building) - no
            # existing TradeEventType represents "position partially opened, not
            # yet complete", same as MT5's model. Wait for the final fill.
            return

        event_type = "pending_triggered" if was_pending else "opened"
        self._on_trade_event(self.account_id, event_type, ticket, order_dict)
        self._position_snapshots[position_id] = {
            "volume": position.tradeData.volume,
            "stop_loss": position.stopLoss,
            "take_profit": position.takeProfit,
        }
        self._open_positions[ticket] = order_dict

    def _maybe_report_sltp_modification(self, position) -> None:
        """
        Called from _translate_event BEFORE the snapshot is overwritten by
        other handlers, so this always compares against the prior known
        state - a freshly opened position (no prior snapshot) has nothing to
        compare and is skipped.

        UNVERIFIED: it isn't confirmed that amending SL/TP on an open position
        via cTrader Copy's own execution engine surfaces as a
        ProtoOAExecutionEvent at all versus some other message type. Log raw
        events against a real subscribed master (per the module docstring)
        before relying on this to catch SL/TP changes in production.
        """
        previous = self._position_snapshots.get(position.positionId)
        if previous is None:
            return
        if previous["stop_loss"] != position.stopLoss or previous["take_profit"] != position.takeProfit:
            ticket = str(position.positionId)
            order_dict = self._position_to_order_dict(position)
            self._on_trade_event(self.account_id, "modified", ticket, order_dict)
            # Must update here, not just rely on a later fill handler to do it -
            # if no fill follows (a pure SL/TP amend with no volume change),
            # leaving the snapshot stale would make every subsequent event
            # re-compare against this same old value and re-fire "modified"
            # again even when nothing further has changed.
            previous["stop_loss"] = position.stopLoss
            previous["take_profit"] = position.takeProfit
            self._open_positions[ticket] = order_dict

    def _order_to_dict(self, order) -> dict:
        symbol_name = self._symbols.name_for(order.tradeData.symbolId)
        lot_size = self._symbols.lot_size_for(order.tradeData.symbolId)
        return {
            "symbol": symbol_name,
            "lots": symbol_map.volume_to_lots(order.tradeData.volume, lot_size),
            "type": _side_to_str(order.tradeData.tradeSide),
            "SL": order.stopLoss,
            "TP": order.takeProfit,
            "open_price": order.limitPrice or order.stopPrice,
            "comment": order.tradeData.comment,
        }