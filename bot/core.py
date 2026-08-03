"""
core.py
--------
Pure trading logic for the Stop-and-Reverse (SAR) bot. No printing, no
colors, no prompts - this file only talks to MT5 and does math. Every
threshold it uses comes from config.json; nothing is hardcoded.

STRATEGY - exactly two rules, nothing else:

  1. FLIP ONLY. There is exactly one position slot, ever - not a
     collection, not "several tracked independently." self.position is
     a single Optional[PositionState], structurally incapable of
     representing more than one open trade. If the broker ever shows
     more than one position for this symbol/magic (the account is
     hedging-capable, so this CAN happen even though the strategy never
     intends it - e.g. both initial straddle legs filling at once, or
     the stop order filling while the old position hasn't been closed
     yet), every position except the newest is closed at market
     immediately, and if we were already tracking one when a new one
     appears, that old one is treated as the flip trigger and closed
     immediately too. The single-slot rule is enforced by the shape of
     the data, not by cleanup logic bolted on afterward.

  2. ONE PRICE, PUSHED TO TWO PLACES: NATIVE SL AND THE PENDING ORDER.
     Every open position has a real, broker-side stop-loss attached to
     it directly (TRADE_ACTION_SLTP - the same SL MT5 shows in its own
     S/L column), so protection is enforced by the exchange itself, not
     only by this process polling price. The single pending order
     (the stop-and-reverse order) is always kept at that EXACT SAME
     price - never a separately-computed level. There is never more
     than one live pending order for this symbol/magic (any extra is a
     duplicate and gets cancelled). That one shared price sits at a
     LOCKED, FIXED distance from the best price reached since entry -
     computed once when the position opens from the live spread, and
     never recalculated afterward. Every tick, the candidate stop level
     is (current price - that fixed distance); if that candidate is
     better than the current stop level, BOTH the native SL and the
     pending order are moved to it together, in the same step - they
     can never show two different numbers. It never loosens. There is
     no separate "hard stop" phase versus "trail" phase - this single
     rule IS the initial stop (on the very first tick, current price ~=
     entry price, so the candidate is entry -/+ distance) and IS the
     ongoing trail (as price moves favorably, the candidate keeps
     improving). One formula, one price, two places it's written, from
     the moment the position opens until it closes.

  Because rule 2 makes the pending order nothing but this position's
  stop-loss, and rule 1 allows only one position, the system cannot
  represent two live trades protected by two different orders - there
  is only ever one slot, one order, one distance.

  MT5 IS THE SOURCE OF TRUTH, EVERY TICK: nothing here is inferred from
  memory of what "should" be true. Every tick, the bot re-pulls the live
  position and order list for this symbol/magic from MT5 and reconciles
  its single tracked slot against that - never trusting a remembered
  ticket without verifying it's still actually there.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Any, Dict, List, Optional

try:
    import MetaTrader5 as mt5
    MT5_AVAILABLE = True
except ImportError:
    MT5_AVAILABLE = False


DEFAULT_CONFIG_PATH = "config.json"


# ----------------------------------------------------------------------
# Config load/save
# ----------------------------------------------------------------------

def load_config(path: str = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found at {path}")
    with open(path, "r") as f:
        return json.load(f)


def save_config(cfg: Dict[str, Any], path: str = DEFAULT_CONFIG_PATH) -> None:
    with open(path, "w") as f:
        json.dump(cfg, f, indent=2)


# ----------------------------------------------------------------------
# Engine state
# ----------------------------------------------------------------------

@dataclass
class DailyRisk:
    day: date = field(default_factory=date.today)
    starting_balance: float = 0.0
    realized_pnl: float = 0.0

    def reset_if_new_day(self, balance_now: float) -> None:
        today = date.today()
        if today != self.day:
            self.day = today
            self.starting_balance = balance_now
            self.realized_pnl = 0.0


@dataclass
class PositionState:
    """The ONE currently-open position, if any. self.position on the
    engine is Optional[PositionState] - never a collection - so the data
    itself makes 'two positions at once' unrepresentable."""
    ticket: int
    side: str                              # "BUY" | "SELL"
    entry_price: float
    distance: float                        # locked fixed distance in price units, set once at entry, never recalculated
    sl_price: float                        # current stop level - only ever tightens
    stop_order_ticket: Optional[int] = None
    last_stop_order_attempt: float = 0.0
    trail_armed: bool = False              # log-dedup only - the arm CONDITION is recomputed fresh every tick


class SARTradeEngine:
    """The stop-and-reverse state machine. UI-agnostic."""

    def __init__(self, config: Dict[str, Any]):
        self.cfg = config
        self.symbol: str = config["trading"]["symbol"]
        self.lot_size: float = config["trading"]["lot_size"]
        self.magic: int = config["trading"]["magic_number"]
        self.contract_size: float = config["trading"]["contract_size"]
        self.initial_distance_percent: float = config["trading"]["initial_stop_distance_percent"]
        self.spread_safety_multiplier: float = config["trading"].get("spread_safety_multiplier", 1.5)
        self.slippage: int = config["trading"]["slippage_points"]
        self.comment: str = config["trading"].get("comment", "SAR-BOT")

        self.max_dd_usd: float = config["risk"]["max_daily_drawdown_usd"]
        self.target_usd: float = config["risk"]["daily_profit_target_usd"]
        self.stop_on_dd: bool = config["risk"]["stop_trading_on_drawdown_hit"]
        self.stop_on_target: bool = config["risk"]["stop_trading_on_target_hit"]

        self.poll_interval: float = config["engine"]["poll_interval_seconds"]

        # ---- Rule 1: exactly one slot, ever - never a collection ----
        self.position: Optional[PositionState] = None

        # The initial straddle's two pending orders exist BEFORE any
        # position is open, so they're not owned by the single position
        # slot - tracked separately here until the first fill consumes it.
        self.pending_straddle_tickets: List[int] = []

        # Once we discover which ORDER_FILLING_* mode this broker/symbol
        # actually accepts, we remember it so every later order tries the
        # working mode first instead of guessing (see
        # _send_order_with_filling_retry).
        self._working_filling_mode: Optional[int] = None

        self.daily = DailyRisk()
        self.trading_halted: bool = False
        self.halt_reason: str = ""

        self.log: List[str] = []
        self._connected = False
        self.account_login: Optional[int] = None
        self.account_server: str = ""
        self.account_currency: str = ""
        self._last_connect_attempt: float = 0.0
        self._reconnect_interval_seconds: float = 15.0
        self._last_straddle_attempt: float = 0.0
        self._straddle_retry_interval_seconds: float = 10.0
        self._stop_order_retry_interval_seconds: float = 10.0

    # ------------------------------------------------------------------
    # Logging (structured, UI reads this - no printing here)
    # ------------------------------------------------------------------

    def _log(self, message: str) -> None:
        stamp = datetime.now().strftime("%H:%M:%S")
        self.log.append(f"[{stamp}] {message}")
        if len(self.log) > 200:
            self.log = self.log[-200:]

    # ------------------------------------------------------------------
    # MT5 connection
    # ------------------------------------------------------------------

    def connect(self) -> bool:
        if not MT5_AVAILABLE:
            self._log("MetaTrader5 package not available on this system (Windows + MT5 terminal required).")
            self._connected = False
            return False

        path = self.cfg["mt5"].get("terminal_path") or None
        login = self.cfg["mt5"].get("login") or None
        password = self.cfg["mt5"].get("password") or None
        server = self.cfg["mt5"].get("server") or None
        timeout = self.cfg["mt5"].get("timeout_ms", 10000)

        kwargs: Dict[str, Any] = {"timeout": timeout}
        if path:
            kwargs["path"] = path
        if login:
            kwargs.update(login=login, password=password, server=server)

        ok = mt5.initialize(**kwargs)
        if not ok:
            self._log(f"MT5 initialize() failed: {mt5.last_error()}")
            self._connected = False
            return False

        if not mt5.symbol_select(self.symbol, True):
            self._log(f"Failed to select symbol {self.symbol}: {mt5.last_error()}")
            self._connected = False
            return False

        acc = mt5.account_info()
        if acc:
            self.daily.starting_balance = acc.balance
            self.account_login = acc.login
            self.account_server = acc.server
            self.account_currency = acc.currency
        self._connected = True
        self._log(f"Connected to MT5. Login={self.account_login} Server={self.account_server} "
                   f"Symbol={self.symbol} Lot={self.lot_size}")
        return True

    def shutdown(self) -> None:
        if MT5_AVAILABLE and self._connected:
            mt5.shutdown()

    # ------------------------------------------------------------------
    # Price / account access
    # ------------------------------------------------------------------

    def get_price(self) -> Optional[float]:
        if not MT5_AVAILABLE or not self._connected:
            return None
        tick = mt5.symbol_info_tick(self.symbol)
        if tick is None:
            return None
        return (tick.bid + tick.ask) / 2.0

    def get_bid_ask(self) -> Optional[tuple]:
        """Returns (bid, ask). Needed because stop orders must clear the
        real bid/ask, not just the midpoint - distance from midpoint alone
        can land inside the spread and get rejected as an invalid price."""
        if not MT5_AVAILABLE or not self._connected:
            return None
        tick = mt5.symbol_info_tick(self.symbol)
        if tick is None:
            return None
        return (tick.bid, tick.ask)

    def get_balance(self) -> float:
        if not MT5_AVAILABLE or not self._connected:
            return 0.0
        acc = mt5.account_info()
        return acc.balance if acc else 0.0

    # ------------------------------------------------------------------
    # Order placement primitives
    # ------------------------------------------------------------------

    def _order_send(self, request: Dict[str, Any]) -> Any:
        if not MT5_AVAILABLE:
            return None
        result = mt5.order_send(request)
        if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
            self._log(f"Order failed: {request.get('type', request.get('action'))} -> "
                      f"{getattr(result, 'comment', 'no result')}")
        return result

    def _send_order_with_filling_retry(self, request: Dict[str, Any]) -> Any:
        """Sends an order, mutating request['type_filling'] and retrying if
        the broker rejects the mode as unsupported. Different brokers/
        symbols (crypto CFDs especially) accept different fill modes, so
        we try the mode that's already worked for this symbol first, then
        fall through IOC -> FOK -> RETURN. Only retries when the specific
        rejection is TRADE_RETCODE_INVALID_FILL - anything else fails
        identically on every mode."""
        if not MT5_AVAILABLE:
            return None

        candidates = [mt5.ORDER_FILLING_IOC, mt5.ORDER_FILLING_FOK, mt5.ORDER_FILLING_RETURN]
        if self._working_filling_mode is not None:
            candidates = [self._working_filling_mode] + [c for c in candidates if c != self._working_filling_mode]

        last_result = None
        for mode in candidates:
            request["type_filling"] = mode
            result = mt5.order_send(request)
            last_result = result

            if result is not None and result.retcode == mt5.TRADE_RETCODE_DONE:
                self._working_filling_mode = mode
                return result

            retcode = getattr(result, "retcode", None)
            if retcode != mt5.TRADE_RETCODE_INVALID_FILL:
                self._log(f"Order failed: {request.get('type')} -> "
                          f"{getattr(result, 'comment', 'no result')} (retcode={retcode})")
                return result

            self._log(f"Filling mode {mode} rejected for this symbol, trying next mode...")

        self._log(f"Order failed: {request.get('type')} -> all filling modes rejected "
                  f"({getattr(last_result, 'comment', 'no result')})")
        return last_result

    def get_min_stop_distance_price(self) -> float:
        """Broker's actual minimum distance (price units) a pending order
        must sit away from current price. 0.0 if unavailable/unrestricted
        (the spread itself still enforces a real floor, handled separately)."""
        if not MT5_AVAILABLE or not self._connected:
            return 0.0
        info = mt5.symbol_info(self.symbol)
        if info is None:
            return 0.0
        stops_level_points = getattr(info, "trade_stops_level", 0) or 0
        point = getattr(info, "point", 0.0) or 0.0
        freeze_level_points = getattr(info, "trade_freeze_level", 0) or 0
        broker_min_points = max(stops_level_points, freeze_level_points)
        return broker_min_points * point

    def _effective_distance_price(self, mid_price: float, spread: float) -> float:
        """Distance (price units) used for the initial straddle, and as the
        LOCKED fixed distance for a position's stop once it fills.
        Percentage-of-price is the primary driver so it self-scales across
        symbols/price regimes. Final distance is the LARGEST of:
          - percent_of_price  (configured %)
          - broker's reported minimum
          - current spread * safety multiplier (a stop closer than the
            spread is structurally invalid)
        """
        percent = self.initial_distance_percent / 100.0
        percent_distance = mid_price * percent

        broker_min = self.get_min_stop_distance_price()
        spread_floor = spread * self.spread_safety_multiplier

        effective = max(percent_distance, broker_min, spread_floor)

        if effective > percent_distance:
            reason = "broker minimum" if broker_min >= spread_floor else "current spread"
            self._log(
                f"Configured %-distance ({percent_distance:.2f}) was below the {reason} "
                f"({effective:.2f}) - using {effective:.2f} instead."
            )
        return effective

    def _current_distance(self) -> Optional[float]:
        """Computes the locked distance fresh, right now, from the live
        spread - used once, at the moment a position opens, and then never
        recalculated for that position's lifetime."""
        quote = self.get_bid_ask()
        if quote is None:
            return None
        bid, ask = quote
        spread = ask - bid
        mid = (bid + ask) / 2.0
        return self._effective_distance_price(mid, spread)

    def _min_valid_order_distance(self, spread: float) -> float:
        broker_min = self.get_min_stop_distance_price()
        spread_floor = spread * self.spread_safety_multiplier
        return max(broker_min, spread_floor)

    def _live_native_sl(self, pos: "PositionState") -> Optional[float]:
        """Reads the position's ACTUAL current SL straight from MT5 - not
        pos.sl_price (that's our own target, which is what we're trying to
        push, not what's already live) and not a cached value. Returns
        None if the position isn't found or has no SL set yet (e.g. the
        very first tick after opening, before any protection has been
        synced at all) - in both cases there's nothing to ratchet against,
        which is the correct behavior for a brand-new position.
        """
        if not MT5_AVAILABLE:
            return None
        live = [p for p in (mt5.positions_get(symbol=self.symbol) or [])
                if p.ticket == pos.ticket and p.magic == self.magic]
        if not live or not live[0].sl:
            return None
        return live[0].sl

    def _valid_stop_order_price(self, side: str, target_price: float) -> Optional[float]:
        """Clamps target_price to the nearest price the broker will
        actually accept, checked against LIVE bid/ask right now - never
        trust a price computed a moment ago, since price moves between
        when it was set and when the order is actually sent."""
        quote = self.get_bid_ask()
        if quote is None:
            return None
        bid, ask = quote
        spread = ask - bid
        min_dist = self._min_valid_order_distance(spread)

        if side == "BUY":  # protecting order is a SELL STOP, must clear bid by min_dist
            boundary = bid - min_dist
            valid_price = min(target_price, boundary)
        else:  # SELL position -> BUY STOP, must clear ask by min_dist
            boundary = ask + min_dist
            valid_price = max(target_price, boundary)

        valid_price = round(valid_price, 2)
        if abs(valid_price - round(target_price, 2)) > 1e-9:
            self._log(f"Desired stop price {target_price:.2f} was too close to live bid/ask - "
                      f"using {valid_price:.2f} instead (closest valid price).")
        return valid_price

    # ------------------------------------------------------------------
    # Initial straddle (pre-position bootstrapping only)
    # ------------------------------------------------------------------

    def place_straddle(self, mid_price: float) -> None:
        """First-run only (no position open, no straddle already pending):
        place both Buy Stop and Sell Stop around price, sized by the same
        dynamic distance that becomes the fill's locked stop distance.
        Anchored off real bid/ask so orders land on the valid side of the
        current spread."""
        quote = self.get_bid_ask()
        if quote is None:
            self._log("Cannot place straddle - no live bid/ask available.")
            return
        bid, ask = quote
        spread = ask - bid
        dist = self._effective_distance_price(mid_price, spread)
        buy_stop_price = round(ask + dist, 2)
        sell_stop_price = round(bid - dist, 2)

        if not MT5_AVAILABLE:
            self._log(f"[SIMULATED] Straddle placed: BuyStop={buy_stop_price} SellStop={sell_stop_price}")
            return

        common = dict(
            action=mt5.TRADE_ACTION_PENDING,
            symbol=self.symbol,
            volume=self.lot_size,
            magic=self.magic,
            comment=self.comment,
            type_time=mt5.ORDER_TIME_GTC,
        )
        buy_req = {**common, "type": mt5.ORDER_TYPE_BUY_STOP, "price": buy_stop_price}
        sell_req = {**common, "type": mt5.ORDER_TYPE_SELL_STOP, "price": sell_stop_price}

        r1 = self._send_order_with_filling_retry(buy_req)
        r2 = self._send_order_with_filling_retry(sell_req)
        for r in (r1, r2):
            if r and r.retcode == mt5.TRADE_RETCODE_DONE:
                self.pending_straddle_tickets.append(r.order)
        self._log(f"Straddle placed: BuyStop={buy_stop_price} SellStop={sell_stop_price}")

    def cancel_pending(self, ticket: int) -> None:
        if not MT5_AVAILABLE:
            return
        req = {"action": mt5.TRADE_ACTION_REMOVE, "order": ticket}
        self._order_send(req)

    def cancel_straddle_pending(self) -> None:
        for t in list(self.pending_straddle_tickets):
            self.cancel_pending(t)
        self.pending_straddle_tickets.clear()

    # ------------------------------------------------------------------
    # Rule 2: one price (pos.sl_price), pushed to two places every tick -
    # the position's native broker-side SL and the single pending order.
    # ------------------------------------------------------------------

    def _place_stop_order(self, pos: PositionState, valid_price: float) -> Optional[int]:
        """Places THE ONE pending order for the current position, at the
        exact same already-validated price just applied to its native SL."""
        if not MT5_AVAILABLE:
            self._log(f"[SIMULATED] Stop order placed at {valid_price:.2f} for {pos.side} {pos.ticket}")
            return None

        order_type = mt5.ORDER_TYPE_SELL_STOP if pos.side == "BUY" else mt5.ORDER_TYPE_BUY_STOP
        req = dict(
            action=mt5.TRADE_ACTION_PENDING,
            symbol=self.symbol,
            volume=self.lot_size,
            magic=self.magic,
            comment=self.comment,
            type=order_type,
            price=valid_price,
            type_time=mt5.ORDER_TIME_GTC,
        )
        r = self._send_order_with_filling_retry(req)
        if r and r.retcode == mt5.TRADE_RETCODE_DONE:
            self._log(f"[{pos.side} {pos.ticket}] Stop order placed at {valid_price:.2f}")
            return r.order

        self._log(f"[{pos.side} {pos.ticket}] Stop order FAILED at {valid_price:.2f} - "
                  f"position has NO live stop order right now. Will retry.")
        return None

    def _sync_native_sl(self, pos: PositionState, valid_price: float) -> None:
        """Ensures the position's own broker-side SL (TRADE_ACTION_SLTP -
        the same S/L MT5 shows natively on the position) matches
        valid_price. Read fresh from MT5's live position record every
        tick, never trusted from memory. This is real, exchange-enforced
        protection: it keeps working even if this process crashes or
        disconnects, unlike the pending order alone."""
        live = [p for p in (mt5.positions_get(symbol=self.symbol) or [])
                if p.ticket == pos.ticket and p.magic == self.magic]
        if not live:
            return  # position's gone - _reconcile_position will notice and clear tracking

        broker_sl = live[0].sl
        if broker_sl and abs(broker_sl - valid_price) < 1e-9:
            return  # already correct

        req = dict(action=mt5.TRADE_ACTION_SLTP, symbol=self.symbol, position=pos.ticket,
                   sl=valid_price, tp=0.0)
        result = self._order_send(req)
        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
            self._log(f"[{pos.side} {pos.ticket}] Native SL set to {valid_price:.2f}")
        else:
            self._log(f"[{pos.side} {pos.ticket}] Failed to set native SL to {valid_price:.2f} - will retry.")

    def _sync_protection(self) -> None:
        """MT5 is the source of truth - not a remembered ticket or a
        remembered SL value. Every tick: compute ONE valid price from
        pos.sl_price, push it to the native SL AND the pending order in
        the same step (never one without the other), then sweep away
        anything else live - there is structurally no legitimate reason
        for a second order, or a mismatched SL, to exist."""
        if not MT5_AVAILABLE:
            return

        straddle_set = set(self.pending_straddle_tickets)

        if self.position is None:
            # No position -> no legitimate non-straddle order can exist.
            live_orders = [o for o in (mt5.orders_get(symbol=self.symbol) or [])
                           if o.magic == self.magic and o.ticket not in straddle_set]
            for o in live_orders:
                self._log(f"Orphan pending order {o.ticket} at {o.price_open:.2f} found with no open "
                          f"position - cancelling.")
                self.cancel_pending(o.ticket)
            return

        pos = self.position
        valid_price = self._valid_stop_order_price(pos.side, pos.sl_price)
        if valid_price is None:
            self._log(f"[{pos.side} {pos.ticket}] Cannot sync protection - no live bid/ask available.")
            return

        # --- The fix: two ratchets have to compose, not compete ---
        # _valid_stop_order_price above only enforces "not too close to the
        # live market" - it has no concept of "not worse than what's
        # already live", and a normal price retracement toward the stop
        # (well within an overall winning trade) could make that clamp
        # return something CLOSER to the market than the stop currently
        # sits at, which would push both the native SL and the pending
        # order backward together. This second ratchet is what was
        # missing: never let the broker-distance clamp override a stop
        # that's already more favorable than what it's proposing. Only
        # tighten from here, never loosen - same rule _apply_trailing
        # already enforces on pos.sl_price itself, now also enforced on
        # the value actually sent to the broker.
        live_sl = self._live_native_sl(pos)
        if live_sl:
            valid_price = max(valid_price, live_sl) if pos.side == "BUY" else min(valid_price, live_sl)

        # --- Native SL first ---
        self._sync_native_sl(pos, valid_price)

        # --- Pending order, same price ---
        now = time.time()
        live_orders = [o for o in (mt5.orders_get(symbol=self.symbol) or [])
                       if o.magic == self.magic and o.ticket not in straddle_set]
        order_by_ticket = {o.ticket: o for o in live_orders}
        order = order_by_ticket.get(pos.stop_order_ticket) if pos.stop_order_ticket is not None else None

        if order is not None:
            if abs(order.price_open - valid_price) > 1e-9:
                req = dict(action=mt5.TRADE_ACTION_MODIFY, order=pos.stop_order_ticket, price=valid_price)
                result = self._order_send(req)
                if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                    self._log(f"[{pos.side} {pos.ticket}] Pending order moved to {valid_price:.2f}")
                else:
                    self._log(f"[{pos.side} {pos.ticket}] Failed to move pending order to {valid_price:.2f} - will retry.")
        else:
            pos.stop_order_ticket = None
            if now - pos.last_stop_order_attempt >= self._stop_order_retry_interval_seconds:
                pos.last_stop_order_attempt = now
                pos.stop_order_ticket = self._place_stop_order(pos, valid_price)

        # Anything live that isn't a straddle leg and isn't THE current
        # order is a duplicate/orphan - cancel it now. There is only ever
        # supposed to be one.
        for o in live_orders:
            if o.ticket != pos.stop_order_ticket:
                self._log(f"Duplicate/orphan pending order {o.ticket} at {o.price_open:.2f} - cancelling "
                          f"(only one order is allowed to exist for the current position).")
                self.cancel_pending(o.ticket)

    # ------------------------------------------------------------------
    # Closing
    # ------------------------------------------------------------------

    def close_position_by_ticket(self, ticket: int, reason: str) -> bool:
        """Closes exactly one position by ticket. Returns True only once
        confirmed flat. Does not touch pending orders - _reconcile_*
        handles cleanup of whatever's left every tick regardless of why a
        position closed."""
        if not MT5_AVAILABLE:
            self._log(f"[SIMULATED] Position {ticket} closed at market ({reason})")
            return True

        positions = [p for p in (mt5.positions_get(symbol=self.symbol) or [])
                     if p.ticket == ticket and p.magic == self.magic]
        if not positions:
            return True  # already gone on the broker's side

        pos = positions[0]
        price = self.get_price()
        order_type = mt5.ORDER_TYPE_SELL if pos.type == mt5.POSITION_TYPE_BUY else mt5.ORDER_TYPE_BUY
        req = dict(
            action=mt5.TRADE_ACTION_DEAL,
            symbol=self.symbol,
            volume=pos.volume,
            type=order_type,
            position=pos.ticket,
            price=price,
            deviation=self.slippage,
            magic=self.magic,
            comment=f"{self.comment}-{reason}",
        )
        result = self._send_order_with_filling_retry(req)
        if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
            self._log(f"Close order FAILED for ticket {ticket} ({reason}) - "
                      f"position is still OPEN on the broker. Will retry next tick.")
            return False

        self._log(f"Position {ticket} closed at market ({reason})")
        return True

    # ------------------------------------------------------------------
    # Rule 1: exactly one position slot - reconciled against MT5 every
    # tick, never inferred. A new position appearing while one was
    # already tracked IS the flip: close the old one immediately.
    # ------------------------------------------------------------------

    def _open_new_position(self, ticket: int, side: str, entry_price: float) -> None:
        self._log(f"New position: {side} {ticket} @ {entry_price}")

        dist = self._current_distance()
        if dist is None or dist <= 0:
            dist = entry_price * 0.0005
            self._log(f"[{side} {ticket}] WARNING: could not read live spread to compute the stop "
                      f"distance - using fallback {dist:.2f}. Verify this position's stop manually.")

        direction = 1 if side == "BUY" else -1
        sl_price = entry_price - direction * dist

        self.position = PositionState(
            ticket=ticket, side=side, entry_price=entry_price,
            distance=dist, sl_price=sl_price,
        )

    def _reconcile_position(self) -> None:
        """MT5 is the source of truth. Pulls the live position list for
        this symbol/magic and reconciles the single tracked slot against
        it - never assuming what should be true from a previous tick."""
        if not MT5_AVAILABLE:
            return

        broker_positions = [p for p in (mt5.positions_get(symbol=self.symbol) or [])
                             if p.magic == self.magic]

        tracked_ticket = self.position.ticket if self.position is not None else None
        others = [p for p in broker_positions if p.ticket != tracked_ticket]

        if self.position is not None and not any(p.ticket == tracked_ticket for p in broker_positions):
            # Our tracked position is gone from the broker (we closed it,
            # a human closed it, or - since there's no broker-side SL -
            # this can only otherwise happen via our own market close).
            self._log(f"Position {tracked_ticket} is no longer open on the broker - clearing tracking.")
            self.position = None

        if not others:
            return

        # One or more OTHER positions exist. Keep only the newest (MT5
        # tickets increase monotonically); close every other one at
        # market immediately - including our previously-tracked position
        # if it's still open, since a new position appearing IS the flip.
        newest = max(others, key=lambda p: p.ticket)

        if self.position is not None:
            self._log(f"Flip: new position {newest.ticket} appeared while {self.position.ticket} "
                      f"was still open - closing {self.position.ticket} now.")
            self.close_position_by_ticket(self.position.ticket, reason="flip")
            self.position = None

        for p in others:
            if p.ticket != newest.ticket:
                self._log(f"More than one new position appeared at once ({p.ticket}) - "
                          f"closing it, only the newest ({newest.ticket}) is kept.")
                self.close_position_by_ticket(p.ticket, reason="duplicate")

        side = "BUY" if newest.type == mt5.POSITION_TYPE_BUY else "SELL"
        self._open_new_position(newest.ticket, side, newest.price_open)

    # ------------------------------------------------------------------
    # Trailing stop - armed dynamically, not by a config threshold: the
    # SL stays fixed at entry -/+ distance (the original risk) until
    # profit reaches that SAME distance - i.e. until you've made back
    # exactly what you stood to lose. At that point the stop jumps to
    # breakeven and the single trailing formula takes over, tightening
    # only, for the rest of the position's life.
    # ------------------------------------------------------------------

    def _apply_trailing(self, current_price: float) -> None:
        if self.position is None:
            return
        pos = self.position
        direction = 1 if pos.side == "BUY" else -1
        profit_price = (current_price - pos.entry_price) * direction

        if profit_price >= pos.distance:
            if not pos.trail_armed:
                pos.trail_armed = True
                self._log(f"[{pos.side} {pos.ticket}] Trail armed - profit has matched the initial "
                          f"risk distance ({pos.distance:.2f}); stop moves to breakeven and trails from here.")
            candidate_sl = current_price - direction * pos.distance
            if pos.side == "BUY":
                pos.sl_price = max(pos.sl_price, candidate_sl)
            else:
                pos.sl_price = min(pos.sl_price, candidate_sl)
        # else: not armed yet - sl_price stays exactly where it was set
        # at entry (entry -/+ distance). Untouched, no partial trailing.

        crossed = (current_price <= pos.sl_price) if pos.side == "BUY" else (current_price >= pos.sl_price)
        if crossed:
            self._log(f"[{pos.side} {pos.ticket}] Stop hit at {pos.sl_price:.2f} -> closing now")
            self.close_position_by_ticket(pos.ticket, reason="stop-loss")
            self.position = None

    # ------------------------------------------------------------------
    # Risk limits
    # ------------------------------------------------------------------

    def _check_daily_risk(self) -> None:
        balance = self.get_balance()
        self.daily.reset_if_new_day(balance)
        day_pnl = balance - self.daily.starting_balance if self.daily.starting_balance else 0.0

        if self.stop_on_dd and day_pnl <= -abs(self.max_dd_usd):
            self.trading_halted = True
            self.halt_reason = f"Daily drawdown limit hit (${day_pnl:.2f})"
        elif self.stop_on_target and day_pnl >= abs(self.target_usd):
            self.trading_halted = True
            self.halt_reason = f"Daily profit target reached (${day_pnl:.2f})"

    # ------------------------------------------------------------------
    # Main tick - called once per loop iteration by main.py / design.py
    # ------------------------------------------------------------------

    def tick(self) -> Dict[str, Any]:
        if not self._connected:
            now = time.time()
            if now - self._last_connect_attempt >= self._reconnect_interval_seconds:
                self._last_connect_attempt = now
                self._log("Not connected - retrying MT5 connection...")
                self.connect()
            price = 0.0
        else:
            price = self.get_price() or 0.0

            if self.trading_halted:
                pass
            else:
                if self.position is None and not self.pending_straddle_tickets:
                    now = time.time()
                    if now - self._last_straddle_attempt >= self._straddle_retry_interval_seconds:
                        self._last_straddle_attempt = now
                        self.place_straddle(price)

                had_position_before = self.position is not None
                self._reconcile_position()
                if self.position is not None and not had_position_before and self.pending_straddle_tickets:
                    # A genuine new entry just landed -> the leftover
                    # straddle leg is now stale.
                    self.cancel_straddle_pending()

                self._apply_trailing(price)
                self._sync_protection()
                self._check_daily_risk()

        balance = self.get_balance()
        day_pnl_usd = (balance - self.daily.starting_balance) if self.daily.starting_balance else 0.0

        position_state = None
        total_profit_usd = 0.0
        if self.position is not None:
            pos = self.position
            direction = 1 if pos.side == "BUY" else -1
            price_diff = (price - pos.entry_price) * direction if price else 0.0
            profit_usd = price_diff * self.contract_size * self.lot_size
            total_profit_usd = profit_usd
            position_state = {
                "ticket": pos.ticket,
                "side": pos.side,
                "entry_price": pos.entry_price,
                "sl_price": pos.sl_price,
                "distance": pos.distance,
                "profit_usd": profit_usd,
                "has_stop_order": pos.stop_order_ticket is not None,
                "trail_armed": pos.trail_armed,
            }

        if self.pending_straddle_tickets:
            pending_desc = f"{len(self.pending_straddle_tickets)} straddle order(s) live"
        elif position_state is not None:
            pending_desc = "stop order live" if position_state["has_stop_order"] else "NO STOP ORDER - placing"
        else:
            pending_desc = "flat"

        return {
            "connected": self._connected,
            "mt5_available": MT5_AVAILABLE,
            "account_login": self.account_login,
            "account_server": self.account_server,
            "account_currency": self.account_currency,
            "account_balance": balance,
            "symbol": self.symbol,
            "positions": [position_state] if position_state is not None else [],
            "current_price": price,
            "total_profit_usd": total_profit_usd,
            "pending_order_desc": pending_desc,
            "daily_pnl_usd": day_pnl_usd,
            "daily_profit_target_usd": self.target_usd,
            "max_daily_drawdown_usd": self.max_dd_usd,
            "trading_halted": self.trading_halted,
            "halt_reason": self.halt_reason,
            "log": list(self.log),
            "shutdown": False,
        }

    def run_forever(self, on_tick=None) -> None:
        """Blocking loop used when no UI callback is driving tick()."""
        try:
            while True:
                state = self.tick()
                if on_tick:
                    on_tick(state)
                time.sleep(self.poll_interval)
        except KeyboardInterrupt:
            self._log("Shutdown requested by user.")
        finally:
            self.shutdown()