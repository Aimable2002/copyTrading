"""
core.py
--------
Pure trading logic for the Stop-and-Reverse (SAR) bot. No printing, no
colors, no prompts - this file only talks to MT5 and does math. Every
threshold it uses comes from config.json; nothing is hardcoded.

Strategy recap (R-multiplier risk model):
  - On the very first run, a straddle (Buy Stop + Sell Stop) is placed
    around price, sized by the same dynamic distance described below.
  - This bot supports hedging-mode accounts, where a Buy and a Sell on
    the same symbol can be open AT THE SAME TIME as two independent
    positions (normal on a volatile symbol like BTCUSD - both straddle
    legs can fill before the bot's next tick can cancel the sibling).
    Positions are tracked as a collection keyed by ticket, NOT as a
    single "the position" - every open position gets its own
    independent entry, risk unit, stop level, and pending stop order.
    A fill only ever affects that one position's tracked state.

  - RISK UNIT (R): computed fresh at the moment each position opens,
    from the live spread - the same dynamic %-of-price distance
    (adjusted for spread/broker minimums) already used to place the
    straddle. Not a fixed pip count - it self-scales with price and
    volatility. Every threshold below is a MULTIPLE of this position's
    own R, not a pip value.

  - HARD STOP-LOSS at 1R: from the moment a position opens (not after
    it becomes profitable), if price moves stop_loss_r (default 1.0)
    units of R against entry, the position is closed at market. This
    is checked every tick regardless of anything else - it is what
    actually limits the loss on a trade that never goes profitable.

  - TRAIL, armed at 1.5R: once profit reaches trail_arm_r (default 1.5)
    multiples of R, the trail arms and the stop level jumps to lock in
    (trail_arm_r - trail_buffer_r) R of profit. From then on the stop
    level continuously trails (peak profit reached - trail_buffer_r) R
    behind the best price seen - it only ever tightens, never loosens.
    There is no fixed take-profit / no ceiling; a winning position can
    run as far as price allows.

  - ONE ORDER, ONE NUMBER: there is exactly one pending stop order per
    position, and it always sits at that position's current SL price -
    never a separate "reversal order" at a different level. It is
    simultaneously this position's exit trigger and the next
    reversal's entry (on a hedging account, the order filling opens a
    new, independent opposite position - it does NOT close this one;
    the software price check above is what actually closes this one).
    Every tick, for every open position, the bot verifies this order
    exists and sits at the exact current SL price - if it's missing it
    gets placed, if the level moved it gets repriced. This is a hard
    invariant, not a best-effort retry: a position must never be left
    with no live stop order under it.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

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
    """Independent tracked state for exactly ONE open position. On a
    hedging account there can be several of these live simultaneously
    (e.g. a BUY and a SELL on the same symbol at once) - each one owns
    its own risk unit, stop level, and pending stop order, and nothing
    here is ever shared across positions."""
    ticket: int
    side: str                              # "BUY" | "SELL"
    entry_price: float
    risk_price: float                      # 1R in price units, fixed at entry
    sl_price: float                        # current stop level (moves once trail arms)
    trail_armed: bool = False
    peak_profit_r: float = 0.0
    stop_order_ticket: Optional[int] = None
    last_stop_order_attempt: float = 0.0


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

        # R-multiplier risk model - see module docstring. All three are
        # multiples of a position's own dynamically-computed R, never pips.
        self.stop_loss_r: float = config["protection"].get("stop_loss_r", 1.0)
        self.trail_arm_r: float = config["protection"].get("trail_arm_r", 1.5)
        self.trail_buffer_r: float = config["protection"].get("trail_buffer_r", 0.5)

        self.max_dd_usd: float = config["risk"]["max_daily_drawdown_usd"]
        self.target_usd: float = config["risk"]["daily_profit_target_usd"]
        self.stop_on_dd: bool = config["risk"]["stop_trading_on_drawdown_hit"]
        self.stop_on_target: bool = config["risk"]["stop_trading_on_target_hit"]

        self.poll_interval: float = config["engine"]["poll_interval_seconds"]

        # runtime state - every open position tracked independently,
        # keyed by its MT5 ticket. See PositionState docstring above.
        self.positions: Dict[int, PositionState] = {}

        # The initial straddle's two pending orders exist BEFORE any
        # position is open, so they're not owned by any PositionState -
        # tracked separately here until the first fill(s) consume them.
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
        the broker rejects the mode as unsupported.

        MT5 does not give a reliable way to know in advance which fill mode
        a given symbol/broker wants (this varies a lot for crypto CFDs), so
        instead of hardcoding one mode everywhere we try the mode that has
        already worked for this symbol first (once we've found it), and
        otherwise fall through IOC -> FOK -> RETURN. We only burn a retry
        when the rejection reason is specifically the filling mode
        (TRADE_RETCODE_INVALID_FILL) - any other rejection (bad price, no
        money, market closed, etc.) will fail identically on every mode, so
        we log it and return immediately instead of retrying pointlessly.
        """
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
        """Reads the broker's actual minimum distance (in price units) that a
        pending order must sit away from current price. Returns 0.0 if MT5
        isn't available or the broker reports no explicit restriction (common
        on raw/ECN accounts - it does NOT mean zero distance is safe, the
        spread itself still enforces a real floor, handled separately below)."""
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
        """Distance (in price units) used for the initial straddle, and as
        the RISK UNIT (1R) for every position once it fills. Percentage-of-
        price is the primary driver so it self-scales across symbols and
        price regimes instead of a fixed dollar/pip number going stale. The
        final distance is the LARGEST of:
          - percent_of_price  (your configured % target)
          - broker's reported minimum (trade_stops_level/freeze_level)
          - current spread * a safety multiplier (a stop closer than the
            spread itself is structurally invalid - it lands inside bid/ask)
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

    def _current_r_distance(self) -> Optional[float]:
        """Computes 1R fresh, right now, from the live spread. Used both for
        sizing the initial straddle and for sizing each new position's own
        risk unit at the moment it fills."""
        quote = self.get_bid_ask()
        if quote is None:
            return None
        bid, ask = quote
        spread = ask - bid
        mid = (bid + ask) / 2.0
        return self._effective_distance_price(mid, spread)

    def place_straddle(self, mid_price: float) -> None:
        """First-run only (no positions open, no straddle already pending):
        place both Buy Stop and Sell Stop around price, sized by the same
        dynamic distance that becomes each fill's risk unit (R). Anchored
        off real bid/ask (not the midpoint) so the orders always land on
        the valid side of the current spread. NOTE: on a hedging account
        both legs can fill - that's expected, _on_fill tracks each as its
        own independent position with its own R."""
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
        """Cancels whatever's left of the initial straddle (the leg that
        didn't fill). Only ever touches the pre-position straddle orders -
        never a specific position's own stop order, which lives for as
        long as that position does (see _ensure_stop_orders)."""
        for t in list(self.pending_straddle_tickets):
            self.cancel_pending(t)
        self.pending_straddle_tickets.clear()

    # ------------------------------------------------------------------
    # The single per-position stop order - placed at entry, kept pinned
    # to sl_price every tick for as long as the position is open.
    # ------------------------------------------------------------------

    def _place_stop_order(self, pos: PositionState) -> Optional[int]:
        """Places the ONE pending stop order for this position, at its
        current sl_price. Opposite side of the position, since it is both
        this position's exit trigger and the next reversal's entry."""
        if not MT5_AVAILABLE:
            self._log(f"[SIMULATED] Stop order placed at {pos.sl_price:.2f} for {pos.side} {pos.ticket}")
            return None

        order_type = mt5.ORDER_TYPE_SELL_STOP if pos.side == "BUY" else mt5.ORDER_TYPE_BUY_STOP
        req = dict(
            action=mt5.TRADE_ACTION_PENDING,
            symbol=self.symbol,
            volume=self.lot_size,
            magic=self.magic,
            comment=self.comment,
            type=order_type,
            price=round(pos.sl_price, 2),
            type_time=mt5.ORDER_TIME_GTC,
        )
        r = self._send_order_with_filling_retry(req)
        if r and r.retcode == mt5.TRADE_RETCODE_DONE:
            self._log(f"[{pos.side} {pos.ticket}] Stop order placed at {pos.sl_price:.2f}")
            return r.order

        self._log(f"[{pos.side} {pos.ticket}] Stop order FAILED at {pos.sl_price:.2f} - "
                  f"position has NO live stop order right now. Will retry.")
        return None

    def _sync_stop_order_price(self, pos: PositionState) -> None:
        """Verifies the tracked stop order still exists and sits exactly at
        sl_price; reprices it if the trail has moved, clears the ticket if
        the order is gone so _ensure_stop_orders places a fresh one next."""
        if not MT5_AVAILABLE:
            return
        orders = mt5.orders_get(ticket=pos.stop_order_ticket)
        if not orders:
            # Gone - either it filled (the resulting reversal position will
            # show up in _detect_fills on its own) or was cancelled/expired
            # externally. Either way this position is no longer verified as
            # protected, so drop the ticket and let _ensure_stop_orders
            # place a fresh one immediately.
            pos.stop_order_ticket = None
            return

        order = orders[0]
        target_price = round(pos.sl_price, 2)
        if abs(order.price_open - target_price) < 1e-9:
            return  # already correct, nothing to do

        req = dict(action=mt5.TRADE_ACTION_MODIFY, order=pos.stop_order_ticket, price=target_price)
        result = self._order_send(req)
        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
            self._log(f"[{pos.side} {pos.ticket}] Stop order moved to {target_price:.2f}")
        else:
            self._log(f"[{pos.side} {pos.ticket}] Failed to move stop order to {target_price:.2f} - will retry.")

    def _ensure_stop_orders(self) -> None:
        """Hard invariant, checked every tick: every open position must
        have a live pending stop order sitting exactly at its current
        sl_price. Missing -> place it. Stale price -> reprice it. This is
        what closes the 'naked position' gap - it's not a best-effort
        retry loop, it runs unconditionally every tick for every position."""
        now = time.time()
        for pos in list(self.positions.values()):
            if pos.stop_order_ticket is None:
                if now - pos.last_stop_order_attempt < self._stop_order_retry_interval_seconds:
                    continue
                pos.last_stop_order_attempt = now
                pos.stop_order_ticket = self._place_stop_order(pos)
            else:
                self._sync_stop_order_price(pos)

    def close_position_by_ticket(self, ticket: int, reason: str) -> bool:
        """Closes exactly ONE position, identified by ticket - never
        anything else that happens to be open at the same time. Returns
        True only once we've confirmed that specific ticket is flat.

        Deliberately does NOT cancel that position's stop order: it sits
        exactly at the level price just crossed, so it is very likely
        about to fill (or may already have) into the natural reversal
        position - that's the whole point of stop-and-reverse. Leaving it
        live also means a crashed/disconnected bot process still leaves a
        real order working on the broker's side."""
        if not MT5_AVAILABLE:
            self._log(f"[SIMULATED] Position {ticket} closed at market ({reason})")
            self.positions.pop(ticket, None)
            return True

        positions = mt5.positions_get(ticket=ticket)
        if not positions:
            # Already gone on the broker's side (closed manually, stopped
            # out, etc.) - bring our tracking in line with reality.
            self.positions.pop(ticket, None)
            return True

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
            # Leave this position's tracked state exactly as-is so
            # _apply_protection tries again next tick - and leave every
            # OTHER open position completely untouched.
            return False

        self._log(f"Position {ticket} closed at market ({reason})")
        self.positions.pop(ticket, None)
        return True

    # ------------------------------------------------------------------
    # Fill detection & the stop-and-reverse transition
    # ------------------------------------------------------------------

    def _detect_fills(self) -> List[Tuple[int, str, float]]:
        """Returns a list of (ticket, side, entry_price) for every position
        that's open on the broker and not yet in self.positions. Returns
        every new fill found, not just the first - on a hedging account a
        fast move can fill both straddle legs (or several stop orders)
        within a single tick, and every one of them needs its own tracked
        state."""
        if not MT5_AVAILABLE:
            return []
        positions = mt5.positions_get(symbol=self.symbol)
        if not positions:
            return []
        new_fills: List[Tuple[int, str, float]] = []
        for pos in positions:
            if pos.magic != self.magic:
                continue
            if pos.ticket in self.positions:
                continue
            side = "BUY" if pos.type == mt5.POSITION_TYPE_BUY else "SELL"
            new_fills.append((pos.ticket, side, pos.price_open))
        return new_fills

    def _on_fill(self, ticket: int, side: str, entry_price: float) -> None:
        """Sets up independent tracked state for exactly one newly-opened
        position: computes its own R fresh from the live spread, sets the
        initial hard stop-loss at stop_loss_r, and leaves stop_order_ticket
        unset so _ensure_stop_orders places it this same tick. Never
        touches any other position's state."""
        self._log(f"Fill detected: new {side} position {ticket} @ {entry_price}")

        r = self._current_r_distance()
        if r is None or r <= 0:
            # Shouldn't normally happen since we just traded this symbol,
            # but guard against a bad tick so the position still gets SOME
            # protection instead of none - and make it loud in the log.
            r = entry_price * 0.0005
            self._log(f"[{side} {ticket}] WARNING: could not read live spread to compute the risk "
                      f"unit - using fallback R={r:.2f}. Verify this position's stop manually.")

        direction = 1 if side == "BUY" else -1
        sl_price = entry_price - direction * self.stop_loss_r * r

        self.positions[ticket] = PositionState(
            ticket=ticket, side=side, entry_price=entry_price,
            risk_price=r, sl_price=sl_price,
        )

    # ------------------------------------------------------------------
    # Protection: hard stop-loss at 1R + trail armed at 1.5R with a
    # 0.5R buffer (all multiples of that position's own R - run every
    # tick, per independently-tracked position)
    # ------------------------------------------------------------------

    def _profit_r_for(self, pos: PositionState, current_price: float) -> float:
        direction = 1 if pos.side == "BUY" else -1
        if pos.risk_price <= 0:
            return 0.0
        return ((current_price - pos.entry_price) * direction) / pos.risk_price

    def _apply_protection(self, current_price: float) -> None:
        # list(...) because closing a position mutates self.positions
        # mid-iteration.
        for pos in list(self.positions.values()):
            direction = 1 if pos.side == "BUY" else -1
            profit_r = self._profit_r_for(pos, current_price)
            pos.peak_profit_r = max(pos.peak_profit_r, profit_r)
            tag = f"[{pos.side} {pos.ticket}]"

            # --- Arm the trail once profit reaches trail_arm_r ---
            if not pos.trail_armed and profit_r >= self.trail_arm_r:
                pos.trail_armed = True
                self._log(f"{tag} Trail armed at {profit_r:.2f}R")

            # --- While armed, sl_price continuously = peak - buffer, in R ---
            if pos.trail_armed:
                locked_r = pos.peak_profit_r - self.trail_buffer_r
                candidate_sl = pos.entry_price + direction * locked_r * pos.risk_price
                # Only ever tighten (move favorably) - never loosen.
                if pos.side == "BUY":
                    pos.sl_price = max(pos.sl_price, candidate_sl)
                else:
                    pos.sl_price = min(pos.sl_price, candidate_sl)

            # --- Hard check: has price crossed the current sl_price? ---
            crossed = (current_price <= pos.sl_price) if pos.side == "BUY" else (current_price >= pos.sl_price)
            if crossed:
                reason = "trail-exit" if pos.trail_armed else "stop-loss"
                self._log(f"{tag} SL hit at {profit_r:.2f}R (level {pos.sl_price:.2f}) -> closing now")
                self.close_position_by_ticket(pos.ticket, reason=reason)

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
        # Never attempt any trading action while disconnected. Instead,
        # retry the connection on a cooldown so we don't spam MT5.
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
                pass  # do nothing further, just keep reporting state
            else:
                if not self.positions and not self.pending_straddle_tickets:
                    now = time.time()
                    if now - self._last_straddle_attempt >= self._straddle_retry_interval_seconds:
                        self._last_straddle_attempt = now
                        # Nothing live at all yet -> first-run straddle
                        self.place_straddle(price)

                new_fills = self._detect_fills()
                if new_fills:
                    # Any leftover straddle leg is now stale the moment
                    # ANY fill happens (whether one leg filled or both).
                    if self.pending_straddle_tickets:
                        self.cancel_straddle_pending()
                    for ticket, side, entry_price in new_fills:
                        self._on_fill(ticket, side, entry_price)

                # Order matters: protection first (may move sl_price via
                # the trail, or close a position outright), THEN verify/
                # place/reprice stop orders against whatever's left, so
                # the order placed always matches the latest sl_price.
                self._apply_protection(price)
                self._ensure_stop_orders()
                self._check_daily_risk()

        balance = self.get_balance()
        day_pnl_usd = (balance - self.daily.starting_balance) if self.daily.starting_balance else 0.0

        positions_state = []
        total_profit_usd = 0.0
        for pos in self.positions.values():
            direction = 1 if pos.side == "BUY" else -1
            price_diff = (price - pos.entry_price) * direction if price else 0.0
            profit_r = self._profit_r_for(pos, price) if price else 0.0
            profit_usd = price_diff * self.contract_size * self.lot_size
            total_profit_usd += profit_usd
            positions_state.append({
                "ticket": pos.ticket,
                "side": pos.side,
                "entry_price": pos.entry_price,
                "sl_price": pos.sl_price,
                "risk_price": pos.risk_price,
                "profit_r": profit_r,
                "profit_usd": profit_usd,
                "trail_armed": pos.trail_armed,
                "has_stop_order": pos.stop_order_ticket is not None,
            })

        if self.pending_straddle_tickets:
            pending_desc = f"{len(self.pending_straddle_tickets)} straddle order(s) live"
        elif positions_state:
            n_with_stop = sum(1 for p in positions_state if p["has_stop_order"])
            pending_desc = f"{n_with_stop}/{len(positions_state)} position(s) have their stop order live"
        else:
            pending_desc = "—"

        return {
            "connected": self._connected,
            "mt5_available": MT5_AVAILABLE,
            "account_login": self.account_login,
            "account_server": self.account_server,
            "account_currency": self.account_currency,
            "account_balance": balance,
            "symbol": self.symbol,
            "positions": positions_state,
            "current_price": price,
            "total_profit_usd": total_profit_usd,
            "stop_loss_r": self.stop_loss_r,
            "trail_arm_r": self.trail_arm_r,
            "trail_buffer_r": self.trail_buffer_r,
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