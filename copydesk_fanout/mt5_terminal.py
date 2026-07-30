from __future__ import annotations

"""
Mt5Terminal - the sole source of truth for one account's MT5 terminal.

Replaces dwx_client / DWX_Server_MT5.mq5 completely. No files, no EA.
One MetaTrader5 IPC connection per account, held in a dedicated child
process (the "sidecar") because the MetaTrader5 package only supports one
live connection per OS process - see the module docstring on
_worker_main() for why that forces a process-per-account design even
though every other part of this system (provisioning, fanout_core,
sizing) is completely unaware of this and stays untouched.

Public surface deliberately mirrors what BaseAgent/TerminalAgent/
FollowerAgent already relied on from dwx_client, so nothing above this
module needed to change shape - only the name (self.dwx -> self.terminal)
and where the data actually comes from:

    .open_orders     -> {ticket_str: {symbol, type, lots, SL, TP,
                                       open_price, magic, comment}}
    .account_info    -> {balance, equity, margin, currency, leverage}
    .historic_trades -> {deal_ticket_str: {symbol, lots, type, entry,
                                            deal_time, deal_price, pnl,
                                            commission, swap, comment}}
    .is_connected    -> bool
    .start() / .stop()
    .open_order(...) / .close_order(...) / .modify_order(...)
    .get_historic_trades(lookback_days)

The pure functions below (order-type mapping, dict shaping, request
building, the order-cap guard) take no MetaTrader5-module dependency
where possible and are unit tested directly - that's the part of this
file that's actually verifiable without a Windows box and a live
terminal. _worker_main() and the multiprocessing plumbing around it are
not testable in this sandbox (no MetaTrader5 wheel for Linux) and are
kept as thin as possible for exactly that reason - all real logic lives
in the pure functions.
"""

import multiprocessing as mp
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

# MT5 request/trade constants. Hardcoded rather than read off the mt5
# module so the pure builder functions below can be unit tested without
# MetaTrader5 installed - these are stable, documented values on every
# platform build of the terminal, not something that varies per-broker.
TRADE_ACTION_DEAL = 1
TRADE_ACTION_SLTP = 6
ORDER_TYPE_BUY = 0
ORDER_TYPE_SELL = 1
POSITION_TYPE_BUY = 0
POSITION_TYPE_SELL = 1
ORDER_TIME_GTC = 0
ORDER_FILLING_IOC = 1
TRADE_RETCODE_DONE = 10009


# --------------------------------------------------------------------- #
# Pure helpers - no IPC, no mt5 module dependency, fully unit-testable
# --------------------------------------------------------------------- #

def position_type_to_str(position_type: int) -> str:
    """MT5's POSITION_TYPE_BUY/SELL (int) -> the 'buy'/'sell' string
    convention every other file in this app already expects (see
    follower_agent.py's execute_open docstring)."""
    return "sell" if position_type == POSITION_TYPE_SELL else "buy"


def order_type_str_to_mt5(order_type: str) -> int:
    """Inverse of the above, for placing new orders. Only 'buy'/'sell'
    are needed - this whole app never places pending orders (see
    fanout_core.py: master trades are always fills, never pendings)."""
    return ORDER_TYPE_SELL if order_type.lower() == "sell" else ORDER_TYPE_BUY


def position_to_order_dict(pos: Any) -> dict:
    """Converts one MetaTrader5 TradePosition object (as returned by
    positions_get()) into the exact dict shape dwx_client's open_orders
    used - so terminal_agent.py, fanout_core.py, live_state_publisher.py
    need zero changes to their comparison/read logic, only to the two
    lines that used to say `.dwx.open_orders` and now say
    `.terminal.open_orders`.
    """
    return {
        "symbol": pos.symbol,
        "type": position_type_to_str(pos.type),
        "lots": round(pos.volume, 2),
        "SL": pos.sl,
        "TP": pos.tp,
        "open_price": pos.price_open,
        "magic": pos.magic,
        "comment": pos.comment,
    }


def positions_to_open_orders(positions: Optional[list]) -> dict:
    """positions_get() returns None on a hard failure, () on 'no
    positions' - both are valid empty states, not errors, so both
    normalize to {}."""
    if not positions:
        return {}
    return {str(pos.ticket): position_to_order_dict(pos) for pos in positions}


def account_info_to_dict(info: Any) -> dict:
    if info is None:
        return {}
    return {
        "balance": info.balance,
        "equity": info.equity,
        "margin": info.margin,
        "currency": info.currency,
        "leverage": info.leverage,
    }


DEAL_ENTRY_IN = 0
DEAL_ENTRY_OUT = 1


def deal_to_historic_trade_dict(deal: Any) -> dict:
    return {
        "symbol": deal.symbol,
        "lots": round(deal.volume, 2),
        "type": position_type_to_str(deal.type),
        "entry": deal.entry,
        "deal_time": datetime.fromtimestamp(deal.time, tz=timezone.utc).isoformat(),
        "deal_price": deal.price,
        "pnl": deal.profit,
        "commission": deal.commission,
        "swap": deal.swap,
        "comment": deal.comment,
    }


def deals_to_historic_trades(deals: Optional[list]) -> dict:
    if not deals:
        return {}
    return {str(d.ticket): deal_to_historic_trade_dict(d) for d in deals}


class OrderCapExceeded(Exception):
    """Raised by check_order_caps() when an open would violate the
    per-instance limits DWX_Server_MT5.mq5 used to hard-reject before
    ever calling OrderSend (see its OPEN_ORDER handler:
    numOrders >= MaximumOrders / lots > MaximumLotSize). Rebuilt here in
    Python since the EA that used to enforce this is gone."""


def check_order_caps(
    *,
    current_open_count: int,
    lots: float,
    max_orders: int,
    max_lot_size: float,
) -> None:
    """Raises OrderCapExceeded if the requested order would violate
    either cap. Called from FollowerAgent.execute_open() BEFORE building
    or sending the request - same reject-before-send behavior the EA had,
    just moved from MQL to Python. Pure function: no IO, so no mock
    needed to test it.
    """
    if current_open_count >= max_orders:
        raise OrderCapExceeded(
            f"Number of open orders ({current_open_count}) >= MaximumOrders ({max_orders})."
        )
    if lots > max_lot_size:
        raise OrderCapExceeded(
            f"Lot size ({lots:.2f}) larger than MaximumLotSize ({max_lot_size:.2f})."
        )


def build_open_request(
    *,
    symbol: str,
    order_type: str,
    lots: float,
    price: float,
    stop_loss: float,
    take_profit: float,
    magic: int,
    comment: str,
    deviation: int = 20,
) -> dict:
    """Builds the TRADE_ACTION_DEAL request dict for order_send(). price=0
    means "use current market price" in dwx_client's convention - the
    caller (FollowerAgent) is responsible for resolving that to a real
    tick price before calling this, since the actual current bid/ask can
    only come from the live mt5 module, not this pure function.
    """
    return {
        "action": TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": round(lots, 2),
        "type": order_type_str_to_mt5(order_type),
        "price": price,
        "sl": stop_loss,
        "tp": take_profit,
        "deviation": deviation,
        "magic": magic,
        "comment": comment[:31],  # MT5 comment length limit
        "type_time": ORDER_TIME_GTC,
        "type_filling": ORDER_FILLING_IOC,
    }


def build_close_request(
    *,
    ticket: int,
    symbol: str,
    position_type: int,
    volume: float,
    close_price: float,
    magic: int = 0,
    deviation: int = 20,
) -> dict:
    """Closing a position in the native API means sending the OPPOSING
    order type against the same position ticket - there is no separate
    'close' action like dwx_client's CLOSE_ORDER command had."""
    opposite_type = ORDER_TYPE_SELL if position_type == POSITION_TYPE_BUY else ORDER_TYPE_BUY
    return {
        "action": TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": round(volume, 2),
        "type": opposite_type,
        "position": ticket,
        "price": close_price,
        "deviation": deviation,
        "magic": magic,
        "type_time": ORDER_TIME_GTC,
        "type_filling": ORDER_FILLING_IOC,
    }


def build_modify_request(
    *,
    ticket: int,
    symbol: str,
    stop_loss: float,
    take_profit: float,
) -> dict:
    return {
        "action": TRADE_ACTION_SLTP,
        "position": ticket,
        "symbol": symbol,
        "sl": stop_loss,
        "tp": take_profit,
    }


# --------------------------------------------------------------------- #
# Worker process - the actual sidecar. Not unit-testable in this
# sandbox (no MetaTrader5 wheel on Linux); kept deliberately thin -
# everything it does is call one of the pure functions above.
# --------------------------------------------------------------------- #

def _worker_main(
    terminal_path: str,
    login: int,
    password: str,
    server: str,
    state: "mp.managers.DictProxy",
    command_q: "mp.Queue",
    result_q: "mp.Queue",
    poll_seconds: float = 0.05,
) -> None:
    """Runs in its own OS process. Holds the ONE live MetaTrader5
    connection this account is allowed to have (see module docstring -
    MetaTrader5 does not support multiple simultaneous connections in one
    process, confirmed against the MQL5 forum and official docs: the next
    initialize() call tears down the previous connection). Every other
    account gets its own instance of this same function in its own
    process, attached to its own already-running terminal at
    instance_dir/terminal64.exe (see provisioning.py's _launch_terminal).
    """
    import MetaTrader5 as mt5  # imported here, not module-level - only the worker process needs it

    ok = mt5.initialize(path=terminal_path, login=login, password=password, server=server, portable=True)
    state["connected"] = bool(ok)
    if not ok:
        state["last_error"] = str(mt5.last_error())

    while True:
        # Drain any pending write commands first - these are latency
        # sensitive (a real order), reads are just periodic refreshes.
        try:
            while True:
                cmd_id, cmd_type, payload = command_q.get_nowait()
                result = _handle_command(mt5, cmd_type, payload)
                result_q.put((cmd_id, result))
        except Exception:
            pass  # queue.Empty or a malformed command - either way, move on to the read poll

        if mt5.terminal_info() is None:
            # Connection dropped - try to recover rather than spinning on a dead handle.
            state["connected"] = False
            ok = mt5.initialize(path=terminal_path, login=login, password=password, server=server, portable=True)
            state["connected"] = bool(ok)
            time.sleep(1)
            continue

        state["connected"] = True
        state["open_orders"] = positions_to_open_orders(mt5.positions_get())
        state["account_info"] = account_info_to_dict(mt5.account_info())

        time.sleep(poll_seconds)


def _handle_command(mt5_module, cmd_type: str, payload: dict) -> dict:
    if cmd_type == "open_order":
        request = build_open_request(**payload)
        result = mt5_module.order_send(request)
        return _order_result_to_dict(result)
    if cmd_type == "close_order":
        request = build_close_request(**payload)
        result = mt5_module.order_send(request)
        return _order_result_to_dict(result)
    if cmd_type == "modify_order":
        request = build_modify_request(**payload)
        result = mt5_module.order_send(request)
        return _order_result_to_dict(result)
    if cmd_type == "get_historic_trades":
        from datetime import datetime, timedelta, timezone
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=payload["lookback_days"])
        deals = mt5_module.history_deals_get(start, end)
        return {"historic_trades": deals_to_historic_trades(deals)}
    return {"error": f"unknown command type: {cmd_type}"}


def _order_result_to_dict(result: Any) -> dict:
    if result is None:
        return {"success": False, "retcode": None, "comment": "order_send returned None"}
    return {
        "success": result.retcode == TRADE_RETCODE_DONE,
        "retcode": result.retcode,
        "comment": result.comment,
        "order": getattr(result, "order", None),
        "deal": getattr(result, "deal", None),
        "price": getattr(result, "price", None),
        "volume": getattr(result, "volume", None),
    }


# --------------------------------------------------------------------- #
# Parent-side handle - this is what BaseAgent holds as `self.terminal`
# --------------------------------------------------------------------- #

@dataclass
class Mt5Terminal:
    account_id: str
    terminal_path: str
    login: int
    password: str
    server: str
    max_orders: int = 200
    max_lot_size: float = 100.0
    command_timeout: float = 10.0

    _manager: Any = field(default=None, init=False, repr=False)
    _state: Any = field(default=None, init=False, repr=False)
    _command_q: Any = field(default=None, init=False, repr=False)
    _result_q: Any = field(default=None, init=False, repr=False)
    _process: Any = field(default=None, init=False, repr=False)
    _historic_trades: dict = field(default_factory=dict, init=False, repr=False)

    def start(self) -> None:
        self._manager = mp.Manager()
        self._state = self._manager.dict()
        self._state["connected"] = False
        self._state["open_orders"] = {}
        self._state["account_info"] = {}
        self._command_q = mp.Queue()
        self._result_q = mp.Queue()
        self._process = mp.Process(
            target=_worker_main,
            args=(self.terminal_path, self.login, self.password, self.server,
                  self._state, self._command_q, self._result_q),
            daemon=True,
        )
        self._process.start()

    def stop(self) -> None:
        if self._process is not None:
            self._process.terminate()
        if self._manager is not None:
            self._manager.shutdown()

    @property
    def is_connected(self) -> bool:
        return bool(self._state and self._state.get("connected", False))

    @property
    def open_orders(self) -> dict:
        return dict(self._state.get("open_orders", {})) if self._state else {}

    @property
    def account_info(self) -> dict:
        return dict(self._state.get("account_info", {})) if self._state else {}

    @property
    def balance(self) -> float | None:
        return self.account_info.get("balance")

    @property
    def historic_trades(self) -> dict:
        return self._historic_trades

    def _send_command(self, cmd_type: str, payload: dict) -> dict:
        cmd_id = str(uuid.uuid4())
        self._command_q.put((cmd_id, cmd_type, payload))
        deadline = time.monotonic() + self.command_timeout
        while time.monotonic() < deadline:
            try:
                result_id, result = self._result_q.get(timeout=0.1)
            except Exception:
                continue
            if result_id == cmd_id:
                return result
            # not ours (another concurrent command) - put it back for its owner
            self._result_q.put((result_id, result))
        return {"success": False, "retcode": None, "comment": "command timed out"}

    def open_order(self, *, symbol: str, order_type: str, lots: float, price: float = 0,
                   stop_loss: float = 0, take_profit: float = 0, magic: int = 0, comment: str = "") -> dict:
        check_order_caps(
            current_open_count=len(self.open_orders),
            lots=lots,
            max_orders=self.max_orders,
            max_lot_size=self.max_lot_size,
        )
        return self._send_command("open_order", dict(
            symbol=symbol, order_type=order_type, lots=lots, price=price,
            stop_loss=stop_loss, take_profit=take_profit, magic=magic, comment=comment,
        ))

    def close_order(self, ticket: int, lots: float = 0) -> dict:
        order = self.open_orders.get(str(ticket))
        if order is None:
            return {"success": False, "retcode": None, "comment": f"ticket {ticket} not in open_orders"}
        position_type = POSITION_TYPE_SELL if order["type"] == "sell" else POSITION_TYPE_BUY
        volume = lots if lots else order["lots"]
        return self._send_command("close_order", dict(
            ticket=ticket, symbol=order["symbol"], position_type=position_type,
            volume=volume, close_price=0, magic=order.get("magic", 0),
        ))

    def modify_order(self, ticket: int, price: float = 0, stop_loss: float = 0,
                      take_profit: float = 0, expiration: int = 0) -> dict:
        order = self.open_orders.get(str(ticket))
        symbol = order["symbol"] if order else ""
        return self._send_command("modify_order", dict(
            ticket=ticket, symbol=symbol, stop_loss=stop_loss, take_profit=take_profit,
        ))

    def get_historic_trades(self, lookback_days: int = 30) -> dict:
        result = self._send_command("get_historic_trades", dict(lookback_days=lookback_days))
        self._historic_trades = result.get("historic_trades", {})
        return self._historic_trades