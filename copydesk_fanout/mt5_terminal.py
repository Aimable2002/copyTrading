from __future__ import annotations
import multiprocessing as mp
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional
from datetime import datetime, timedelta, timezone

from .sltp import apply_sl_tp_distance  

TRADE_ACTION_DEAL = 1
TRADE_ACTION_PENDING = 5
TRADE_ACTION_SLTP = 6
TRADE_ACTION_MODIFY = 7
TRADE_ACTION_REMOVE = 8
ORDER_TYPE_BUY = 0
ORDER_TYPE_SELL = 1
ORDER_TYPE_BUY_LIMIT = 2
ORDER_TYPE_SELL_LIMIT = 3
ORDER_TYPE_BUY_STOP = 4
ORDER_TYPE_SELL_STOP = 5
POSITION_TYPE_BUY = 0
POSITION_TYPE_SELL = 1
ORDER_TIME_GTC = 0
ORDER_TIME_SPECIFIED = 2
ORDER_FILLING_IOC = 1
ORDER_FILLING_RETURN = 2
TRADE_RETCODE_DONE = 10009

PENDING_ORDER_TYPES = frozenset({"buy_limit", "sell_limit", "buy_stop", "sell_stop"})

_ORDER_TYPE_STR_TO_MT5 = {
    "buy": ORDER_TYPE_BUY,
    "sell": ORDER_TYPE_SELL,
    "buy_limit": ORDER_TYPE_BUY_LIMIT,
    "sell_limit": ORDER_TYPE_SELL_LIMIT,
    "buy_stop": ORDER_TYPE_BUY_STOP,
    "sell_stop": ORDER_TYPE_SELL_STOP,
}
_MT5_PENDING_TYPE_TO_STR = {
    ORDER_TYPE_BUY: "buy",
    ORDER_TYPE_SELL: "sell",
    ORDER_TYPE_BUY_LIMIT: "buy_limit",
    ORDER_TYPE_SELL_LIMIT: "sell_limit",
    ORDER_TYPE_BUY_STOP: "buy_stop",
    ORDER_TYPE_SELL_STOP: "sell_stop",
}

_GENUINE_PENDING_TYPES = frozenset({
    ORDER_TYPE_BUY_LIMIT, ORDER_TYPE_SELL_LIMIT, ORDER_TYPE_BUY_STOP, ORDER_TYPE_SELL_STOP,
})


# --------------------------------------------------------------------- #
# Pure helpers - no IPC, no mt5 module dependency, fully unit-testable
# --------------------------------------------------------------------- #

def position_type_to_str(position_type: int) -> str:
    return "sell" if position_type == POSITION_TYPE_SELL else "buy"


def order_type_str_to_mt5(order_type: str) -> int:
    return _ORDER_TYPE_STR_TO_MT5.get(order_type.lower(), ORDER_TYPE_BUY)


def position_to_order_dict(pos: Any) -> dict:
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
    if not positions:
        return {}
    return {str(pos.ticket): position_to_order_dict(pos) for pos in positions}


def order_to_pending_order_dict(order: Any) -> dict:
    return {
        "symbol": order.symbol,
        "type": _MT5_PENDING_TYPE_TO_STR.get(order.type, "buy_limit"),
        "lots": round(order.volume_current, 2),
        "SL": order.sl,
        "TP": order.tp,
        "open_price": order.price_open,
        "magic": order.magic,
        "comment": order.comment,
    }


def orders_to_pending_orders(orders: Optional[list]) -> dict:
    if not orders:
        return {}
    return {
        str(o.ticket): order_to_pending_order_dict(o)
        for o in orders
        if o.type in _GENUINE_PENDING_TYPES
    }


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
    """this class i m not sure why it exist at all hhhhhhhaaaaaaaaaaaaa"""

def check_order_caps(
    *,
    current_open_count: int,
    lots: float,
    max_orders: int,
    max_lot_size: float,
) -> None:
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
        "comment": comment[:31],  
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


def build_pending_request(
    *,
    symbol: str,
    order_type: str,
    lots: float,
    price: float,
    stop_loss: float,
    take_profit: float,
    magic: int,
    comment: str,
    expiration: int = 0,
    deviation: int = 20,
) -> dict:
    return {
        "action": TRADE_ACTION_PENDING,
        "symbol": symbol,
        "volume": round(lots, 2),
        "type": order_type_str_to_mt5(order_type),
        "price": price,
        "sl": stop_loss,
        "tp": take_profit,
        "deviation": deviation,
        "magic": magic,
        "comment": comment[:31],  
        "type_time": ORDER_TIME_GTC if not expiration else ORDER_TIME_SPECIFIED,
        "expiration": expiration,
        "type_filling": ORDER_FILLING_RETURN,
    }


def build_cancel_pending_request(*, ticket: int) -> dict:
    return {"action": TRADE_ACTION_REMOVE, "order": ticket}


def build_modify_pending_request(
    *,
    ticket: int,
    symbol: str,
    price: float,
    stop_loss: float,
    take_profit: float,
    expiration: int = 0,
) -> dict:
    return {
        "action": TRADE_ACTION_MODIFY,
        "order": ticket,
        "symbol": symbol,
        "price": price,
        "sl": stop_loss,
        "tp": take_profit,
        "type_time": ORDER_TIME_GTC if not expiration else ORDER_TIME_SPECIFIED,
        "expiration": expiration,
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
    import MetaTrader5 as mt5  
    ok = mt5.initialize(path=terminal_path, login=int(login), password=password, server=server, portable=True)
    state["connected"] = bool(ok)
    if not ok:
        print(" Failed in not ok ...............", mt5.last_error())
        state["last_error"] = str(mt5.last_error())

    while True:
        try:
            while True:
                cmd_id, cmd_type, payload = command_q.get_nowait()
                result = _handle_command(mt5, cmd_type, payload)
                result_q.put((cmd_id, result))
        except Exception:
            pass  

        if mt5.terminal_info() is None:
            state["connected"] = False
            ok = mt5.initialize(path=terminal_path, login=login, password=password, server=server, portable=True)
            state["connected"] = bool(ok)
            time.sleep(1)
            continue

        state["connected"] = True
        state["open_orders"] = positions_to_open_orders(mt5.positions_get())
        state["pending_orders"] = orders_to_pending_orders(mt5.orders_get())
        state["account_info"] = account_info_to_dict(mt5.account_info())

        time.sleep(poll_seconds)


def _handle_command(mt5_module, cmd_type: str, payload: dict) -> dict:
    if cmd_type == "open_order":
        if not mt5_module.symbol_select(payload["symbol"], True):
            return {
                "success": False,
                "retcode": None,
                "comment": f"symbol_select failed for {payload['symbol']}",
            }

        if not payload.get("price"):
            tick = mt5_module.symbol_info_tick(payload["symbol"])
            if tick is None:
                return {
                    "success": False,
                    "retcode": None,
                    "comment": f"symbol_info_tick returned None for {payload['symbol']}",
                }
            is_buy = order_type_str_to_mt5(payload["order_type"]) == ORDER_TYPE_BUY
            payload = {**payload, "price": tick.ask if is_buy else tick.bid}

        request = build_open_request(**payload)
        result = mt5_module.order_send(request)
        if result is None:
            return {
                "success": False,
                "retcode": None,
                "comment": f"order_send returned None, last_error={mt5_module.last_error()}",
            }
        return _order_result_to_dict(result)

    if cmd_type == "close_order":
        if not mt5_module.symbol_select(payload["symbol"], True):
            return {
                "success": False,
                "retcode": None,
                "comment": f"symbol_select failed for {payload['symbol']}",
            }

        if not payload.get("close_price"):
            tick = mt5_module.symbol_info_tick(payload["symbol"])
            if tick is None:
                return {
                    "success": False,
                    "retcode": None,
                    "comment": f"symbol_info_tick returned None for {payload['symbol']}",
                }
            is_closing_sell = payload["position_type"] == POSITION_TYPE_BUY
            payload = {**payload, "close_price": tick.bid if is_closing_sell else tick.ask}

        request = build_close_request(**payload)
        result = mt5_module.order_send(request)
        if result is None:
            return {
                "success": False,
                "retcode": None,
                "comment": f"order_send returned None, last_error={mt5_module.last_error()}",
            }
        return _order_result_to_dict(result)
    if cmd_type == "modify_order":
        if not mt5_module.symbol_select(payload["symbol"], True):
            return {
                "success": False,
                "retcode": None,
                "comment": f"symbol_select failed for {payload['symbol']}",
            }
        request = build_modify_request(**payload)
        result = mt5_module.order_send(request)
        if result is None:
            return {
                "success": False,
                "retcode": None,
                "comment": f"order_send returned None, last_error={mt5_module.last_error()}",
            }
        return _order_result_to_dict(result)
    if cmd_type == "open_pending_order":
        if not mt5_module.symbol_select(payload["symbol"], True):
            return {
                "success": False,
                "retcode": None,
                "comment": f"symbol_select failed for {payload['symbol']}",
            }

        request = build_pending_request(**payload)
        result = mt5_module.order_send(request)
        if result is None:
            return {
                "success": False,
                "retcode": None,
                "comment": f"order_send returned None, last_error={mt5_module.last_error()}",
            }
        return _order_result_to_dict(result)

    if cmd_type == "cancel_pending_order":
        request = build_cancel_pending_request(ticket=payload["ticket"])
        result = mt5_module.order_send(request)
        if result is None:
            return {
                "success": False,
                "retcode": None,
                "comment": f"order_send returned None, last_error={mt5_module.last_error()}",
            }
        return _order_result_to_dict(result)

    if cmd_type == "modify_pending_order":
        if not mt5_module.symbol_select(payload["symbol"], True):
            return {
                "success": False,
                "retcode": None,
                "comment": f"symbol_select failed for {payload['symbol']}",
            }
        request = build_modify_pending_request(**payload)
        result = mt5_module.order_send(request)
        if result is None:
            return {
                "success": False,
                "retcode": None,
                "comment": f"order_send returned None, last_error={mt5_module.last_error()}",
            }
        return _order_result_to_dict(result)

    if cmd_type == "get_tick":
        if not mt5_module.symbol_select(payload["symbol"], True):
            return {"bid": None, "ask": None, "comment": f"symbol_select failed for {payload['symbol']}"}
        tick = mt5_module.symbol_info_tick(payload["symbol"])
        if tick is None:
            return {"bid": None, "ask": None, "comment": f"symbol_info_tick returned None for {payload['symbol']}"}
        return {"bid": tick.bid, "ask": tick.ask, "comment": ""}

    if cmd_type == "get_historic_trades":
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
        self._state["pending_orders"] = {}
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
    def pending_orders(self) -> dict:
        return dict(self._state.get("pending_orders", {})) if self._state else {}

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
            return {
                "success": True,
                "retcode": None,
                "comment": f"ticket {ticket} already not open - treating as closed",
            }
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

    def open_pending_order(self, *, symbol: str, order_type: str, lots: float, price: float,
                           stop_loss: float = 0, take_profit: float = 0, magic: int = 0,
                           comment: str = "", expiration: int = 0) -> dict:
        check_order_caps(
            current_open_count=len(self.open_orders) + len(self.pending_orders),
            lots=lots,
            max_orders=self.max_orders,
            max_lot_size=self.max_lot_size,
        )
        return self._send_command("open_pending_order", dict(
            symbol=symbol, order_type=order_type, lots=lots, price=price,
            stop_loss=stop_loss, take_profit=take_profit, magic=magic,
            comment=comment, expiration=expiration,
        ))

    def cancel_pending_order(self, ticket: int) -> dict:
        if str(ticket) not in self.pending_orders:
            return {
                "success": True,
                "retcode": None,
                "comment": f"pending order {ticket} already not resting - treating as cancelled",
            }
        return self._send_command("cancel_pending_order", dict(ticket=ticket))

    def modify_pending_order(self, ticket: int, price: float, stop_loss: float = 0,
                             take_profit: float = 0, expiration: int = 0) -> dict:
        order = self.pending_orders.get(str(ticket))
        symbol = order["symbol"] if order else ""
        return self._send_command("modify_pending_order", dict(
            ticket=ticket, symbol=symbol, price=price, stop_loss=stop_loss,
            take_profit=take_profit, expiration=expiration,
        ))

    def get_tick(self, symbol: str) -> dict:
        return self._send_command("get_tick", dict(symbol=symbol))

    def get_historic_trades(self, lookback_days: int = 30) -> dict:
        result = self._send_command("get_historic_trades", dict(lookback_days=lookback_days))
        self._historic_trades = result.get("historic_trades", {})
        return self._historic_trades