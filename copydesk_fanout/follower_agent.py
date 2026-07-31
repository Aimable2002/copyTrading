from __future__ import annotations

from .terminal_agent import TerminalAgent, TradeEventCallback

# OrderCapExceeded (see mt5_terminal.py) is raised inside
# self.terminal.open_order() itself, before any order_send() call - not
# caught here on purpose. fanout_core.py's _fan_out_open() catches it
# per-follower so one capped follower doesn't stop the rest of the
# fan-out loop from dispatching.


class FollowerAgent(TerminalAgent):
    """
    A follower's terminal. Inherits open/close detection from TerminalAgent
    (used to confirm a copied order actually filled, by matching the
    'comment' tag we set when placing it) and adds the actual order
    placement methods on top.

    Every order this places is tagged via `comment` with the master ticket
    it's copying, e.g. "cp:<master_ticket>" - this is a secondary,
    human-inspectable correlation signal (visible directly in the MT5
    terminal). The authoritative correlation used by the fanout core is the
    in-memory OrderPairStore, not this comment - the comment is a debugging
    aid, not the mechanism itself.

    Execution goes straight to the native MetaTrader5 connection via
    self.terminal (Mt5Terminal) - order_send() under the hood, no EA, no
    command files. See mt5_terminal.py for the request-building and the
    order-cap guard (check_order_caps) that replaces what
    DWX_Server_MT5.mq5's OPEN_ORDER handler used to hard-reject
    (MaximumOrders / MaximumLotSize) before it was retired along with the
    rest of DWX.
    """

    COMMENT_PREFIX = "cp:"
    # MT5 order comments have a real length limit - keep the tag short.
    _MAX_COMMENT_LEN = 31

    def __init__(self, account_id: str, terminal_path: str, login: int, password: str, server: str,
                 on_trade_event: TradeEventCallback, max_orders: int = 200, max_lot_size: float = 100.0,
                 verbose: bool = True):
        super().__init__(account_id, terminal_path, login, password, server, on_trade_event,
                          max_orders=max_orders, max_lot_size=max_lot_size, verbose=verbose)

    def _copy_comment(self, master_ticket: str) -> str:
        comment = f"{self.COMMENT_PREFIX}{master_ticket}"
        return comment[: self._MAX_COMMENT_LEN]

    def execute_open(
        self,
        *,
        master_ticket: str,
        symbol: str,
        order_type: str,
        lots: float,
        price: float = 0,
        sl_distance: float = 0,
        tp_distance: float = 0,
        magic: int = 0,
    ) -> dict:
        return self.terminal.open_order(
            symbol=symbol,
            order_type=order_type,
            lots=round(lots, 2),
            price=price,
            sl_distance=sl_distance,
            tp_distance=tp_distance,
            magic=magic,
            comment=self._copy_comment(master_ticket),
        )

    def execute_close(self, *, follower_ticket: str, lots: float = 0) -> dict:
        return self.terminal.close_order(int(follower_ticket), lots=round(lots, 2) if lots else 0)

    def execute_modify(
        self,
        *,
        follower_ticket: str,
        price: float = 0,
        stop_loss: float = 0,
        take_profit: float = 0,
    ) -> dict:
        return self.terminal.modify_order(int(follower_ticket), price=price,
                                           stop_loss=stop_loss, take_profit=take_profit)