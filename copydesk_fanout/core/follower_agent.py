from __future__ import annotations

from .terminal_agent import TerminalAgent, TradeEventCallback

class FollowerAgent(TerminalAgent):
    COMMENT_PREFIX = "cp:"
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
        stop_loss: float = 0,
        take_profit: float = 0,
        magic: int = 0,
    ) -> dict:
        return self.terminal.open_order(
            symbol=symbol,
            order_type=order_type,
            lots=round(lots, 2),
            price=price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            magic=magic,
            comment=self._copy_comment(master_ticket),
        )

    def execute_open_pending(
        self,
        *,
        master_ticket: str,
        symbol: str,
        order_type: str,
        lots: float,
        price: float,
        stop_loss: float = 0,
        take_profit: float = 0,
        magic: int = 0,
        expiration: int = 0,
    ) -> dict:
        return self.terminal.open_pending_order(
            symbol=symbol,
            order_type=order_type,
            lots=round(lots, 2),
            price=price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            magic=magic,
            comment=self._copy_comment(master_ticket),
            expiration=expiration,
        )

    def execute_cancel_pending(self, *, follower_ticket: str) -> dict:
        return self.terminal.cancel_pending_order(int(follower_ticket))

    def execute_modify_pending(
        self, *, follower_ticket: str, price: float, stop_loss: float = 0,
        take_profit: float = 0, expiration: int = 0,
    ) -> dict:
        return self.terminal.modify_pending_order(
            int(follower_ticket), price=price, stop_loss=stop_loss,
            take_profit=take_profit, expiration=expiration,
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