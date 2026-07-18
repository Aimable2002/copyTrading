from __future__ import annotations

from .terminal_agent import TerminalAgent, TradeEventCallback


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
    """

    COMMENT_PREFIX = "cp:"
    # MT5 order comments have a real length limit - keep the tag short.
    _MAX_COMMENT_LEN = 31

    def __init__(self, account_id: str, metatrader_dir_path: str, on_trade_event: TradeEventCallback, verbose: bool = True):
        super().__init__(account_id, metatrader_dir_path, on_trade_event, verbose=verbose)

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
    ) -> None:
        """order_type must be 'buy' or 'sell' (lowercase) - matches dwx_client's convention exactly."""
        self.dwx.open_order(
            symbol=symbol,
            order_type=order_type,
            lots=round(lots, 2),
            price=price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            magic=magic,
            comment=self._copy_comment(master_ticket),
        )

    def execute_close(self, *, follower_ticket: str, lots: float = 0) -> None:
        self.dwx.close_order(follower_ticket, lots=round(lots, 2) if lots else 0)

    def execute_modify(
        self,
        *,
        follower_ticket: str,
        price: float = 0,
        stop_loss: float = 0,
        take_profit: float = 0,
    ) -> None:
        self.dwx.modify_order(follower_ticket, price=price, stop_loss=stop_loss, take_profit=take_profit)
