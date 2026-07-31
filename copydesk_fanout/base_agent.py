from __future__ import annotations

from .mt5_terminal import Mt5Terminal


class BaseAgent:
    """
    Base class for MasterAgent/FollowerAgent. Holds the one native MT5
    connection this account is allowed to have (self.terminal, an
    Mt5Terminal - see mt5_terminal.py's module docstring for why that's
    a dedicated child process rather than an in-process call). Replaces
    the old dwx_client/DWX_Server_MT5.mq5 file bridge completely - no
    files, no EA, this account's terminal only needs to be logged in
    with AutoTrading on.
    """

    def __init__(self, account_id: str, terminal_path: str, login: int, password: str, server: str,
                 max_orders: int = 200, max_lot_size: float = 100.0, verbose: bool = True):
        self.account_id = account_id
        self.terminal_path = terminal_path
        self.verbose = verbose
        self.terminal = Mt5Terminal(
            account_id=account_id,
            terminal_path=terminal_path,
            login=login,
            password=password,
            server=server,
            max_orders=max_orders,
            max_lot_size=max_lot_size,
        )

    def start(self) -> None:
        self.terminal.start()

    def stop(self) -> None:
        self.terminal.stop()

    @property
    def is_connected(self) -> bool:
        return self.terminal.is_connected

    @property
    def balance(self) -> float | None:
        return self.terminal.balance

    def fetch_historic_trades(self, lookback_days: int = 30, timeout: float = 10.0) -> dict:
        return self.terminal.get_historic_trades(lookback_days)