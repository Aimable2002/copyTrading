from __future__ import annotations

from .mt5_terminal import Mt5Terminal


class BaseAgent:
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