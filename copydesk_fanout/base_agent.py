from __future__ import annotations

import sys
import time
from pathlib import Path

# Make the official repo's python/api package importable without copying or
# modifying any of its files. This folder (copydesk_fanout) sits alongside
# the official `python/` folder inside the same dwxconnect repo checkout:
#
#   dwxconnect/
#     python/api/dwx_client.py   <- official, untouched
#     copydesk_fanout/           <- everything we're adding
#
_DWXCONNECT_PYTHON_DIR = Path(__file__).resolve().parent.parent / "python"
if str(_DWXCONNECT_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_DWXCONNECT_PYTHON_DIR))

from api.dwx_client import dwx_client  # noqa: E402  (import after sys.path setup, official package)


class BaseAgent:
    """
    Base class for MasterAgent/FollowerAgent. Implements every callback
    dwx_client actually invokes on its event_handler (confirmed directly
    from dwx_client.py: on_order_event, on_message, on_tick, on_bar_data,
    on_historic_data, on_historic_trades) as safe no-ops, so subclasses only
    need to override the ones they actually care about.
    """

    def __init__(self, account_id: str, metatrader_dir_path: str, verbose: bool = True):
        self.account_id = account_id
        self.metatrader_dir_path = metatrader_dir_path
        self.dwx: dwx_client = dwx_client(self, metatrader_dir_path, verbose=verbose)

    def start(self) -> None:
        self.dwx.start()

    def stop(self) -> None:
        self.dwx.ACTIVE = False

    @property
    def is_connected(self) -> bool:
        """True once the EA has written at least one account_info payload."""
        return bool(self.dwx.account_info)

    @property
    def balance(self) -> float | None:
        return self.dwx.account_info.get("balance")

    # ------------------------------------------------------------------ #
    # dwx_client callback contract - safe no-ops by default
    # ------------------------------------------------------------------ #
    def on_order_event(self) -> None:
        pass

    def on_message(self, message: dict) -> None:
        if message.get("type") == "ERROR":
            print(f"[{self.account_id}] ERROR: {message.get('error_type')} | {message.get('description')}")

    def on_tick(self, symbol: str, bid: float, ask: float) -> None:
        pass

    def on_bar_data(self, symbol, time_frame, time, open_price, high, low, close_price, tick_volume) -> None:
        pass

    def on_historic_data(self, symbol, time_frame, data) -> None:
        pass

    def on_historic_trades(self) -> None:
        pass

    def fetch_historic_trades(self, lookback_days: int = 30, timeout: float = 10.0) -> dict:
        """Requests MT5's own real closed-trade record via DWX Connect's
        GET_HISTORIC_TRADES command (see dwx_client.py's get_historic_trades)
        and waits for the EA to write it back. Returns dwx_client's raw
        historic_trades dict completely as-is - every field in it
        (symbol, lots, type, entry, deal_time, deal_price, pnl, commission,
        swap, comment) is MT5's own broker-computed value, straight from
        HistoryDealGet* calls in the EA (see mql/DWX_Server_MT5.mq5's
        GET_HISTORIC_TRADES handler). No computation, no renaming, no
        filtering happens here - that's deliberate, this is a pass-through,
        not a transform.

        Blocking (polls in the calling thread) - callers in an async context
        (e.g. api_server.py's routes) should run this in an executor."""
        self.dwx.historic_trades = {}  
        self.dwx.get_historic_trades(lookback_days)

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.dwx.historic_trades:
                return dict(self.dwx.historic_trades)
            time.sleep(0.1)
        return {} 

