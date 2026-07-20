"""
Trade history - pure pass-through of MT5's own real closed-trade record.

Deliberately does NOT compute anything: no P&L, no ROI, no aggregation,
no field renaming beyond dict-to-list. base_agent.py's
fetch_historic_trades() already returns MT5's own HistoryDealGet* values
completely as-is (see that function's docstring for the exact fields) -
this file's only job is turning that dict into a JSON-friendly list for
the API response. If the frontend wants ROI, win rate, or any other
derived metric, it computes that itself from this raw list - not built
here, on purpose, per an explicit decision to keep the backend a faithful
reader of MT5's data, not a calculator.
"""

from __future__ import annotations

from typing import Any

from .base_agent import BaseAgent


def get_account_trade_history(agent: BaseAgent, lookback_days: int = 30) -> list[dict[str, Any]]:
    """agent is whichever TerminalAgent/FollowerAgent is registered for the
    requested account_id (resolved by api_server.py before calling this).
    Returns MT5's own deal records, unmodified, as a list. Each entry is a
    single MT5 deal (MT5 records TWO deals per closed position - one 'in'
    at open, one 'out' at close - both included, with `entry` indicating
    which; the frontend can pair them by `comment`/`deal_time` if it needs
    to, that pairing logic isn't done here either)."""
    raw = agent.fetch_historic_trades(lookback_days=lookback_days)
    return [{"deal_ticket": ticket, **deal} for ticket, deal in raw.items()]