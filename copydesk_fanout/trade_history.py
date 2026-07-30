from __future__ import annotations

from typing import Any

from .base_agent import BaseAgent

# MT5's native entry value (int) -> the "in"/"out" contract api.ts documents.
# Unknown/future values pass through unchanged rather than being dropped,
# so we fail loud (frontend sees the raw value) instead of silently.
ENTRY_MAP = {
    0: "in",   # DEAL_ENTRY_IN
    1: "out",  # DEAL_ENTRY_OUT
}


def get_account_trade_history(agent: BaseAgent, lookback_days: int = 30) -> list[dict[str, Any]]:
    """agent is whichever TerminalAgent/FollowerAgent is registered for the
    requested account_id (resolved by api_server.py before calling this).
    Returns MT5's own deal records as a list, with `entry` normalized to
    the "in"/"out" contract the API already documents (see ENTRY_MAP
    above) - every other field is MT5's own value, unmodified. Each entry
    is a single MT5 deal (MT5 records TWO deals per closed position - one
    'in' at open, one 'out' at close - both included, with `entry`
    indicating which; the frontend can pair them by `comment`/
    `deal_time` if it needs to, that pairing logic isn't done here
    either)."""
    raw = agent.fetch_historic_trades(lookback_days=lookback_days)
    trades = []
    for ticket, deal in raw.items():
        deal = dict(deal)
        deal["entry"] = ENTRY_MAP.get(deal.get("entry"), deal.get("entry"))
        trades.append({"deal_ticket": ticket, **deal})
    print(" trades :", trades)
    return trades