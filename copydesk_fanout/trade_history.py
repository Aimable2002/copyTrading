"""
Trade history - pure pass-through of MT5's own real closed-trade record,
with ONE deliberate exception: the `entry` field.

Everything else here is untouched: no P&L, no ROI, no aggregation, no
renaming. base_agent.py's fetch_historic_trades() already returns MT5's
own HistoryDealGet* values completely as-is (see that function's
docstring for the exact fields) - this file's only job is turning that
dict into a JSON-friendly list for the API response. If the frontend
wants ROI, win rate, or any other derived metric, it computes that
itself from this raw list - not built here, on purpose, per an explicit
decision to keep the backend a faithful reader of MT5's data, not a
calculator.

The one exception: DWX_Server_MT5.mq5's HistoryDealEntryTypeToString()
sends `entry` as the literal strings "entry_in"/"entry_out" (confirmed
against the EA's own source and against test_historic_trades.py's raw
output, which talks to the EA directly). The API's documented contract
(api.ts's Deal type: entry: "in" | "out" | ...) has always been "in"/
"out" - the frontend's pairDeals()/countClosed()/winRate() all filter on
those exact short values. That contract was never being honored, so
every deal was silently landing in the "unpaired" bucket and every
derived stat computed to 0 regardless of how many real trades existed.
This is a contract-normalization, not a calculation, so it stays here:
ENTRY_MAP below is the single source of truth for it.
"""

from __future__ import annotations

from typing import Any

from .base_agent import BaseAgent

# MT5/DWX's raw entry value -> the "in"/"out" contract api.ts documents.
# Unknown/future values pass through unchanged rather than being dropped,
# so we fail loud (frontend sees the raw value) instead of silently.
ENTRY_MAP = {
    "entry_in": "in",
    "entry_out": "out",
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
    return trades