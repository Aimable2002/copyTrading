from __future__ import annotations

from typing import Any

from .base_agent import BaseAgent

ENTRY_MAP = {
    0: "in",  
    1: "out",  
}


def get_account_trade_history(agent: BaseAgent, lookback_days: int = 300) -> list[dict[str, Any]]:
    raw = agent.fetch_historic_trades(lookback_days=lookback_days)
    trades = []
    for ticket, deal in raw.items():
        deal = dict(deal)
        deal["entry"] = ENTRY_MAP.get(deal.get("entry"), deal.get("entry"))
        trades.append({"deal_ticket": ticket, **deal})
    return trades