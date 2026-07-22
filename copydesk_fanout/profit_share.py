"""
Profit-share billing - "if it's profit, the split is taken; if it's a
loss, none is taken", applied per closed trade, exactly as specified.

Deliberately a POLLER, not a hook on fanout_core.py's live close event.
_fan_out_close fires the instant a position closes, but has no realized
P&L at that moment - P&L only exists once MT5 records the deal, which is
the same historic-trades data trade_history.py already exposes via
fetch_historic_trades(). So this reads through that exact same path
(get_account_trade_history), on a schedule (see main.py), rather than
trying to correlate P&L into the live open/close event stream.

Idempotency: billed_deals is keyed on (follower_account_id, deal_ticket) -
MT5 deal tickets are already globally unique, so this table doubles as
both the "don't double-charge" guard AND the natural place a retry/restart
resumes from (just re-run the poll; anything already in billed_deals is
skipped).

The rate applied is always the LOCKED-IN snapshot for the follower's
CURRENT roster slot (master_rate.get_copy_rate_for_slot), never the
master's live rate - matches the "later rate changes only affect new
copiers" rule from master_rate.py.
"""

from __future__ import annotations

import logging
from typing import Any

from . import roster, trade_history, wallet
from .master_rate import get_copy_rate_for_slot

logger = logging.getLogger("profit_share")


def _already_billed(follower_account_id: str, deal_ticket: str, supabase_client: Any) -> bool:
    response = (
        supabase_client.table("billed_deals")
        .select("deal_ticket")
        .eq("follower_account_id", follower_account_id)
        .eq("deal_ticket", deal_ticket)
        .execute()
    )
    return bool(response.data)


def process_follower_deals(
    *, follower_account_id: str, billing_period_id: str, agent: Any, supabase_client: Any,
) -> list[dict]:
    """Bills every newly-closed, profitable deal for one follower against
    their current roster slot's locked-in rate. Returns the list of
    charges applied this run (empty if nothing new, or nothing profitable,
    or no current slot at all - e.g. they haven't switched to any master
    yet this period)."""
    current_slot = roster.get_current_slot(billing_period_id, follower_account_id, supabase_client)
    if current_slot is None:
        return []

    rate = get_copy_rate_for_slot(current_slot["id"], supabase_client)
    if rate is None:
        logger.warning("No rate snapshot for roster slot %s (follower %s) - skipping billing this run", current_slot["id"], follower_account_id)
        return []

    deals = trade_history.get_account_trade_history(agent)
    charges = []

    for deal in deals:
        ticket = str(deal["deal_ticket"])
        pnl = float(deal.get("pnl", 0) or 0)
        entry = deal.get("entry")

        # Only closing deals ("out") carry realized P&L; "in" deals are
        # always pnl=0 by construction (see trade_history.py's contract).
        if entry != "out" or pnl <= 0:
            continue
        if _already_billed(follower_account_id, ticket, supabase_client):
            continue

        total_cut = pnl * float(rate["rate_percent"]) / 100
        platform_amount = pnl * float(rate["platform_cut_percent"]) / 100
        master_amount = total_cut - platform_amount
        master_account_id = current_slot["master_account_id"]

        wallet.debit(
            follower_account_id, platform_amount, "profit_share_platform", supabase_client,
            related_master_account_id=master_account_id, related_deal_ticket=ticket,
        )
        wallet.debit(
            follower_account_id, master_amount, "profit_share_master", supabase_client,
            related_master_account_id=master_account_id, related_deal_ticket=ticket,
        )
        supabase_client.table("billed_deals").insert(
            {
                "follower_account_id": follower_account_id,
                "deal_ticket": ticket,
                "master_account_id": master_account_id,
                "pnl": pnl,
                "platform_amount": platform_amount,
                "master_amount": master_amount,
            }
        ).execute()

        charges.append({"deal_ticket": ticket, "pnl": pnl, "platform_amount": platform_amount, "master_amount": master_amount})
        logger.info(
            "Billed follower %s deal %s: pnl=%.2f, rate=%.2f%% -> platform %.2f, master %.2f",
            follower_account_id, ticket, pnl, rate["rate_percent"], platform_amount, master_amount,
        )

    return charges


def run_poll_cycle(*, fanout: Any, account_user_map: dict[str, str], supabase_client: Any) -> int:
    """One pass over every follower with an active/grace billing period.
    Called on a schedule from main.py, same pattern as the existing
    stale-pending sweep. Returns total charges applied this pass."""
    from . import billing  # local import - avoids a module-level cycle with billing.py

    total = 0
    for account_id, agent in fanout.follower_agents.items():
        period = billing.get_active_period(account_id, supabase_client)
        if period is None:
            continue
        charges = process_follower_deals(
            follower_account_id=account_id, billing_period_id=period["id"], agent=agent, supabase_client=supabase_client,
        )
        total += len(charges)
    return total


def get_master_earnings(master_account_id: str, supabase_client: Any, limit: int = 100) -> dict:
    """Aggregates every profit_share_master transaction ever recorded
    against this master, across ALL their followers' wallets - these
    transactions live on the FOLLOWER's account_id (it's their wallet
    being debited), so this is the one place that has to query across
    accounts by related_master_account_id instead of a single account_id,
    unlike everything else in wallet.py."""
    response = (
        supabase_client.table("wallet_transactions")
        .select("account_id, amount, related_deal_ticket, created_at")
        .eq("type", "profit_share_master")
        .eq("related_master_account_id", master_account_id)
        .order("created_at", desc=True)
        .execute()
    )
    rows = response.data or []
    # amount is stored negative (a debit from the follower's wallet) -
    # the master's earned figure is the positive magnitude of that.
    total_earned = sum(-float(r["amount"]) for r in rows)
    return {
        "master_account_id": master_account_id,
        "total_earned": total_earned,
        "transaction_count": len(rows),
        "recent": rows[:limit],
    }