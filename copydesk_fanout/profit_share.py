from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from . import roster, trade_history, wallet
from .master_rate import get_copy_rate_for_slot
from .supabase_client import execute_with_retry

logger = logging.getLogger("profit_share")


def _already_billed(follower_account_id: str, deal_ticket: str, supabase_client: Any) -> bool:
    response = execute_with_retry(
        lambda: (
            supabase_client.table("billed_deals")
            .select("deal_ticket")
            .eq("follower_account_id", follower_account_id)
            .eq("deal_ticket", deal_ticket)
            .execute()
        )
    )
    return bool(response.data)


def process_follower_deals(
    *, follower_account_id: str, billing_period_id: str, agent: Any, supabase_client: Any, pair_store: Any,
) -> list[dict]:
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

        if entry != "out" or pnl <= 0:
            continue
        if _already_billed(follower_account_id, ticket, supabase_client):
            continue
        if not pair_store.was_follower_ticket_copied(follower_account_id, ticket):
            logger.info(
                "Follower %s deal %s (pnl=%.2f) was never a confirmed copy - not billing it",
                follower_account_id, ticket, pnl,
            )
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
        execute_with_retry(
            lambda: supabase_client.table("billed_deals").insert(
                {
                    "follower_account_id": follower_account_id,
                    "deal_ticket": ticket,
                    "master_account_id": master_account_id,
                    "pnl": pnl,
                    "platform_amount": platform_amount,
                    "master_amount": master_amount,
                }
            ).execute()
        )

        charges.append({"deal_ticket": ticket, "pnl": pnl, "platform_amount": platform_amount, "master_amount": master_amount})
        logger.info(
            "Billed follower %s deal %s: pnl=%.2f, rate=%.2f%% -> platform %.2f, master %.2f",
            follower_account_id, ticket, pnl, rate["rate_percent"], platform_amount, master_amount,
        )

    return charges


def run_poll_cycle(*, fanout: Any, account_user_map: dict[str, str], supabase_client: Any) -> int:
    from . import billing 

    total = 0
    for account_id, agent in fanout.follower_agents.items():
        try:
            period = billing.get_active_period(account_id, supabase_client)
            if period is None:
                continue
            charges = process_follower_deals(
                follower_account_id=account_id, billing_period_id=period["id"], agent=agent,
                supabase_client=supabase_client, pair_store=fanout.pair_store,
            )
            total += len(charges)
        except Exception:
            logger.exception("Profit-share billing failed for follower %s this cycle - will retry next cycle", account_id)
    return total


def get_master_earnings(master_account_id: str, supabase_client: Any, limit: int = 100) -> dict:
    challenge_response = execute_with_retry(
        lambda: (
            supabase_client.table("wallet_transactions")
            .select("account_id, type, amount, related_deal_ticket, created_at")
            .eq("type", "challenge_reward")
            .eq("account_id", master_account_id)
            .order("created_at", desc=True)
            .execute()
        )
    )
    legacy_response = execute_with_retry(
        lambda: (
            supabase_client.table("wallet_transactions")
            .select("account_id, type, amount, related_deal_ticket, created_at")
            .eq("type", "profit_share_master")
            .eq("related_master_account_id", master_account_id)
            .order("created_at", desc=True)
            .execute()
        )
    )
    rows = list(challenge_response.data or []) + list(legacy_response.data or [])
    rows.sort(key=lambda r: r["created_at"], reverse=True)

    cutoff_iso = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()

    def _earned(row: dict) -> float:
        amount = float(row["amount"])
        return -amount if row["type"] == "profit_share_master" else amount

    total_earned = sum(_earned(r) for r in rows)
    total_earned_30d = sum(_earned(r) for r in rows if r["created_at"] >= cutoff_iso)

    return {
        "master_account_id": master_account_id,
        "total_earned": total_earned,
        "total_earned_30d": total_earned_30d,
        "transaction_count": len(rows),
        "recent": rows[:limit],
    }