from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

# from . import billing, trade_history, wallet
from . import billing, wallet
from ..core import trade_history
from ..provisioning.account_lifecycle import LifecycleError, pause_account
from ..infra.supabase_client import execute_with_retry

logger = logging.getLogger("weekly_charge")

PLATFORM_CUT_PERCENT = 20.0

_LOOKBACK_DAYS = 7


def _week_start(now: datetime) -> datetime:
    days_since_monday = now.weekday()
    return (now - timedelta(days=days_since_monday)).replace(hour=0, minute=0, second=0, microsecond=0)


def _already_charged_this_week(follower_account_id: str, week_start_iso: str, supabase_client: Any) -> bool:
    response = execute_with_retry(
        lambda: (
            supabase_client.table("weekly_charges")
            .select("follower_account_id")
            .eq("follower_account_id", follower_account_id)
            .eq("week_start", week_start_iso)
            .execute()
        )
    )
    return bool(response.data)


def charge_follower(
    *, follower_account_id: str, agent: Any, pair_store: Any, role: str, fanout: Any, supabase_client: Any,
) -> dict | None:
    period = billing.get_active_period(follower_account_id, supabase_client)
    if period is None:
        return None 

    now = datetime.now(timezone.utc)
    week_start = _week_start(now)
    week_start_iso = week_start.isoformat()

    if _already_charged_this_week(follower_account_id, week_start_iso, supabase_client):
        return None

    deals = trade_history.get_account_trade_history(agent, lookback_days=_LOOKBACK_DAYS)
    copied_profit = 0.0
    for deal in deals:
        if deal.get("entry") != "out":
            continue
        pnl = float(deal.get("pnl", 0) or 0)
        if pnl <= 0:
            continue
        ticket = str(deal["deal_ticket"])
        if not pair_store.was_follower_ticket_copied(follower_account_id, ticket):
            continue 
        copied_profit += pnl

    if copied_profit <= 0:
        return None

    charge_amount = round(copied_profit * PLATFORM_CUT_PERCENT / 100, 2)
    result = wallet.debit(follower_account_id, charge_amount, "platform_weekly_charge", supabase_client)

    execute_with_retry(
        lambda: supabase_client.table("weekly_charges").insert(
            {
                "follower_account_id": follower_account_id,
                "week_start": week_start_iso,
                "copied_profit": copied_profit,
                "charge_amount": charge_amount,
            }
        ).execute()
    )

    if result["in_debt"]:
        logger.warning(
            "Weekly charge put follower %s in debt (balance %.2f) - pausing new copies, same "
            "as a failed infra-fee charge already does",
            follower_account_id, result["balance"],
        )
        try:
            pause_account(
                account_id=follower_account_id, role=role, force_close=False,
                fanout=fanout, supabase_client=supabase_client,
            )
        except LifecycleError:
            logger.warning("Could not pause %s on entering wallet debt (already paused/closed?)", follower_account_id)

    logger.info(
        "Weekly charge: follower %s copied-profit=%.2f -> charged %.2f (%.0f%%), balance now %.2f%s",
        follower_account_id, copied_profit, charge_amount, PLATFORM_CUT_PERCENT, result["balance"],
        " (IN DEBT - paused)" if result["in_debt"] else "",
    )
    return {"follower_account_id": follower_account_id, "copied_profit": copied_profit, "charge_amount": charge_amount}


def run_weekly_charge_cycle(*, fanout: Any, supabase_client: Any) -> int:
    total = 0
    for account_id, agent in fanout.follower_agents.items():
        try:
            charged = charge_follower(
                follower_account_id=account_id, agent=agent, pair_store=fanout.pair_store,
                role="follower", fanout=fanout, supabase_client=supabase_client,
            )
            if charged:
                total += 1
        except Exception:
            logger.exception("Weekly platform charge failed for follower %s this cycle - will retry next cycle", account_id)
    return total