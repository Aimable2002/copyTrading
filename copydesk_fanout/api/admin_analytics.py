from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

# from . import master_profiles, master_rate, profit_share, roster
from .masters import master_profiles, roster
from .billing import master_rate, profit_share
from .infra.supabase_client import execute_with_retry

logger = logging.getLogger("admin_analytics")

REVENUE_TX_TYPES = ("infra_fee", "slot_fee", "profit_share_platform")


def _month_key(iso_timestamp: str) -> str:
    """'2026-08-05T12:34:56+00:00' -> '2026-08'. String-prefix bucketing is
    reliable here regardless of the timezone suffix Supabase returns."""
    return iso_timestamp[:7]


def _last_n_months(n: int) -> list[str]:
    now = datetime.now(timezone.utc).replace(day=1)
    months = []
    cursor = now
    for _ in range(n):
        months.append(cursor.strftime("%Y-%m"))
        cursor = (cursor - timedelta(days=1)).replace(day=1)
    return list(reversed(months))


def get_revenue_by_month(supabase_client: Any, months: int = 12) -> list[dict]:
    since = (datetime.now(timezone.utc) - timedelta(days=31 * months)).isoformat()
    response = execute_with_retry(
        lambda: (
            supabase_client.table("wallet_transactions")
            .select("type, amount, created_at")
            .in_("type", list(REVENUE_TX_TYPES))
            .gte("created_at", since)
            .execute()
        )
    )
    buckets: dict[str, dict[str, float]] = defaultdict(lambda: {"infra": 0.0, "slots": 0.0, "profit_share": 0.0})
    for row in response.data or []:
        key = _month_key(row["created_at"])
        amount = abs(float(row["amount"]))
        if row["type"] == "infra_fee":
            buckets[key]["infra"] += amount
        elif row["type"] == "slot_fee":
            buckets[key]["slots"] += amount
        elif row["type"] == "profit_share_platform":
            buckets[key]["profit_share"] += amount

    return [
        {"month": m, "infra": round(buckets[m]["infra"], 2), "slots": round(buckets[m]["slots"], 2),
         "profit_share": round(buckets[m]["profit_share"], 2)}
        for m in _last_n_months(months)
    ]


def get_growth_by_month(supabase_client: Any, months: int = 12) -> list[dict]:
    """Cumulative account count by role, as of the end of each month -
    matches how a growth chart is normally read (running total, not new
    signups per month)."""
    response = execute_with_retry(
        lambda: supabase_client.table("accounts").select("role, created_at").order("created_at").execute()
    )
    rows = response.data or []

    month_list = _last_n_months(months)
    result = []
    masters = followers = 0
    row_idx = 0
    rows.sort(key=lambda r: r["created_at"])

    for month in month_list:
        month_end = month + "-31"  # string comparison against 'YYYY-MM-DD...' is safe here
        while row_idx < len(rows) and _month_key(rows[row_idx]["created_at"]) <= month:
            if rows[row_idx]["role"] == "master":
                masters += 1
            else:
                followers += 1
            row_idx += 1
        result.append({"month": month, "masters": masters, "followers": followers})

    return result


def get_admin_summary(supabase_client: Any) -> dict:
    this_month = _last_n_months(1)[0]
    last_month = _last_n_months(2)[0]
    revenue = get_revenue_by_month(supabase_client, months=2)
    revenue_by_month = {r["month"]: r["infra"] + r["slots"] + r["profit_share"] for r in revenue}
    mrr = revenue_by_month.get(this_month, 0.0)
    mrr_prev = revenue_by_month.get(last_month, 0.0)
    mrr_change_pct = ((mrr - mrr_prev) / mrr_prev * 100) if mrr_prev else 0.0

    accounts_response = execute_with_retry(lambda: supabase_client.table("accounts").select("role").execute())
    roles = [r["role"] for r in (accounts_response.data or [])]
    masters_count = sum(1 for r in roles if r == "master")
    followers_count = sum(1 for r in roles if r == "follower")

    payouts_response = execute_with_retry(
        lambda: supabase_client.table("master_payouts").select("amount").eq("status", "pending").execute()
    )
    payout_rows = payouts_response.data or []
    payouts_pending_amount = sum(float(r["amount"]) for r in payout_rows)

    wallets_response = execute_with_retry(lambda: supabase_client.table("wallets").select("balance").execute())
    at_risk_debt = sum(1 for r in (wallets_response.data or []) if float(r["balance"]) < 0)

    grace_response = execute_with_retry(
        lambda: supabase_client.table("billing_periods").select("id", count="exact").eq("status", "grace").execute()
    )
    at_risk_grace = grace_response.count or 0

    return {
        "mrr": round(mrr, 2),
        "mrr_change_pct": round(mrr_change_pct, 1),
        "accounts_total": len(roles),
        "masters_count": masters_count,
        "followers_count": followers_count,
        "payouts_pending_amount": round(payouts_pending_amount, 2),
        "payouts_pending_count": len(payout_rows),
        "at_risk_wallets_count": at_risk_debt + at_risk_grace,
    }


def list_all_users(supabase_client: Any) -> list[dict]:
    """No email (backend has no access to auth.users without the
    service-role admin API - deliberately not used here per the earlier
    "email not needed" decision). lifetimeValue is the simplest honest
    number available: sum of all wallet_transactions for the account,
    every type combined - not topups-only or earnings-only."""
    accounts_response = execute_with_retry(
        lambda: supabase_client.table("accounts").select("account_id, role, status, created_at").execute()
    )
    accounts = accounts_response.data or []
    if not accounts:
        return []

    account_ids = [a["account_id"] for a in accounts]
    tx_response = execute_with_retry(
        lambda: (
            supabase_client.table("wallet_transactions")
            .select("account_id, amount")
            .in_("account_id", account_ids)
            .execute()
        )
    )
    lifetime_value: dict[str, float] = defaultdict(float)
    for row in tx_response.data or []:
        lifetime_value[row["account_id"]] += abs(float(row["amount"]))

    return [
        {
            "account_id": a["account_id"],
            "role": a["role"],
            "status": a["status"],
            "joined": a["created_at"],
            "lifetime_value": round(lifetime_value.get(a["account_id"], 0.0), 2),
        }
        for a in accounts
    ]


def get_symbol_exposure(supabase_client: Any) -> list[dict]:
    """Current open exposure by symbol, platform-wide - not a historical
    volume chart. live_account_state is refreshed continuously for every
    connected account, but only ever holds "now", so this reflects
    what's open right this moment, nothing more."""
    response = execute_with_retry(
        lambda: supabase_client.table("live_account_state").select("open_positions").execute()
    )
    totals: dict[str, dict[str, float]] = defaultdict(lambda: {"lots": 0.0, "position_count": 0})
    for row in response.data or []:
        for position in row.get("open_positions") or []:
            symbol = position.get("symbol")
            if not symbol:
                continue
            totals[symbol]["lots"] += float(position.get("lots") or 0)
            totals[symbol]["position_count"] += 1

    return sorted(
        [{"symbol": s, "lots": round(v["lots"], 2), "position_count": v["position_count"]} for s, v in totals.items()],
        key=lambda r: r["lots"],
        reverse=True,
    )


def get_top_masters(supabase_client: Any, limit: int = 10) -> list[dict]:
    """followers/revenue/rate are solid (same sources as /admin/masters
    and /admin/masters/{id}). net_pnl is a deliberate best-effort, not a
    true net figure: billed_deals only contains deals that were actually
    profit-share-billed, i.e. profitable closed trades - losing trades
    are never written there (profit_share.py only bills entry=="out" and
    pnl>0). So this sums realized profit on billed trades, NOT true net
    P&L including losses. There's no drawdown figure at all - nothing in
    the schema tracks equity curve/peak-to-trough, so it's omitted
    rather than faked."""
    all_masters = master_profiles.list_all_masters(supabase_client)

    deals_response = execute_with_retry(
        lambda: supabase_client.table("billed_deals").select("master_account_id, pnl").execute()
    )
    billed_pnl: dict[str, float] = defaultdict(float)
    for row in deals_response.data or []:
        billed_pnl[row["master_account_id"]] += float(row["pnl"])

    rows = []
    for m in all_masters:
        account_id = m["account_id"]
        earnings = profit_share.get_master_earnings(account_id, supabase_client)
        rows.append(
            {
                "account_id": account_id,
                "name": m["display_name"],
                "followers": roster.count_active_followers(account_id, supabase_client),
                "revenue": round(earnings["total_earned"], 2),
                "rate_percent": m["rate_percent"],
                "billed_pnl": round(billed_pnl.get(account_id, 0.0), 2),
            }
        )

    rows.sort(key=lambda r: r["revenue"], reverse=True)
    return rows[:limit]