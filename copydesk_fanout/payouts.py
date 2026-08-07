from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from . import profit_share
from .supabase_client import execute_with_retry

logger = logging.getLogger("payouts")


class PayoutError(Exception):
    """Raised for any failure here. Message is safe to surface to an API caller."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_available_balance(master_account_id: str, supabase_client: Any) -> float:
    """Lifetime earnings minus everything already claimed against them.
    Rejected requests don't count - that's what makes the money available
    again. Pending DOES count, so the same earnings can't be requested
    twice while a first request is still awaiting review."""
    earnings = profit_share.get_master_earnings(master_account_id, supabase_client)
    claimed_response = execute_with_retry(
        lambda: (
            supabase_client.table("master_payouts")
            .select("amount, status")
            .eq("master_account_id", master_account_id)
            .in_("status", ["pending", "paid"])
            .execute()
        )
    )
    already_claimed = sum(float(r["amount"]) for r in (claimed_response.data or []))
    return earnings["total_earned"] - already_claimed


def _latest_period_end(master_account_id: str, supabase_client: Any) -> str | None:
    response = execute_with_retry(
        lambda: (
            supabase_client.table("master_payouts")
            .select("period_end")
            .eq("master_account_id", master_account_id)
            .order("period_end", desc=True)
            .limit(1)
            .execute()
        )
    )
    rows = response.data or []
    return rows[0]["period_end"] if rows else None


def request_payout(
    master_account_id: str, amount: float, recipient_name: str, recipient_phone: str, supabase_client: Any,
) -> dict:
    if amount <= 0:
        raise PayoutError("Amount must be greater than zero")

    available = get_available_balance(master_account_id, supabase_client)
    if amount > available:
        raise PayoutError(f"Requested {amount:.2f} exceeds available balance {available:.2f}")

    period_start = _latest_period_end(master_account_id, supabase_client)
    now = _now_iso()

    response = execute_with_retry(
        lambda: (
            supabase_client.table("master_payouts")
            .insert(
                {
                    "master_account_id": master_account_id,
                    "period_start": period_start or now,
                    "period_end": now,
                    "amount": amount,
                    "recipient_name": recipient_name,
                    "recipient_phone": recipient_phone,
                    "status": "pending",
                }
            )
            .execute()
        )
    )
    return response.data[0]


def list_payouts_for_master(master_account_id: str, supabase_client: Any) -> list[dict]:
    response = execute_with_retry(
        lambda: (
            supabase_client.table("master_payouts")
            .select("*")
            .eq("master_account_id", master_account_id)
            .order("period_end", desc=True)
            .execute()
        )
    )
    return response.data or []


def list_pending_payouts(supabase_client: Any) -> list[dict]:
    """Admin queue. master_payouts.master_account_id has no declared FK
    to master_profiles (only to accounts), so PostgREST can't embed the
    join automatically - fetch display names separately and merge here,
    same pattern as master_profiles.list_all_masters uses for
    accounts.status."""
    payouts_response = execute_with_retry(
        lambda: (
            supabase_client.table("master_payouts")
            .select("*")
            .eq("status", "pending")
            .order("period_end", desc=True)
            .execute()
        )
    )
    rows = payouts_response.data or []
    if not rows:
        return []

    account_ids = list({r["master_account_id"] for r in rows})
    profiles_response = execute_with_retry(
        lambda: (
            supabase_client.table("master_profiles")
            .select("master_account_id, display_name")
            .in_("master_account_id", account_ids)
            .execute()
        )
    )
    name_by_id = {p["master_account_id"]: p["display_name"] for p in (profiles_response.data or [])}

    return [{**r, "master_display_name": name_by_id.get(r["master_account_id"])} for r in rows]


def approve_payout(payout_id: str, supabase_client: Any) -> dict:
    response = execute_with_retry(
        lambda: (
            supabase_client.table("master_payouts")
            .update({"status": "paid", "paid_at": _now_iso()})
            .eq("id", payout_id)
            .eq("status", "pending")  # can't approve something already resolved
            .execute()
        )
    )
    if not response.data:
        raise PayoutError(f"No pending payout {payout_id} to approve")
    return response.data[0]


def reject_payout(payout_id: str, reason: str, supabase_client: Any) -> dict:
    if not reason.strip():
        raise PayoutError("A rejection reason is required")
    response = execute_with_retry(
        lambda: (
            supabase_client.table("master_payouts")
            .update({"status": "rejected", "rejection_reason": reason})
            .eq("id", payout_id)
            .eq("status", "pending")
            .execute()
        )
    )
    if not response.data:
        raise PayoutError(f"No pending payout {payout_id} to reject")
    return response.data[0]