from __future__ import annotations

import logging
from typing import Any

from postgrest.exceptions import APIError

from ..billing import wallet
from ..infra.supabase_client import execute_with_retry

logger = logging.getLogger("roster")

_ROSTER_SLOT_UNIQUE_VIOLATION = "23505"


class RosterError(Exception):
    """Raised for any failure here. Message is safe to surface to an API caller."""


def get_roster(billing_period_id: str, follower_account_id: str, supabase_client: Any) -> list[dict]:
    response = execute_with_retry(
        lambda: (
            supabase_client.table("roster_slots")
            .select("id, master_account_id, is_current, first_used_at, last_used_at")
            .eq("billing_period_id", billing_period_id)
            .eq("follower_account_id", follower_account_id)
            .execute()
        )
    )
    return response.data or []


def _get_billing_period(billing_period_id: str, supabase_client: Any) -> dict:
    response = execute_with_retry(
        lambda: (
            supabase_client.table("billing_periods")
            .select("id, account_id, status, base_roster_size, purchased_extra_slots, slot_fee_per_slot")
            .eq("id", billing_period_id)
            .execute()
        )
    )
    rows = response.data or []
    if not rows:
        raise RosterError(f"No billing period {billing_period_id}")
    return rows[0]


def switch_master(
    *, billing_period_id: str, follower_account_id: str, new_master_account_id: str, supabase_client: Any,
) -> dict:
    period = _get_billing_period(billing_period_id, supabase_client)
    if period["account_id"] != follower_account_id:
        raise RosterError("Billing period does not belong to this follower account")
    if period["status"] == "closed":
        raise RosterError("Subscription is closed - reactivate before switching masters")

    roster = get_roster(billing_period_id, follower_account_id, supabase_client)
    existing = next((r for r in roster if r["master_account_id"] == new_master_account_id), None)

    if existing is not None:
        _set_current(billing_period_id, follower_account_id, existing["id"], supabase_client)
        _sync_active_subscription(follower_account_id, new_master_account_id, supabase_client)
        logger.info(
            "Follower %s switched back to previously-used master %s (billing_period %s) - no charge",
            follower_account_id, new_master_account_id, billing_period_id,
        )
        return {"master_account_id": new_master_account_id, "roster_slot_id": existing["id"], "charged": False}

    capacity = period["base_roster_size"] + period["purchased_extra_slots"]
    used = len(roster)
    charged = False

    if used >= capacity:
        new_balance = wallet.debit(
            follower_account_id, period["slot_fee_per_slot"], "slot_fee", supabase_client,
            related_master_account_id=new_master_account_id,
        )
        execute_with_retry(
            lambda: supabase_client.table("billing_periods").update(
                {"purchased_extra_slots": period["purchased_extra_slots"] + 1}
            ).eq("id", billing_period_id).execute()
        )
        charged = True
        logger.info(
            "Follower %s bought a slot for new master %s (billing_period %s), wallet now %.2f",
            follower_account_id, new_master_account_id, billing_period_id, new_balance["balance"],
        )

    _clear_current(billing_period_id, follower_account_id, supabase_client)
    try:
        insert_response = execute_with_retry(
            lambda: supabase_client.table("roster_slots").insert(
                {
                    "billing_period_id": billing_period_id,
                    "follower_account_id": follower_account_id,
                    "master_account_id": new_master_account_id,
                    "is_current": True,
                }
            ).execute()
        )
        new_slot_id = insert_response.data[0]["id"]
    except APIError as exc:
        if getattr(exc, "code", None) != _ROSTER_SLOT_UNIQUE_VIOLATION:
            raise
            
        logger.warning(
            "roster_slots insert raced for billing_period %s / master %s - "
            "another request already created this slot, recovering.",
            billing_period_id, new_master_account_id,
        )
        roster_after_race = get_roster(billing_period_id, follower_account_id, supabase_client)
        winner = next((r for r in roster_after_race if r["master_account_id"] == new_master_account_id), None)
        if winner is None:
            raise RosterError(
                "Switch conflicted with a concurrent request and the resulting slot could not be found - "
                "please retry."
            ) from exc
        new_slot_id = winner["id"]
        _set_current(billing_period_id, follower_account_id, new_slot_id, supabase_client)

    _sync_active_subscription(follower_account_id, new_master_account_id, supabase_client)

    return {
        "master_account_id": new_master_account_id,
        "roster_slot_id": new_slot_id,
        "charged": charged,
    }


def _sync_active_subscription(follower_account_id: str, new_master_account_id: str, supabase_client: Any) -> None:
    current = execute_with_retry(
        lambda: (
            supabase_client.table("subscriptions")
            .select("*")
            .eq("follower_account_id", follower_account_id)
            .eq("active", True)
            .execute()
        )
    )
    current_rows = current.data or []

    if current_rows and current_rows[0]["master_account_id"] == new_master_account_id:
        return
    if current_rows:
        sizing_template = current_rows[0]
        execute_with_retry(
            lambda: (
                supabase_client.table("subscriptions")
                .update({"active": False})
                .eq("follower_account_id", follower_account_id)
                .eq("master_account_id", current_rows[0]["master_account_id"])
                .eq("active", True)
                .execute()
            )
        )
    else:
        history = execute_with_retry(
            lambda: (
                supabase_client.table("subscriptions")
                .select("*")
                .eq("follower_account_id", follower_account_id)
                .order("created_at", desc=True)
                .limit(1)
                .execute()
            )
        )
        if not history.data:
            raise RosterError(
                f"Follower {follower_account_id} has no subscription history to carry sizing "
                f"settings from - cannot determine sizing_mode/sizing_value for the new subscription"
            )
        sizing_template = history.data[0]

    existing_for_new_master = execute_with_retry(
        lambda: (
            supabase_client.table("subscriptions")
            .select("*")
            .eq("follower_account_id", follower_account_id)
            .eq("master_account_id", new_master_account_id)
            .execute()
        )
    )
    new_row_fields = {
        "sizing_mode": sizing_template["sizing_mode"],
        "sizing_value": sizing_template.get("sizing_value"),
        "active": True,
    }
    if existing_for_new_master.data:
        execute_with_retry(
            lambda: (
                supabase_client.table("subscriptions")
                .update(new_row_fields)
                .eq("follower_account_id", follower_account_id)
                .eq("master_account_id", new_master_account_id)
                .execute()
            )
        )
    else:
        execute_with_retry(
            lambda: (
                supabase_client.table("subscriptions")
                .insert({"follower_account_id": follower_account_id, "master_account_id": new_master_account_id, **new_row_fields})
                .execute()
            )
        )
    logger.info(
        "Synced real trade routing: follower %s subscription now active on master %s",
        follower_account_id, new_master_account_id,
    )


def _clear_current(billing_period_id: str, follower_account_id: str, supabase_client: Any) -> None:
    execute_with_retry(
        lambda: supabase_client.table("roster_slots").update({"is_current": False}).eq(
            "billing_period_id", billing_period_id
        ).eq("follower_account_id", follower_account_id).execute()
    )


def _set_current(billing_period_id: str, follower_account_id: str, roster_slot_id: str, supabase_client: Any) -> None:
    _clear_current(billing_period_id, follower_account_id, supabase_client)
    execute_with_retry(
        lambda: supabase_client.table("roster_slots").update({"is_current": True}).eq("id", roster_slot_id).execute()
    )


def count_active_followers(master_account_id: str, supabase_client: Any) -> int:
    """Admin-only: how many followers currently have this master as their
    active subscription. Reads `subscriptions` (the live trade-routing
    table), not `roster_slots` (billing-period history) - `active=True`
    rows are the ones actually being copied right now."""
    response = execute_with_retry(
        lambda: (
            supabase_client.table("subscriptions")
            .select("follower_account_id", count="exact")
            .eq("master_account_id", master_account_id)
            .eq("active", True)
            .execute()
        )
    )
    return response.count or 0


def get_current_slot(billing_period_id: str, follower_account_id: str, supabase_client: Any) -> dict | None:
    response = execute_with_retry(
        lambda: (
            supabase_client.table("roster_slots")
            .select("id, master_account_id")
            .eq("billing_period_id", billing_period_id)
            .eq("follower_account_id", follower_account_id)
            .eq("is_current", True)
            .limit(1)
            .execute()
        )
    )
    rows = response.data or []
    return rows[0] if rows else None


def get_master_followers(master_account_id: str, supabase_client: Any) -> list[dict]:
    """Master-facing: who is currently copying this master, with just
    enough detail for the master's own dashboard (broker, live equity,
    sizing, since-date, status). Scoped to `active=True` subscriptions only
    (the live trade-routing table) - same source as count_active_followers,
    just returning the rows instead of a count. Never exposes anything
    beyond what's already visible elsewhere per-account (no email, no login,
    no balance history)."""
    subs_response = execute_with_retry(
        lambda: (
            supabase_client.table("subscriptions")
            .select("follower_account_id, sizing_mode, sizing_value, active, created_at")
            .eq("master_account_id", master_account_id)
            .eq("active", True)
            .execute()
        )
    )
    subs = subs_response.data or []
    if not subs:
        return []

    follower_ids = [s["follower_account_id"] for s in subs]

    accounts_response = execute_with_retry(
        lambda: (
            supabase_client.table("accounts")
            .select("account_id, broker, platform, status")
            .in_("account_id", follower_ids)
            .execute()
        )
    )
    accounts_by_id = {a["account_id"]: a for a in (accounts_response.data or [])}

    live_response = execute_with_retry(
        lambda: (
            supabase_client.table("live_account_state")
            .select("account_id, equity")
            .in_("account_id", follower_ids)
            .execute()
        )
    )
    equity_by_id = {r["account_id"]: r.get("equity") for r in (live_response.data or [])}

    return [
        {
            "follower_account_id": s["follower_account_id"],
            "broker": accounts_by_id.get(s["follower_account_id"], {}).get("broker"),
            "platform": accounts_by_id.get(s["follower_account_id"], {}).get("platform", "mt5"),
            "equity": equity_by_id.get(s["follower_account_id"]),
            "sizing_mode": s["sizing_mode"],
            "sizing_value": s.get("sizing_value"),
            "since": s["created_at"],
            "status": accounts_by_id.get(s["follower_account_id"], {}).get("status", "unknown"),
        }
        for s in subs
    ]