"""
Roster / switch slots.

ASSUMPTION, stated explicitly because it wasn't fully pinned down in
discussion and this module is unbuildable without picking one: a follower
copies exactly ONE master at a time (is_current=True on exactly one
roster_slots row per billing_period). "Roster capacity" is not "how many
masters you can copy simultaneously", it's "how many distinct masters
you're allowed to have EVER TOUCHED this billing period" - switching among
masters you've already touched is free (that's the whole point), touching
a brand-new one beyond your current capacity costs a slot. If simultaneous
multi-master copying turns out to be the actual intent, this module's
capacity-counting logic still holds, only is_current would need to allow
multiple true rows at once - flag this back if that's wrong.

Roster rows are scoped to billing_period_id, not to the account directly -
that's deliberate and is what makes "resets at renewal" free: a new
billing_period row means an empty roster, no explicit reset/cleanup job
needed anywhere. See migration 002's comment on roster_slots.

Capacity = billing_periods.base_roster_size + billing_periods.purchased_extra_slots.
Switching to a master already in the roster (any time this period, current
or not) never touches capacity or the wallet - the unique index on
(billing_period_id, master_account_id) is what makes "have I used this
master before, this period" a single lookup. Switching to a genuinely new
master:
  - if under capacity: free, just consumes one more of the roster's
    existing (already-paid-for) seats.
  - if at capacity: this IS a slot purchase - debits slot_fee_per_slot
    from the wallet and increments purchased_extra_slots by one, in the
    same action. There's no separate "buy a slot" action ahead of time;
    buying capacity and using it happen together, the moment they're
    needed.
"""

from __future__ import annotations

import logging
from typing import Any

from . import master_rate, wallet

logger = logging.getLogger("roster")


class RosterError(Exception):
    """Raised for any failure here. Message is safe to surface to an API caller."""


def get_roster(billing_period_id: str, follower_account_id: str, supabase_client: Any) -> list[dict]:
    response = (
        supabase_client.table("roster_slots")
        .select("id, master_account_id, is_current, first_used_at, last_used_at")
        .eq("billing_period_id", billing_period_id)
        .eq("follower_account_id", follower_account_id)
        .execute()
    )
    return response.data or []


def _get_billing_period(billing_period_id: str, supabase_client: Any) -> dict:
    response = (
        supabase_client.table("billing_periods")
        .select("id, account_id, status, base_roster_size, purchased_extra_slots, slot_fee_per_slot")
        .eq("id", billing_period_id)
        .execute()
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
        # Already used this period - free switch, no slot, no rate re-snapshot
        # (the original snapshot for this slot still applies).
        _set_current(billing_period_id, follower_account_id, existing["id"], supabase_client)
        logger.info(
            "Follower %s switched back to previously-used master %s (billing_period %s) - no charge",
            follower_account_id, new_master_account_id, billing_period_id,
        )
        return {"master_account_id": new_master_account_id, "roster_slot_id": existing["id"], "charged": False}

    capacity = period["base_roster_size"] + period["purchased_extra_slots"]
    used = len(roster)
    charged = False

    if used >= capacity:
        # Overflow - this switch IS a slot purchase.
        new_balance = wallet.debit(
            follower_account_id, period["slot_fee_per_slot"], "slot_fee", supabase_client,
            related_master_account_id=new_master_account_id,
        )
        supabase_client.table("billing_periods").update(
            {"purchased_extra_slots": period["purchased_extra_slots"] + 1}
        ).eq("id", billing_period_id).execute()
        charged = True
        logger.info(
            "Follower %s bought a slot for new master %s (billing_period %s), wallet now %.2f",
            follower_account_id, new_master_account_id, billing_period_id, new_balance["balance"],
        )

    # Flip every other slot to not-current, then insert the new current one.
    _clear_current(billing_period_id, follower_account_id, supabase_client)
    insert_response = supabase_client.table("roster_slots").insert(
        {
            "billing_period_id": billing_period_id,
            "follower_account_id": follower_account_id,
            "master_account_id": new_master_account_id,
            "is_current": True,
        }
    ).execute()
    new_slot_id = insert_response.data[0]["id"]

    rate_snapshot = master_rate.snapshot_rate_for_copy(
        follower_account_id=follower_account_id, master_account_id=new_master_account_id,
        roster_slot_id=new_slot_id, supabase_client=supabase_client,
    )

    return {
        "master_account_id": new_master_account_id,
        "roster_slot_id": new_slot_id,
        "charged": charged,
        "rate_percent": rate_snapshot["rate_percent"],
    }


def _clear_current(billing_period_id: str, follower_account_id: str, supabase_client: Any) -> None:
    supabase_client.table("roster_slots").update({"is_current": False}).eq(
        "billing_period_id", billing_period_id
    ).eq("follower_account_id", follower_account_id).execute()


def _set_current(billing_period_id: str, follower_account_id: str, roster_slot_id: str, supabase_client: Any) -> None:
    _clear_current(billing_period_id, follower_account_id, supabase_client)
    supabase_client.table("roster_slots").update({"is_current": True}).eq("id", roster_slot_id).execute()


def get_current_slot(billing_period_id: str, follower_account_id: str, supabase_client: Any) -> dict | None:
    """What profit_share.py's poller uses to find which master a
    follower's closed trades should be billed against."""
    response = (
        supabase_client.table("roster_slots")
        .select("id, master_account_id")
        .eq("billing_period_id", billing_period_id)
        .eq("follower_account_id", follower_account_id)
        .eq("is_current", True)
        .limit(1)
        .execute()
    )
    rows = response.data or []
    return rows[0] if rows else None