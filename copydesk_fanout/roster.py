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
from .supabase_client import execute_with_retry

logger = logging.getLogger("roster")


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
        # Already used this period - free switch, no slot, no rate re-snapshot
        # (the original snapshot for this slot still applies)... except that
        # assumption can be false: if the FIRST time this slot was created,
        # snapshot_rate_for_copy() raised (master hadn't set a rate yet) after
        # the roster_slots row had already committed, this slot is left
        # permanently orphaned - real slot row, no follower_copy_rates row,
        # ever. That used to mean silent, permanent unbillable trades for
        # this slot (profit_share.py's poller just logs "no rate snapshot"
        # forever and skips billing - it never self-heals on its own).
        # Self-heal here instead: if this slot genuinely has no snapshot,
        # take one now against whatever rate the master has set AT THIS
        # MOMENT, rather than assuming one already exists.
        snapshot = master_rate.get_copy_rate_for_slot(existing["id"], supabase_client)
        if snapshot is None:
            logger.warning(
                "Roster slot %s (follower %s, master %s) has no rate snapshot - self-healing by "
                "snapshotting the master's current rate now (most likely cause: the master hadn't "
                "set a rate yet the first time this slot was created)",
                existing["id"], follower_account_id, new_master_account_id,
            )
            master_rate.snapshot_rate_for_copy(
                follower_account_id=follower_account_id, master_account_id=new_master_account_id,
                roster_slot_id=existing["id"], supabase_client=supabase_client,
            )
        _set_current(billing_period_id, follower_account_id, existing["id"], supabase_client)
        _sync_active_subscription(follower_account_id, new_master_account_id, supabase_client)
        logger.info(
            "Follower %s switched back to previously-used master %s (billing_period %s) - no charge",
            follower_account_id, new_master_account_id, billing_period_id,
        )
        return {"master_account_id": new_master_account_id, "roster_slot_id": existing["id"], "charged": False}

    # Confirmed BEFORE any commit or charge below - this is what stops a
    # brand-new slot from ever ending up orphaned like the case above.
    # Previously this check only happened implicitly, inside
    # snapshot_rate_for_copy(), AFTER the roster_slots insert (and after
    # the slot-purchase wallet debit, if this switch was an overflow) had
    # already committed - so a master with no rate set left both of those
    # stuck in place with no way to undo them.
    if master_rate.get_current_rate(new_master_account_id, supabase_client) is None:
        raise RosterError(f"Master {new_master_account_id} has not set a rate yet - cannot be copied")

    capacity = period["base_roster_size"] + period["purchased_extra_slots"]
    used = len(roster)
    charged = False

    if used >= capacity:
        # Overflow - this switch IS a slot purchase.
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

    # Flip every other slot to not-current, then insert the new current one.
    _clear_current(billing_period_id, follower_account_id, supabase_client)
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
    _sync_active_subscription(follower_account_id, new_master_account_id, supabase_client)

    # Guaranteed to succeed now - the rate-exists check above already ran
    # before this slot (or any charge) was committed.
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


def _sync_active_subscription(follower_account_id: str, new_master_account_id: str, supabase_client: Any) -> None:
    """Keeps the REAL trade-routing table (`subscriptions`, what
    ConfigStore/fanout_core actually reads to decide who gets copied
    trades) in lockstep with whichever master roster_slots currently says
    is active for this follower.

    Without this, roster_slots and subscriptions are two entirely separate
    systems that silently disagree: roster_slots (this module) tracks
    billing/slot-capacity and is what switch_master() has always updated;
    subscriptions is a single row inserted once at provisioning time and
    never touched again by anything - so switching masters here changed
    what the follower is BILLED for, without changing what they actually
    COPY. A follower could be charged against a master whose trades
    they've never received a single copy of.

    Only one subscription should ever be active at a time per follower -
    a follower copies exactly one master's live signals at once, even
    though roster_slots may remember up to their package's full roster
    capacity of masters they've used this billing period. So this always
    deactivates whatever was active before (if anything) and activates
    (or inserts, if this master was never subscribed to before) exactly
    one row for new_master_account_id.

    multiplier/sizing_mode/fixed_master_balance are carried forward from
    whatever the follower's most recent subscription was (any master) -
    switch_master()'s API never collects new sizing input, so these are
    effectively follower-level settings that travel with whichever master
    is currently active, not master-specific configuration.
    """
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
        return  # already the active subscription for this exact master - nothing to change

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
        # No currently-active row (shouldn't normally happen - provisioning
        # always creates one - but fall back to this follower's most recent
        # subscription of any status for the multiplier/sizing_mode
        # template, rather than inventing defaults with no basis).
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
                f"settings from - cannot determine multiplier/sizing_mode for the new subscription"
            )
        sizing_template = history.data[0]

    # Explicit check-then-insert-or-update rather than upsert(on_conflict=...)
    # - matches the rest of this codebase's style (see master_rate.py,
    # roster_slots lookups above) and doesn't assume a unique constraint on
    # (follower_account_id, master_account_id) exists in the real schema,
    # which hasn't been verified. Reactivating a master this follower has
    # subscribed to before (e.g. switching back) vs a genuinely first-time
    # master both end up with exactly one row for (follower, new_master)
    # active=True either way.
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
        "multiplier": sizing_template["multiplier"],
        "sizing_mode": sizing_template["sizing_mode"],
        "fixed_master_balance": sizing_template.get("fixed_master_balance"),
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


def get_current_slot(billing_period_id: str, follower_account_id: str, supabase_client: Any) -> dict | None:
    """What profit_share.py's poller uses to find which master a
    follower's closed trades should be billed against."""
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