"""
Subscription billing - the infra + slot fee, duration-based packages.
Deliberately separate from `subscriptions` (the existing table, which is
about the copy-relationship itself: multiplier, sizing_mode, active) -
this is about paying to run the account at all, not about how it copies.

Package price scales DOWN with duration (the discount is the point of
picking a longer commitment) - PACKAGES below is the source of truth for
that pricing, same pattern as sizing.py being the source of truth for
sizing modes elsewhere in this codebase.

Wallet creation lives here, not in provisioning.py: select_package() is
the first moment an account's wallet can come into existence, via
wallet.ensure_wallet(). An account can be live and trading with zero
wallet for as long as its owner hasn't bought a package yet.

Grace handling reuses account_lifecycle's EXISTING pause/close machinery
rather than inventing a second one - the moment a charge fails
(insufficient wallet funds), this module calls account_lifecycle.pause_
account(force_close=False) immediately (stops new copies, same mechanism
already used for a manual pause) and records grace_started_at. A
scheduled sweep (see main.py) later calls check_grace_expirations(), which
calls account_lifecycle.close_account() for anything past 5 days
unresolved. Nothing here talks to FanoutCore/Supabase account status
directly except through those two existing functions.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from . import wallet
from .account_lifecycle import LifecycleError, close_account, pause_account
from .supabase_client import execute_with_retry

logger = logging.getLogger("billing")

GRACE_PERIOD_DAYS = 5


class BillingError(Exception):
    """Raised for any failure here. Message is safe to surface to an API caller."""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def get_package(package_code: str, supabase_client: Any) -> dict:
    """Pricing lives in Supabase, not in this file - the frontend's
    pricing page reads the same `packages` table directly (it has its own
    public-read RLS policy, see migration 002), so there is exactly one
    source of truth for a price, editable without a backend deploy."""
    response = execute_with_retry(
        lambda: (
            supabase_client.table("packages")
            .select("code, duration_days, infra_fee, slot_fee_per_slot, base_roster_size")
            .eq("code", package_code)
            .eq("is_active", True)
            .execute()
        )
    )
    rows = response.data or []
    if not rows:
        raise BillingError(f"Unknown or inactive package {package_code}")
    return rows[0]


def get_active_period(account_id: str, supabase_client: Any) -> dict | None:
    response = execute_with_retry(
        lambda: (
            supabase_client.table("billing_periods")
            .select("*")
            .eq("account_id", account_id)
            .in_("status", ["active", "grace"])
            .order("started_at", desc=True)
            .limit(1)
            .execute()
        )
    )
    rows = response.data or []
    return rows[0] if rows else None


def _charge_period(account_id: str, package: dict, supabase_client: Any) -> dict:
    """The actual infra-fee debit + resulting active/grace decision, shared
    by select_package (new period) and renew (same account, next period)."""
    result = wallet.debit(account_id, package["infra_fee"], "infra_fee", supabase_client)
    status = "grace" if result["in_debt"] else "active"
    return {"status": status, "wallet": result}


def select_package(*, account_id: str, package_code: str, role: str, fanout: Any, supabase_client: Any) -> dict:
    if get_active_period(account_id, supabase_client) is not None:
        raise BillingError("Account already has an active or grace-period subscription - close or wait for it to lapse first")

    package = get_package(package_code, supabase_client)
    wallet.ensure_wallet(account_id, supabase_client)
    charge = _charge_period(account_id, package, supabase_client)

    started_at = _now()
    renews_at = started_at + timedelta(days=package["duration_days"])
    insert_response = execute_with_retry(
        lambda: supabase_client.table("billing_periods").insert(
            {
                "account_id": account_id,
                "package_code": package_code,
                "duration_days": package["duration_days"],
                "infra_fee": package["infra_fee"],
                "slot_fee_per_slot": package["slot_fee_per_slot"],
                "base_roster_size": package["base_roster_size"],
                "purchased_extra_slots": 0,
                "status": charge["status"],
                "grace_started_at": _now().isoformat() if charge["status"] == "grace" else None,
                "started_at": started_at.isoformat(),
                "renews_at": renews_at.isoformat(),
            }
        ).execute()
    )
    period = insert_response.data[0]

    if charge["status"] == "grace":
        _enter_grace(account_id, role, fanout, supabase_client)

    logger.info("Account %s selected package %s -> status=%s, wallet balance %.2f", account_id, package_code, charge["status"], charge["wallet"]["balance"])
    return period


def _enter_grace(account_id: str, role: str, fanout: Any, supabase_client: Any) -> None:
    try:
        pause_account(account_id=account_id, role=role, force_close=False, fanout=fanout, supabase_client=supabase_client)
    except LifecycleError:
        # Account may not have a running agent (e.g. already paused) -
        # grace status on billing_periods is still recorded regardless.
        logger.warning("Could not pause %s on entering billing grace (already paused/closed?)", account_id)


def check_grace_expirations(fanout: Any, supabase_client: Any, agents: list) -> list[str]:
    """Scheduled sweep target - closes any billing_period that's been in
    grace longer than GRACE_PERIOD_DAYS. Returns the account_ids closed."""
    cutoff = (_now() - timedelta(days=GRACE_PERIOD_DAYS)).isoformat()
    response = execute_with_retry(
        lambda: (
            supabase_client.table("billing_periods")
            .select("id, account_id")
            .eq("status", "grace")
            .lte("grace_started_at", cutoff)
            .execute()
        )
    )
    closed: list[str] = []
    for row in response.data or []:
        account_id = row["account_id"]
        execute_with_retry(
            lambda row=row: supabase_client.table("billing_periods").update({"status": "closed"}).eq("id", row["id"]).execute()
        )
        # Role isn't stored on billing_periods - look it up once via accounts,
        # same source api_server.py's account_user_map is built from.
        acct = execute_with_retry(
            lambda account_id=account_id: supabase_client.table("accounts").select("role").eq("account_id", account_id).execute()
        ).data
        role = acct[0]["role"] if acct else "follower"
        try:
            close_account(account_id=account_id, role=role, fanout=fanout, supabase_client=supabase_client, agents=agents)
        except LifecycleError:
            logger.warning("Could not close %s after grace expiry (already closed?)", account_id)
        closed.append(account_id)
        logger.info("Closed %s - billing grace period exceeded %d days", account_id, GRACE_PERIOD_DAYS)
    return closed