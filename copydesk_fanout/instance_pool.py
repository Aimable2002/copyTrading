"""
Pool of pre-warmed, already-running MT5 terminal instances.

This is the piece that replaces per-request clone+launch+wait-for-update
in provisioning.py. All of that expensive, flaky work now happens exactly
once per terminal, offline, run manually by an operator via
scripts/register_pool_instance.py - never on a user's provisioning
request.

Lifecycle of one pool row:
  1. register_instance() - operator script inserts it as 'available'
     after confirming the terminal is up and stable. No account is
     logged into it yet.
  2. claim_instance() - a provisioning request atomically takes it,
     'available' -> 'claimed', tagged with the account_id that owns it.
  3. release_instance() - account_lifecycle.close_account() calls this on
     close. The account's on-disk credentials are deleted and the row
     goes back to 'available' for the NEXT claimant.

The terminal process itself is never stopped across this whole lifecycle.
That's the point - a pool instance is meant to run indefinitely, so no
account ever pays the update/restart tax that motivated this module.
There's no supported "log out to blank" call in the MetaTrader5 API, so
step 3 doesn't attempt one: what "signed out" means here is that the
account's credentials no longer exist anywhere in this system or on disk,
and nothing is polling or trading on that instance until the next
claimant's login overwrites whatever session is sitting on it.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Literal

from .supabase_client import execute_with_retry

logger = logging.getLogger("instance_pool")

Role = Literal["master", "follower"]


class PoolError(Exception):
    """Raised for any failure here. Message is safe to surface to an API caller."""


def register_instance(*, instance_dir: str, terminal_path: str, role: Role, supabase_client: Any) -> dict:
    """
    Adds one already-running, already-warmed, never-logged-in terminal to
    the pool as available. Called ONLY by scripts/register_pool_instance.py
    - never by the request-serving app.
    """
    response = execute_with_retry(
        lambda: supabase_client.table("instance_pool").insert(
            {
                "instance_dir": instance_dir,
                "terminal_path": terminal_path,
                "role": role,
                "status": "available",
            }
        ).execute()
    )
    logger.info("Registered new pool instance (%s): %s", role, instance_dir)
    return response.data[0]


def claim_instance(*, role: Role, account_id: str, supabase_client: Any) -> dict:
    """
    Atomically claims one available instance of the given role for
    account_id, via the claim_pool_instance Postgres function (SELECT ...
    FOR UPDATE SKIP LOCKED). Raises PoolError if the pool is empty for
    that role - this is a capacity problem, not something to silently
    fall back to a cold clone+launch for, so the caller should surface it
    as "contact support to top up the pool" rather than retry.
    """
    response = execute_with_retry(
        lambda: supabase_client.rpc(
            "claim_pool_instance", {"p_role": role, "p_account_id": account_id}
        ).execute()
    )
    rows = response.data or []
    if not rows:
        raise PoolError(
            f"No pre-warmed {role} terminal instances are available right now. This is a "
            f"capacity problem, not a per-request failure - contact support to have the "
            f"pool topped up."
        )
    claimed = rows[0]
    logger.info("Claimed pool instance %s for %s account %s", claimed["instance_dir"], role, account_id)
    return claimed


def release_instance(*, account_id: str, supabase_client: Any) -> dict | None:
    """
    Frees the instance currently claimed by account_id: deletes the
    on-disk credentials file (provisioned_config.ini) so nothing is left
    behind readable, then marks the pool row available again. Returns
    None (and just logs a warning) if no pool row is claimed by this
    account, so this is safe to call defensively on close without first
    checking whether the account predates the pool model.
    """
    response = execute_with_retry(
        lambda: (
            supabase_client.table("instance_pool")
            .select("*")
            .eq("claimed_by_account_id", account_id)
            .limit(1)
            .execute()
        )
    )
    rows = response.data or []
    if not rows:
        logger.warning(
            "release_instance: no pool row is claimed by account %s - nothing to release "
            "(already released, or this account predates the pool model).",
            account_id,
        )
        return None
    pool_row = rows[0]

    config_path = Path(pool_row["instance_dir"]) / "provisioned_config.ini"
    try:
        config_path.unlink(missing_ok=True)
    except OSError as exc:
        logger.warning(
            "release_instance: could not delete %s for account %s (%s) - freeing the pool "
            "row anyway, but stale credentials may still be on disk, check manually.",
            config_path, account_id, exc,
        )

    update_response = execute_with_retry(
        lambda: (
            supabase_client.table("instance_pool")
            .update({"status": "available", "claimed_by_account_id": None, "claimed_at": None})
            .eq("id", pool_row["id"])
            .execute()
        )
    )
    logger.info("Released pool instance %s (was account %s)", pool_row["instance_dir"], account_id)
    return update_response.data[0]


def pool_status(supabase_client: Any) -> dict:
    """
    Free/claimed counts per role. Meant for a health check or an alert
    that pages someone to run the registration script - not for the
    request path, and not a substitute for claim_instance's atomicity.
    """
    response = execute_with_retry(
        lambda: supabase_client.table("instance_pool").select("role, status").execute()
    )
    counts: dict[str, dict[str, int]] = {}
    for row in response.data or []:
        role_counts = counts.setdefault(row["role"], {"available": 0, "claimed": 0})
        role_counts[row["status"]] = role_counts.get(row["status"], 0) + 1
    return counts