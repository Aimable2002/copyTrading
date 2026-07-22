"""
Master profiles - the opt-in layer between an internal `accounts` row and
what a follower browsing the directory actually sees.

Deliberately never exposes the raw MT5 login/account number to a browsing
follower - only this backend's own generated account_id (e.g.
"master_09f692dcf7", see provisioning.py's uuid-based ids), which is
already the safe, synthetic identifier used everywhere else as the foreign
key (subscriptions.master_account_id etc). The actual MT5 login only ever
existed transiently during provisioning to write the startup config - it's
never stored in `accounts` or read back out here.

A master must explicitly opt in (create a profile with is_public=true)
before appearing anywhere a follower can browse - per the earlier decision,
nothing is auto-listed.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("master_profiles")


class MasterProfileError(Exception):
    """Raised for any failure here. Message is safe to surface to an API caller."""


def upsert_profile(
    *, account_id: str, user_id: str, display_name: str, bio: str, is_public: bool, supabase_client: Any,
) -> dict:
    """Create or update the calling user's own master profile. Ownership is
    enforced by the caller (api_server.py) checking account_user_map before
    this is ever reached - this function trusts account_id/user_id already
    match, same pattern as the rest of this codebase (see provisioning.py)."""
    if not display_name.strip():
        raise MasterProfileError("display_name cannot be empty")

    supabase_client.table("master_profiles").upsert(
        {
            "master_account_id": account_id,
            "user_id": user_id,
            "display_name": display_name.strip(),
            "bio": bio.strip(),
            "is_public": is_public,
        },
        on_conflict="master_account_id",
    ).execute()

    logger.info("Upserted master profile for %s (is_public=%s)", account_id, is_public)
    return {"account_id": account_id, "display_name": display_name, "is_public": is_public}


def is_public_master(account_id: str, supabase_client: Any) -> bool:
    """Visibility check for the new /masters/{id}/trades route - deliberately
    NOT an ownership check (that's what account_user_map/_resolve_owned_account
    enforces for a user's own accounts). This is the opposite kind of gate:
    anyone authenticated can see this specific account's data, but only if
    its owner explicitly opted in via upsert_profile(is_public=True). A
    private master or a follower's own account is never reachable this way."""
    response = (
        supabase_client.table("master_profiles")
        .select("is_public")
        .eq("master_account_id", account_id)
        .execute()
    )
    rows = response.data or []
    return bool(rows) and rows[0].get("is_public", False)


def list_public_masters(supabase_client: Any) -> list[dict]:
    """The directory read - every live master that's opted in. Returns
    display_name/bio/account_id only, never the underlying MT5 login (see
    module docstring).

    Deliberately two plain queries + a Python-side intersection rather than
    a single PostgREST embedded-resource filter (accounts!inner(...)) -
    that syntax's exact behavior through supabase-py isn't something to
    guess at without a live instance to verify against; this version is
    slower but unambiguously correct."""
    profiles_response = (
        supabase_client.table("master_profiles")
        .select("master_account_id, display_name, bio")
        .eq("is_public", True)
        .execute()
    )
    profiles = profiles_response.data or []
    if not profiles:
        return []

    account_ids = [p["master_account_id"] for p in profiles]
    live_response = (
        supabase_client.table("accounts")
        .select("account_id")
        .in_("account_id", account_ids)
        .eq("status", "live")
        .execute()
    )
    live_ids = {row["account_id"] for row in (live_response.data or [])}

    return [
        {
            "account_id": p["master_account_id"],
            "display_name": p["display_name"],
            "bio": p["bio"],
            "rate_percent": _get_rate_or_none(p["master_account_id"], supabase_client),
        }
        for p in profiles
        if p["master_account_id"] in live_ids
    ]


def _get_rate_or_none(master_account_id: str, supabase_client: Any) -> float | None:
    """One query per listed master - fine at directory-listing scale,
    worth batching into a single IN query if this list ever grows large
    enough for it to matter. Deliberately imported here rather than at
    module level to avoid a master_profiles<->master_rate import cycle,
    since neither module otherwise needs the other."""
    from .master_rate import get_public_rate
    rate = get_public_rate(master_account_id, supabase_client)
    return rate["rate_percent"] if rate else None