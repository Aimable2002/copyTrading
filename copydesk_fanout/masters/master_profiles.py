from __future__ import annotations

import logging
from typing import Any

from ..infra.supabase_client import execute_with_retry

# Performance-fee rate feature disabled - platform is subscription-only now.
# Kept the import commented rather than removed in case this is revisited.
# from ..billing.master_rate import get_public_rate

logger = logging.getLogger("master_profiles")


class MasterProfileError(Exception):
    """Raised for any failure here. Message is safe to surface to an API caller."""


def upsert_profile(
    *, account_id: str, user_id: str, display_name: str, bio: str, country: str | None = None,
    supabase_client: Any,
) -> dict:
    if not display_name.strip():
        raise MasterProfileError("display_name cannot be empty")

    existing = get_own_profile(account_id, supabase_client)
    payload = {
        "master_account_id": account_id,
        "user_id": user_id,
        "display_name": display_name.strip(),
        "bio": bio.strip(),
        "country": country,
    }
    if existing is None:
        payload["is_public"] = False 

    execute_with_retry(
        lambda: supabase_client.table("master_profiles").upsert(payload, on_conflict="master_account_id").execute()
    )

    is_public = existing["is_public"] if existing is not None else False
    logger.info("Upserted master profile for %s (is_public unchanged at %s)", account_id, is_public)
    return {"account_id": account_id, "display_name": display_name, "country": country, "is_public": is_public}


def set_public_status(account_id: str, is_public: bool, supabase_client: Any) -> None:
    execute_with_retry(
        lambda: supabase_client.table("master_profiles").update({"is_public": is_public}).eq("master_account_id", account_id).execute()
    )
    logger.info("Set is_public=%s for master %s (system-triggered, not a user action)", is_public, account_id)


def get_own_profile(account_id: str, supabase_client: Any) -> dict | None:
    response = execute_with_retry(
        lambda: (
            supabase_client.table("master_profiles")
            .select("master_account_id, display_name, bio, country, is_public")
            .eq("master_account_id", account_id)
            .execute()
        )
    )
    rows = response.data or []
    if not rows:
        return None
    row = rows[0]
    return {
        "account_id": row["master_account_id"],
        "display_name": row["display_name"],
        "bio": row.get("bio"),
        "country": row.get("country"),
        "is_public": row["is_public"],
    }


def is_public_master(account_id: str, supabase_client: Any) -> bool:
    response = execute_with_retry(
        lambda: (
            supabase_client.table("master_profiles")
            .select("is_public")
            .eq("master_account_id", account_id)
            .execute()
        )
    )
    rows = response.data or []
    return bool(rows) and rows[0].get("is_public", False)


def list_public_masters(supabase_client: Any) -> list[dict]:
    profiles_response = execute_with_retry(
        lambda: (
            supabase_client.table("master_profiles")
            .select("master_account_id, display_name, bio, country")
            .eq("is_public", True)
            .execute()
        )
    )
    profiles = profiles_response.data or []
    if not profiles:
        return []

    account_ids = [p["master_account_id"] for p in profiles]
    live_response = execute_with_retry(
        lambda: (
            supabase_client.table("accounts")
            .select("account_id, platform, broker")
            .in_("account_id", account_ids)
            .eq("status", "live")
            .execute()
        )
    )
    live_rows = {row["account_id"]: row for row in (live_response.data or [])}

    return [
        {
            "account_id": p["master_account_id"],
            "display_name": p["display_name"],
            "bio": p["bio"],
            "country": p.get("country"),
            # Performance-fee rate feature disabled - platform is
            # subscription-only now. rate_percent intentionally omitted
            # rather than always-None, so it's clearly gone from the
            # contract instead of looking like a master who never set one.
            # "rate_percent": _get_rate_or_none(p["master_account_id"], supabase_client),
            "platform": live_rows[p["master_account_id"]].get("platform", "mt5"),
            "broker": live_rows[p["master_account_id"]].get("broker"),
        }
        for p in profiles
        if p["master_account_id"] in live_rows
    ]


def list_all_masters(supabase_client: Any) -> list[dict]:
    """Admin-only view: every master profile regardless of is_public or
    account status. Deliberately separate from list_public_masters rather
    than adding a flag to it - that function's contract (public masters
    only) is depended on by the public directory route and must not change."""
    profiles_response = execute_with_retry(
        lambda: (
            supabase_client.table("master_profiles")
            .select("master_account_id, display_name, bio, country, is_public")
            .execute()
        )
    )
    profiles = profiles_response.data or []
    if not profiles:
        return []

    account_ids = [p["master_account_id"] for p in profiles]
    status_response = execute_with_retry(
        lambda: (
            supabase_client.table("accounts")
            .select("account_id, status, platform, broker")
            .in_("account_id", account_ids)
            .execute()
        )
    )
    status_by_id = {row["account_id"]: row for row in (status_response.data or [])}

    return [
        {
            "account_id": p["master_account_id"],
            "display_name": p["display_name"],
            "bio": p["bio"],
            "country": p.get("country"),
            "is_public": p["is_public"],
            "account_status": status_by_id.get(p["master_account_id"], {}).get("status", "unknown"),
            # "rate_percent": _get_rate_or_none(p["master_account_id"], supabase_client),  # disabled, see above
            "platform": status_by_id.get(p["master_account_id"], {}).get("platform", "mt5"),
            "broker": status_by_id.get(p["master_account_id"], {}).get("broker"),
        }
        for p in profiles
    ]


# def _get_rate_or_none(master_account_id: str, supabase_client: Any) -> float | None:
#     # Performance-fee rate feature disabled - platform is subscription-only now.
#     rate = get_public_rate(master_account_id, supabase_client)
#     return rate["rate_percent"] if rate else None