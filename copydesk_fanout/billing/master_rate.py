from __future__ import annotations

import logging
from typing import Any

from .infra.supabase_client import execute_with_retry

logger = logging.getLogger("master_rate")


class MasterRateError(Exception):
    """Raised for any failure here. Message is safe to surface to an API caller."""


def set_rate(master_account_id: str, rate_percent: float, platform_cut_percent: float, supabase_client: Any) -> dict:
    if not (0 < rate_percent <= 100):
        raise MasterRateError("rate_percent must be between 0 and 100")
    if not (0 <= platform_cut_percent <= rate_percent):
        raise MasterRateError("platform_cut_percent must be between 0 and rate_percent")

    master_net_percent = rate_percent - platform_cut_percent
    execute_with_retry(
        lambda: supabase_client.table("master_rates").insert(
            {
                "master_account_id": master_account_id,
                "rate_percent": rate_percent,
                "platform_cut_percent": platform_cut_percent,
            }
        ).execute()
    )

    logger.info(
        "Master %s set rate to %.2f%% (platform cut %.2f%%, master nets %.2f%%)",
        master_account_id, rate_percent, platform_cut_percent, master_net_percent,
    )
    return {
        "master_account_id": master_account_id,
        "rate_percent": rate_percent,
        "platform_cut_percent": platform_cut_percent,
        "master_net_percent": master_net_percent,
    }


def get_current_rate(master_account_id: str, supabase_client: Any) -> dict | None:
    response = execute_with_retry(
        lambda: (
            supabase_client.table("master_rates")
            .select("rate_percent, platform_cut_percent, effective_from")
            .eq("master_account_id", master_account_id)
            .order("effective_from", desc=True)
            .limit(1)
            .execute()
        )
    )
    rows = response.data or []
    if not rows:
        return None
    row = rows[0]
    row["master_net_percent"] = float(row["rate_percent"]) - float(row["platform_cut_percent"])
    return row


def get_public_rate(master_account_id: str, supabase_client: Any) -> dict | None:
    current = get_current_rate(master_account_id, supabase_client)
    if current is None:
        return None
    return {"master_account_id": master_account_id, "rate_percent": current["rate_percent"]}


def snapshot_rate_for_copy(
    *, follower_account_id: str, master_account_id: str, roster_slot_id: str, supabase_client: Any,
) -> dict:
    current = get_current_rate(master_account_id, supabase_client)
    if current is None:
        raise MasterRateError(f"Master {master_account_id} has not set a rate yet - cannot be copied")

    execute_with_retry(
        lambda: supabase_client.table("follower_copy_rates").insert(
            {
                "follower_account_id": follower_account_id,
                "master_account_id": master_account_id,
                "roster_slot_id": roster_slot_id,
                "rate_percent": current["rate_percent"],
                "platform_cut_percent": current["platform_cut_percent"],
            }
        ).execute()
    )

    logger.info(
        "Snapshotted rate %.2f%% (platform %.2f%%) for follower %s copying master %s (slot %s)",
        current["rate_percent"], current["platform_cut_percent"], follower_account_id, master_account_id, roster_slot_id,
    )
    return {
        "follower_account_id": follower_account_id,
        "master_account_id": master_account_id,
        "roster_slot_id": roster_slot_id,
        "rate_percent": current["rate_percent"],
        "platform_cut_percent": current["platform_cut_percent"],
    }


def get_copy_rate_for_slot(roster_slot_id: str, supabase_client: Any) -> dict | None:
    response = execute_with_retry(
        lambda: (
            supabase_client.table("follower_copy_rates")
            .select("rate_percent, platform_cut_percent")
            .eq("roster_slot_id", roster_slot_id)
            .limit(1)
            .execute()
        )
    )
    rows = response.data or []
    return rows[0] if rows else None