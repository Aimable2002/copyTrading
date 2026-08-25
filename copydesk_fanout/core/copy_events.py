from __future__ import annotations

import logging
from typing import Any

from ..infra.supabase_client import execute_with_retry

logger = logging.getLogger("copy_events")


def record_success(
    *, master_account_id: str, follower_account_id: str, master_ticket: str, lots: float,
    supabase_client: Any | None, latency_ms: float | None = None,
) -> None:
    """Persisted record of one master fill successfully copied to one
    follower. Backs PLATFORM_STATS.copiedToday - a simple count of
    status='success' rows created today - and, via latency_ms, the
    platform's average relay-latency stat (see admin_analytics.get_copy_stats).
    supabase_client is optional (None in local-file/no-DB mode) - this is
    best-effort telemetry, never something a copy should fail over."""
    if supabase_client is None:
        return
    try:
        execute_with_retry(
            lambda: supabase_client.table("copy_events").insert(
                {
                    "master_account_id": master_account_id,
                    "follower_account_id": follower_account_id,
                    "master_ticket": str(master_ticket),
                    "lots": lots,
                    "status": "success",
                    "latency_ms": round(latency_ms) if latency_ms is not None else None,
                }
            ).execute()
        )
    except Exception:
        logger.exception(
            "Failed to record copy_events success row for master %s -> follower %s (ticket %s) - "
            "the copy itself already went through fine, this is just the audit log",
            master_account_id, follower_account_id, master_ticket,
        )


def record_failure(
    *, master_account_id: str, follower_account_id: str, master_ticket: str, reason: str,
    supabase_client: Any | None,
) -> None:
    """Persisted record of a copy attempt that did not result in a follower
    fill. Backs the admin 'Failed copies (24h)' KPI. reason is a short,
    human-readable string (e.g. 'not_connected', 'sizing_failed: ...',
    'order_cap_exceeded', 'order_rejected: retcode=...') - free text, not
    an enum, since the failure points in fanout_core.py are varied and this
    is for admin visibility, not machine handling."""
    if supabase_client is None:
        return
    try:
        execute_with_retry(
            lambda: supabase_client.table("copy_events").insert(
                {
                    "master_account_id": master_account_id,
                    "follower_account_id": follower_account_id,
                    "master_ticket": str(master_ticket),
                    "status": "failed",
                    "reason": reason,
                }
            ).execute()
        )
    except Exception:
        logger.exception(
            "Failed to record copy_events failure row for master %s -> follower %s (ticket %s)",
            master_account_id, follower_account_id, master_ticket,
        )