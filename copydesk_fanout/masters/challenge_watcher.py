from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

from ..infra.supabase_client import execute_with_retry
from . import challenges

logger = logging.getLogger("challenge_watcher")

# live_state_publisher.py ticks every ~10ms per connected agent - querying
# master_challenge_enrollments on every single tick for every master would
# be enormous, pointless load for the overwhelming majority of masters who
# aren't enrolled in anything. This cache means at most one query per
# master per _CACHE_TTL_SECONDS; a few seconds of lag before a breach is
# caught is an acceptable tradeoff (MT5/cTrader equity itself doesn't
# update instantaneously either).
_CACHE_TTL_SECONDS = 5.0
_cache: dict[str, tuple[float, dict | None]] = {}


def invalidate_cache(master_account_id: str) -> None:
    """Called by challenges.enroll()/leave() so the watcher doesn't act on
    a stale cached enrollment for up to _CACHE_TTL_SECONDS after a state
    change. Safe to call even if there's nothing cached yet."""
    _cache.pop(master_account_id, None)


def _get_cached_enrollment(master_account_id: str, supabase_client: Any) -> dict | None:
    now = time.monotonic()
    cached = _cache.get(master_account_id)
    if cached is not None and (now - cached[0]) < _CACHE_TTL_SECONDS:
        return cached[1]

    enrollment = challenges.get_current_enrollment(master_account_id, supabase_client)
    _cache[master_account_id] = (now, enrollment)
    return enrollment


def _write_curve_point(enrollment_id: str, snapshot_date: str, equity: float, supabase_client: Any) -> None:
    try:
        execute_with_retry(
            lambda: supabase_client.table("challenge_equity_curve").upsert(
                {"enrollment_id": enrollment_id, "snapshot_date": snapshot_date, "equity": equity},
                on_conflict="enrollment_id,snapshot_date",
            ).execute()
        )
    except Exception:
        logger.exception("Failed to write challenge equity curve point for enrollment %s (%s)", enrollment_id, snapshot_date)


def check_enrollment(master_account_id: str, equity: float | None, supabase_client: Any) -> None:
    """Called from live_state_publisher.py's tick loop for every connected
    master. Standard prop-firm rules, global convention:
      - daily loss measured from each calendar day's opening equity,
        breached the instant it's crossed intraday (not an end-of-day
        check)
      - max drawdown trailing from the peak equity reached since
        enrollment (not just from the starting balance)
      - profit target only counts as a pass once both the target % AND
        the minimum elapsed calendar days are satisfied
    No expiry - an enrollment just sits in 'enrolled' until one of these
    fires; there is deliberately no timeout path here.
    """
    if equity is None:
        return

    enrollment = _get_cached_enrollment(master_account_id, supabase_client)
    if enrollment is None:
        return  # not enrolled in anything right now - the overwhelming common case

    starting_equity = enrollment.get("starting_equity")
    if starting_equity is None:
        # Data issue, not a runtime one: an 'enrolled' row with no
        # starting_equity can't be evaluated at all (every calculation
        # below is relative to it). Only reachable via a row that bypassed
        # challenges.enroll() - e.g. seed/test data inserted directly, or a
        # leftover row from before this file existed. Log once per cache
        # refresh rather than crashing this tick (and every tick until the
        # cache expires), which would otherwise spam the log ~1x/5s forever
        # for this account.
        logger.error(
            "Enrollment %s for master %s has no starting_equity - cannot evaluate, skipping until fixed. "
            "This enrollment did not go through challenges.enroll() (which always sets it) - check for "
            "manually-inserted or pre-migration rows in master_challenge_enrollments.",
            enrollment["id"], master_account_id,
        )
        return

    challenge = enrollment["challenge"]
    today = datetime.now(timezone.utc).date()
    today_iso = today.isoformat()

    day_start_date = enrollment.get("day_start_date")
    day_start_equity = enrollment.get("day_start_equity") or starting_equity
    peak_equity = max(enrollment.get("peak_equity") or starting_equity, equity)

    updates: dict[str, Any] = {}

    if day_start_date != today_iso:
        if day_start_date is not None:
            _write_curve_point(enrollment["id"], day_start_date, day_start_equity, supabase_client)
        day_start_equity = equity
        updates["day_start_equity"] = equity
        updates["day_start_date"] = today_iso

    if peak_equity != enrollment.get("peak_equity"):
        updates["peak_equity"] = peak_equity

    daily_loss_pct = ((day_start_equity - equity) / day_start_equity * 100) if day_start_equity else 0.0
    drawdown_pct = ((peak_equity - equity) / peak_equity * 100) if peak_equity else 0.0
    profit_pct = ((equity - starting_equity) / starting_equity * 100) if starting_equity else 0.0
    enrolled_at_date = datetime.fromisoformat(enrollment["enrolled_at"].replace("Z", "+00:00")).date()
    elapsed_days = (today - enrolled_at_date).days + 1

    if daily_loss_pct >= challenge["max_daily_loss_pct"]:
        challenges.mark_enrollment_outcome(
            enrollment["id"], "breached", supabase_client, breach_reason=f"daily_loss:{daily_loss_pct:.2f}pct",
        )
        invalidate_cache(master_account_id)
        return

    if drawdown_pct >= challenge["max_drawdown_pct"]:
        challenges.mark_enrollment_outcome(
            enrollment["id"], "breached", supabase_client, breach_reason=f"max_drawdown:{drawdown_pct:.2f}pct",
        )
        invalidate_cache(master_account_id)
        return

    if profit_pct >= challenge["profit_target_pct"] and elapsed_days >= challenge["min_days"]:
        challenges.mark_enrollment_outcome(enrollment["id"], "passed", supabase_client)
        invalidate_cache(master_account_id)
        return

    if updates:
        execute_with_retry(
            lambda: (
                supabase_client.table("master_challenge_enrollments")
                .update(updates)
                .eq("id", enrollment["id"])
                .eq("status", "enrolled")
                .execute()
            )
        )
        # Refresh the cache in place rather than waiting out the TTL, so the
        # very next tick (which may be milliseconds away) sees the rolled-over
        # day/peak instead of re-deriving it from stale cached values.
        _cache[master_account_id] = (time.monotonic(), {**enrollment, **updates})