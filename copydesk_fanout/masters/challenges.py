from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Literal

from postgrest.exceptions import APIError

from ..infra.supabase_client import execute_with_retry

logger = logging.getLogger("challenges")

_ENROLLMENT_UNIQUE_VIOLATION = "23505"

# enrolled -> one of passed/breached/failed (system-driven, via
# challenge_watcher.py) or left (master-driven, via leave() below) or reset
# (admin-driven, not wired up here yet). breached/failed are new - the old
# model only had passed/left/reset.
ChallengeStatus = Literal["enrolled", "passed", "breached", "failed", "left", "reset"]


class ChallengeError(Exception):
    """Raised for any failure here. Message is safe to surface to an API caller."""


def list_challenges(supabase_client: Any, active_only: bool = True) -> list[dict]:
    query = supabase_client.table("challenges").select("*")
    if active_only:
        query = query.eq("active", True)
    response = execute_with_retry(lambda: query.order("created_at").execute())
    return response.data or []


def _get_challenge(challenge_id: str, supabase_client: Any) -> dict:
    response = execute_with_retry(
        lambda: supabase_client.table("challenges").select("*").eq("id", challenge_id).limit(1).execute()
    )
    rows = response.data or []
    if not rows:
        raise ChallengeError(f"Unknown challenge {challenge_id}")
    return rows[0]


def _get_fixed_challenge(supabase_client: Any) -> dict | None:
    response = execute_with_retry(
        lambda: supabase_client.table("challenges").select("*").eq("is_fixed", True).limit(1).execute()
    )
    rows = response.data or []
    return rows[0] if rows else None


def has_passed_challenge_one(master_account_id: str, supabase_client: Any) -> bool:
    fixed = _get_fixed_challenge(supabase_client)
    if fixed is None:
        return False
    response = execute_with_retry(
        lambda: (
            supabase_client.table("master_challenge_enrollments")
            .select("id")
            .eq("master_account_id", master_account_id)
            .eq("challenge_id", fixed["id"])
            .eq("status", "passed")
            .limit(1)
            .execute()
        )
    )
    return bool(response.data)


def get_phase(master_account_id: str, supabase_client: Any) -> str:
    return "graduated" if has_passed_challenge_one(master_account_id, supabase_client) else "challenger"


def get_current_enrollment(master_account_id: str, supabase_client: Any) -> dict | None:
    response = execute_with_retry(
        lambda: (
            supabase_client.table("master_challenge_enrollments")
            .select("*")
            .eq("master_account_id", master_account_id)
            .eq("status", "enrolled")
            .order("enrolled_at", desc=True)
            .limit(1)
            .execute()
        )
    )
    rows = response.data or []
    if not rows:
        return None
    enrollment = rows[0]
    enrollment["challenge"] = _get_challenge(enrollment["challenge_id"], supabase_client)
    return enrollment


def _assert_can_enroll(master_account_id: str, challenge: dict, supabase_client: Any) -> None:
    """Shared pre-checks used both by enroll() itself and by
    payments/checkout.py before it ever charges someone for a challenge
    entry - no point taking a fee for an enrollment that was always going
    to be rejected."""
    if not challenge["active"]:
        raise ChallengeError("This challenge is not currently active")
    if get_current_enrollment(master_account_id, supabase_client) is not None:
        raise ChallengeError("Already enrolled in a challenge - leave it before enrolling in another")
    if not challenge["is_fixed"] and not has_passed_challenge_one(master_account_id, supabase_client):
        raise ChallengeError("Challenge 1 must be passed before enrolling in any other challenge")


def assert_can_enroll(master_account_id: str, challenge_id: str, supabase_client: Any) -> dict:
    """Public wrapper for payments/checkout.py - returns the challenge row
    (so checkout can read its fee) if enrollment would currently succeed,
    raises ChallengeError otherwise."""
    challenge = _get_challenge(challenge_id, supabase_client)
    _assert_can_enroll(master_account_id, challenge, supabase_client)
    return challenge


def enroll(
    *, master_account_id: str, challenge_id: str, starting_equity: float, supabase_client: Any,
) -> dict:
    """starting_equity is the master's real live equity at the moment of
    successful payment (resolved by payments/webhooks.py, which has access
    to the running agent or falls back to the last known
    live_account_state row) - it's the baseline every subsequent
    profit/drawdown/daily-loss calculation in challenge_watcher.py is
    computed against. This challenge runs on the master's own real
    account, not a dedicated evaluation account - "account size" on the
    challenge itself is now just a cosmetic reward-tier label."""
    challenge = _get_challenge(challenge_id, supabase_client)
    _assert_can_enroll(master_account_id, challenge, supabase_client)

    today_iso = datetime.now(timezone.utc).date().isoformat()
    row = {
        "master_account_id": master_account_id,
        "challenge_id": challenge_id,
        "status": "enrolled",
        "starting_equity": starting_equity,
        "peak_equity": starting_equity,
        "day_start_equity": starting_equity,
        "day_start_date": today_iso,
    }

    try:
        insert_response = execute_with_retry(
            lambda: supabase_client.table("master_challenge_enrollments").insert(row).execute()
        )
    except APIError as exc:
        if getattr(exc, "code", None) != _ENROLLMENT_UNIQUE_VIOLATION:
            raise

        # The earlier _assert_can_enroll() check is only a pre-check, not a lock -
        # a concurrent request (double-click, client retry, two near-simultaneous
        # calls) can pass it before either insert commits. idx_enrollments_one_active_
        # per_master is the real guard; recover from the race instead of letting a
        # 500 through. See roster.py's switch_master() for the same pattern.
        logger.warning(
            "master_challenge_enrollments insert raced for master %s / challenge %s - "
            "another request already created an active enrollment, recovering.",
            master_account_id, challenge_id,
        )
        winner = get_current_enrollment(master_account_id, supabase_client)
        if winner is None:
            raise ChallengeError(
                "Enrollment conflicted with a concurrent request and the resulting "
                "enrollment could not be found - please retry"
            ) from exc
        if winner["challenge_id"] != challenge_id:
            raise ChallengeError("Already enrolled in a challenge - leave it before enrolling in another") from exc
        from . import challenge_watcher  # local import - avoid a cycle, see below
        challenge_watcher.invalidate_cache(master_account_id)
        return winner

    logger.info(
        "Master %s enrolled in challenge %s (%s), starting_equity=%.2f",
        master_account_id, challenge_id, challenge["name"], starting_equity,
    )
    from . import challenge_watcher  # local import - challenge_watcher imports challenges, avoid a cycle
    challenge_watcher.invalidate_cache(master_account_id)
    return insert_response.data[0]


def leave(*, master_account_id: str, challenge_id: str, supabase_client: Any) -> dict:
    challenge = _get_challenge(challenge_id, supabase_client)
    if challenge["is_fixed"]:
        raise ChallengeError("Challenge 1 cannot be left - it's the mandatory prerequisite")

    current = get_current_enrollment(master_account_id, supabase_client)
    if current is None or current["challenge_id"] != challenge_id:
        raise ChallengeError("Not currently enrolled in this challenge")

    update_response = execute_with_retry(
        lambda: (
            supabase_client.table("master_challenge_enrollments")
            .update({"status": "left", "ended_at": "now()"})
            .eq("id", current["id"])
            .execute()
        )
    )
    logger.info("Master %s left challenge %s", master_account_id, challenge_id)
    from . import challenge_watcher  # local import - avoid a cycle, see enroll() above
    challenge_watcher.invalidate_cache(master_account_id)
    return update_response.data[0]


def mark_enrollment_outcome(
    enrollment_id: str, status: Literal["passed", "breached", "failed"], supabase_client: Any,
    *, breach_reason: str | None = None,
) -> dict | None:
    """Called exclusively by challenge_watcher.py once it determines an
    enrolled attempt has hit a terminal outcome. Guarded by
    .eq("status", "enrolled") so a slow/duplicate watcher tick can never
    flip an already-terminal enrollment a second time. On "passed", the
    challenge's reward_amount (if any) is auto-credited to the master's
    wallet the same way record_challenge_reward() below already does -
    reward_text (the human-fulfilled part, e.g. "featured directory slot")
    is NOT auto-applied here, that's on the admin to action manually."""
    update: dict[str, Any] = {"status": status, "ended_at": "now()"}
    if breach_reason:
        update["breach_reason"] = breach_reason

    response = execute_with_retry(
        lambda: (
            supabase_client.table("master_challenge_enrollments")
            .update(update)
            .eq("id", enrollment_id)
            .eq("status", "enrolled")
            .execute()
        )
    )
    rows = response.data or []
    if not rows:
        logger.warning("mark_enrollment_outcome(%s, %s): no longer 'enrolled', skipping - already resolved", enrollment_id, status)
        return None
    enrollment = rows[0]

    logger.info(
        "Enrollment %s (master %s, challenge %s) -> %s%s",
        enrollment_id, enrollment["master_account_id"], enrollment["challenge_id"], status,
        f" ({breach_reason})" if breach_reason else "",
    )

    if status == "passed":
        challenge = _get_challenge(enrollment["challenge_id"], supabase_client)
        reward_amount = float(challenge.get("reward_amount") or 0)
        if reward_amount > 0:
            record_challenge_reward(
                master_account_id=enrollment["master_account_id"], challenge_id=enrollment["challenge_id"],
                amount=reward_amount, supabase_client=supabase_client,
            )

    return enrollment


def get_history(master_account_id: str, supabase_client: Any, limit: int = 100) -> dict:
    enrollments = execute_with_retry(
        lambda: (
            supabase_client.table("master_challenge_enrollments")
            .select("*")
            .eq("master_account_id", master_account_id)
            .order("enrolled_at", desc=True)
            .limit(limit)
            .execute()
        )
    ).data or []

    challenge_ids = {row["challenge_id"] for row in enrollments}
    challenge_names = {}
    for challenge_id in challenge_ids:
        try:
            challenge_names[challenge_id] = _get_challenge(challenge_id, supabase_client)["name"]
        except ChallengeError:
            challenge_names[challenge_id] = "(deleted challenge)"
    for row in enrollments:
        row["challenge_name"] = challenge_names.get(row["challenge_id"], "(unknown)")

    return {"master_account_id": master_account_id, "enrollments": enrollments}


def get_equity_curve(enrollment_id: str, supabase_client: Any) -> list[dict]:
    response = execute_with_retry(
        lambda: (
            supabase_client.table("challenge_equity_curve")
            .select("snapshot_date, equity")
            .eq("enrollment_id", enrollment_id)
            .order("snapshot_date")
            .execute()
        )
    )
    return response.data or []


def get_status(master_account_id: str, supabase_client: Any) -> dict:
    current = get_current_enrollment(master_account_id, supabase_client)
    return {
        "master_account_id": master_account_id,
        "phase": get_phase(master_account_id, supabase_client),
        "current_enrollment": current,
        "equity_curve": get_equity_curve(current["id"], supabase_client) if current else [],
    }


def record_challenge_reward(*, master_account_id: str, challenge_id: str, amount: float, supabase_client: Any) -> dict:
    insert_response = execute_with_retry(
        lambda: supabase_client.table("wallet_transactions").insert(
            {
                "account_id": master_account_id,
                "type": "challenge_reward",
                "amount": amount,
                "related_master_account_id": None,
                "related_deal_ticket": None,
            }
        ).execute()
    )
    logger.info("Challenge reward recorded: master %s, challenge %s, amount %.2f", master_account_id, challenge_id, amount)
    return insert_response.data[0]