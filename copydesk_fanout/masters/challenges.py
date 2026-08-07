from __future__ import annotations

import logging
from typing import Any

from postgrest.exceptions import APIError

from .infra.supabase_client import execute_with_retry

logger = logging.getLogger("challenges")

_ENROLLMENT_UNIQUE_VIOLATION = "23505"


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


def enroll(*, master_account_id: str, challenge_id: str, supabase_client: Any) -> dict:
    challenge = _get_challenge(challenge_id, supabase_client)
    if not challenge["active"]:
        raise ChallengeError("This challenge is not currently active")

    if get_current_enrollment(master_account_id, supabase_client) is not None:
        raise ChallengeError("Already enrolled in a challenge - leave it before enrolling in another")

    if not challenge["is_fixed"] and not has_passed_challenge_one(master_account_id, supabase_client):
        raise ChallengeError("Challenge 1 must be passed before enrolling in any other challenge")

    try:
        insert_response = execute_with_retry(
            lambda: supabase_client.table("master_challenge_enrollments").insert(
                {"master_account_id": master_account_id, "challenge_id": challenge_id, "status": "enrolled"}
            ).execute()
        )
    except APIError as exc:
        if getattr(exc, "code", None) != _ENROLLMENT_UNIQUE_VIOLATION:
            raise

        # The earlier get_current_enrollment() check is only a pre-check, not a lock -
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
        return winner

    logger.info("Master %s enrolled in challenge %s (%s)", master_account_id, challenge_id, challenge["name"])
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
    return update_response.data[0]


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
    results = execute_with_retry(
        lambda: (
            supabase_client.table("monthly_challenge_results")
            .select("*")
            .eq("master_account_id", master_account_id)
            .order("period", desc=True)
            .limit(limit)
            .execute()
        )
    ).data or []

    challenge_ids = {row["challenge_id"] for row in enrollments} | {row["challenge_id"] for row in results}
    challenge_names = {}
    for challenge_id in challenge_ids:
        try:
            challenge_names[challenge_id] = _get_challenge(challenge_id, supabase_client)["name"]
        except ChallengeError:
            challenge_names[challenge_id] = "(deleted challenge)"
    for row in enrollments:
        row["challenge_name"] = challenge_names.get(row["challenge_id"], "(unknown)")
    for row in results:
        row["challenge_name"] = challenge_names.get(row["challenge_id"], "(unknown)")

    return {"master_account_id": master_account_id, "enrollments": enrollments, "monthly_results": results}


def get_status(master_account_id: str, supabase_client: Any) -> dict:
    return {
        "master_account_id": master_account_id,
        "phase": get_phase(master_account_id, supabase_client),
        "current_enrollment": get_current_enrollment(master_account_id, supabase_client),
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