"""Curated cTrader Copy strategies shown alongside our own provisioned
masters in the directory - but a fundamentally different kind of listing.

A provisioned master (master_profiles.py) is an account WE connected: we
hold a login/OAuth session for it, an agent watches it, and when it trades
fanout_core.py copies that onto every subscribed follower ourselves. We own
that whole pipeline end to end.

An embedded strategy is someone else's cTrader Copy strategy, already
registered inside cTrader's own marketplace product, that we've chosen to
list here because it looked good on cTraderStore. We hold no connection to
it - no agent, no login, no ctid. When someone "starts copying" one, that
entire relationship - the investment, the fee, the actual trade mirroring -
happens inside cTrader Copy itself, not through us. Our involvement stops at
"we showed you the card and embedded the widget." See the embed_code field's
comment in migration_embedded_strategies.sql for exactly what that is.

This is also why /masters/directory has to tag each entry with a `source`
field (see list_directory() below) rather than returning one undifferentiated
list - the frontend needs to know which of these two completely different
things it's rendering a card for, because "start copy" means something
different for each: a provisioned master goes through our own
subscribe/checkout flow; an embedded strategy requires the follower to
already have their own cTrader account (cTrader Copy's own hard requirement,
not ours) before the widget can do anything for them at all.

Curation is manual by design, not a gap to eventually close - cTrader has no
API to list or search Copy strategies (verified: nothing in the Open API's
protobuf schema references Copy at all) and no API to generate a widget
embed code programmatically. An admin has to go find a strategy on
cTraderStore, generate its embed code by hand in cTrader Web's Widgets app,
and paste it in here. That's not a v1 limitation to automate away later -
there is nothing on cTrader's side to automate against.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from ..infra.supabase_client import execute_with_retry

logger = logging.getLogger("embedded_strategies")


class EmbeddedStrategyError(Exception):
    """Raised for any failure here. Message is safe to surface to an API caller."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def add_strategy(
    *, strategy_id: str, display_name: str, bio: str, embed_code: str, added_by_user_id: str,
    cached_roi_percent: float | None, cached_max_drawdown_pct: float | None, supabase_client: Any,
) -> dict:
    strategy_id = strategy_id.strip()
    display_name = display_name.strip()
    embed_code = embed_code.strip()
    if not strategy_id:
        raise EmbeddedStrategyError("strategy_id cannot be empty")
    if not display_name:
        raise EmbeddedStrategyError("display_name cannot be empty")
    if not embed_code:
        raise EmbeddedStrategyError("embed_code cannot be empty")
    if "<script" not in embed_code and "<div" not in embed_code:
        # Cheap sanity check, not real validation - cTrader's own embed
        # snippets are always a <div> placeholder plus a <script> tag (see
        # the docs example in migration_embedded_strategies.sql's comment).
        # Catches "pasted the wrong thing" (a plain URL, empty paste) before
        # it ever reaches a follower's page, without trying to fully parse
        # or sanitize third-party HTML here.
        raise EmbeddedStrategyError(
            "embed_code doesn't look like a cTrader widget snippet (expected a <div>/<script> pair "
            "from cTrader Web's Widgets app, not a plain link)"
        )

    try:
        response = execute_with_retry(
            lambda: (
                supabase_client.table("embedded_strategies")
                .insert(
                    {
                        "strategy_id": strategy_id,
                        "display_name": display_name,
                        "bio": bio.strip(),
                        "embed_code": embed_code,
                        "cached_roi_percent": cached_roi_percent,
                        "cached_max_drawdown_pct": cached_max_drawdown_pct,
                        "added_by_user_id": added_by_user_id,
                        "is_active": True,
                    }
                )
                .execute()
            )
        )
    except Exception as exc:
        message = str(exc).lower()
        if "duplicate key" in message or "23505" in message:
            raise EmbeddedStrategyError(f"Strategy {strategy_id} is already listed") from exc
        raise
    return response.data[0]


def update_strategy(strategy_row_id: str, updates: dict, supabase_client: Any) -> dict:
    allowed_fields = {
        "display_name", "bio", "embed_code", "cached_roi_percent", "cached_max_drawdown_pct", "is_active",
    }
    payload = {k: v for k, v in updates.items() if k in allowed_fields}
    if not payload:
        raise EmbeddedStrategyError("No updatable fields provided")
    payload["updated_at"] = _now_iso()

    response = execute_with_retry(
        lambda: (
            supabase_client.table("embedded_strategies").update(payload).eq("id", strategy_row_id).execute()
        )
    )
    rows = response.data or []
    if not rows:
        raise EmbeddedStrategyError(f"No embedded strategy with id {strategy_row_id}")
    return rows[0]


def remove_strategy(strategy_row_id: str, supabase_client: Any) -> None:
    # Soft delete (is_active=False) rather than a hard delete - a strategy
    # that gets pulled from the directory shouldn't break for a follower who
    # already has that widget open in a page they haven't refreshed, and
    # keeps admin history of what was ever listed.
    response = execute_with_retry(
        lambda: (
            supabase_client.table("embedded_strategies")
            .update({"is_active": False, "updated_at": _now_iso()})
            .eq("id", strategy_row_id)
            .execute()
        )
    )
    if not response.data:
        raise EmbeddedStrategyError(f"No embedded strategy with id {strategy_row_id}")


def list_active_strategies(supabase_client: Any) -> list[dict]:
    response = execute_with_retry(
        lambda: (
            supabase_client.table("embedded_strategies")
            .select("id, strategy_id, display_name, bio, embed_code, cached_roi_percent, cached_max_drawdown_pct")
            .eq("is_active", True)
            .execute()
        )
    )
    return response.data or []


def list_all_strategies(supabase_client: Any) -> list[dict]:
    """Admin-only view: every row regardless of is_active, same reasoning as
    master_profiles.list_all_masters - deliberately separate from
    list_active_strategies rather than adding a flag to it, since that
    function's contract (active only) is depended on by the public
    directory."""
    response = execute_with_retry(
        lambda: supabase_client.table("embedded_strategies").select("*").order("created_at", desc=True).execute()
    )
    return response.data or []