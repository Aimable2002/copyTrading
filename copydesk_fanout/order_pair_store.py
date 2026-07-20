from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger("order_pair_store")


@dataclass
class PendingCopy:
    """A copy that's been dispatched to a follower but not yet confirmed filled
    (DWX Connect's file bridge is async - we don't get the follower's ticket
    back synchronously when placing an order)."""

    master_account_id: str
    master_ticket: str
    follower_account_id: str
    dispatched_lots: float
    created_at: float = field(default_factory=time.monotonic)


@dataclass
class FollowerFill:
    """A confirmed follower fill - everything needed later to reapply
    distance-based SL/TP against this follower's REAL entry price, and to
    compute proportional partial closes against what we actually told this
    follower to open (not the master's lot size, which differs)."""

    ticket: str
    open_price: float
    order_type: str
    dispatched_lots: float
    current_lots: float  # updated on each partial close we execute


class OrderPairStore:
    """
    In-memory dict is still the hot-path read/write target - zero I/O on
    every copy decision, unchanged from the original design. What changed:
    Supabase (order_pairs / pending_copies tables, see supabase/schema.sql)
    is now the source of truth behind that cache. Every mutation here also
    writes through to Supabase; on startup, rebuild_from_supabase()
    repopulates the in-memory dicts from those tables instead of starting
    empty. This closes the original gap noted in this class's earlier
    docstring: "state is lost on restart."

    A Supabase client is optional at construction time on purpose - existing
    callers (e.g. local/offline testing per the original isolated-testing
    README) can still build this with no client and get the exact old
    in-memory-only behavior; write-throughs are just skipped in that case.
    """

    def __init__(self, supabase_client: Any | None = None) -> None:
        self._lock = threading.Lock()
        self._supabase = supabase_client
        # (master_account_id, master_ticket) -> {follower_account_id: FollowerFill}
        self._pairs: dict[tuple[str, str], dict[str, FollowerFill]] = {}
        # follower_account_id -> [PendingCopy, ...] awaiting fill confirmation
        self._pending: dict[str, list[PendingCopy]] = {}

    # ------------------------------------------------------------------ #
    # Startup recovery
    # ------------------------------------------------------------------ #
    def rebuild_from_supabase(self) -> None:
        """Call once at backend startup, before agents start. Repopulates
        in-memory state from Supabase so a restart with open copied trades
        doesn't lose the master<->follower ticket pairings a later close/
        modify/partial-close on the master needs to propagate correctly."""
        if self._supabase is None:
            logger.warning("rebuild_from_supabase called with no Supabase client configured - skipping")
            return

        pairs_response = self._supabase.table("order_pairs").select("*").eq("status", "open").execute()
        pending_response = self._supabase.table("pending_copies").select("*").execute()

        with self._lock:
            self._pairs.clear()
            self._pending.clear()

            for row in pairs_response.data or []:
                key = (row["master_account_id"], row["master_ticket"])
                self._pairs.setdefault(key, {})[row["follower_account_id"]] = FollowerFill(
                    ticket=row["follower_ticket"],
                    open_price=row["open_price"],
                    order_type=row["order_type"],
                    dispatched_lots=row["dispatched_lots"],
                    current_lots=row["current_lots"],
                )

            for row in pending_response.data or []:
                self._pending.setdefault(row["follower_account_id"], []).append(
                    PendingCopy(
                        master_account_id=row["master_account_id"],
                        master_ticket=row["master_ticket"],
                        follower_account_id=row["follower_account_id"],
                        dispatched_lots=row["dispatched_lots"],
                    )
                )

        logger.info(
            "Rebuilt OrderPairStore from Supabase: %d open pair(s), %d pending copy(ies)",
            len(pairs_response.data or []), len(pending_response.data or []),
        )

    # ------------------------------------------------------------------ #
    # Write-through helpers - best-effort, never raise into the caller.
    # A failed write-through means the next restart's rebuild may be
    # slightly stale, not that the live in-memory copy operation fails;
    # the in-memory dict (the hot path) is always updated regardless.
    # ------------------------------------------------------------------ #
    def _write_through(self, table: str, values: dict[str, Any], on_conflict: str | None = None) -> None:
        if self._supabase is None:
            return
        try:
            query = self._supabase.table(table).upsert(values, on_conflict=on_conflict) if on_conflict \
                else self._supabase.table(table).insert(values)
            query.execute()
        except Exception:
            logger.exception("Supabase write-through to %s failed for %s", table, values)

    def _delete_pending_row(self, master_account_id: str, master_ticket: str, follower_account_id: str) -> None:
        if self._supabase is None:
            return
        try:
            (
                self._supabase.table("pending_copies")
                .delete()
                .eq("master_account_id", master_account_id)
                .eq("master_ticket", master_ticket)
                .eq("follower_account_id", follower_account_id)
                .execute()
            )
        except Exception:
            logger.exception(
                "Supabase pending_copies delete failed for %s/%s/%s",
                master_account_id, master_ticket, follower_account_id,
            )

    def _mark_pair_closed(self, master_account_id: str, master_ticket: str, follower_account_id: str) -> None:
        if self._supabase is None:
            return
        try:
            (
                self._supabase.table("order_pairs")
                .update({"status": "closed"})
                .eq("master_account_id", master_account_id)
                .eq("master_ticket", master_ticket)
                .eq("follower_account_id", follower_account_id)
                .execute()
            )
        except Exception:
            logger.exception(
                "Supabase order_pairs close-status update failed for %s/%s/%s",
                master_account_id, master_ticket, follower_account_id,
            )

    # ------------------------------------------------------------------ #
    # Existing API - same signatures/behavior, now with write-through
    # ------------------------------------------------------------------ #
    def add_pending(self, master_account_id: str, master_ticket: str, follower_account_id: str, dispatched_lots: float) -> None:
        with self._lock:
            self._pending.setdefault(follower_account_id, []).append(
                PendingCopy(master_account_id, master_ticket, follower_account_id, dispatched_lots)
            )
        self._write_through(
            "pending_copies",
            {
                "master_account_id": master_account_id,
                "master_ticket": master_ticket,
                "follower_account_id": follower_account_id,
                "dispatched_lots": dispatched_lots,
            },
        )

    def confirm_fill(
        self, follower_account_id: str, master_ticket: str, follower_ticket: str, open_price: float, order_type: str
    ) -> bool:
        """Call this once a follower's new order is observed with a comment
        tag matching master_ticket. Moves it from pending to confirmed."""
        master_account_id: str | None = None
        dispatched_lots: float | None = None

        with self._lock:
            pending_list = self._pending.get(follower_account_id, [])
            for i, pending in enumerate(pending_list):
                if pending.master_ticket == master_ticket:
                    key = (pending.master_account_id, pending.master_ticket)
                    self._pairs.setdefault(key, {})[follower_account_id] = FollowerFill(
                        ticket=follower_ticket,
                        open_price=open_price,
                        order_type=order_type,
                        dispatched_lots=pending.dispatched_lots,
                        current_lots=pending.dispatched_lots,
                    )
                    master_account_id = pending.master_account_id
                    dispatched_lots = pending.dispatched_lots
                    del pending_list[i]
                    break

        if master_account_id is None:
            return False

        # Write-through happens outside the lock - these are network calls
        # and must not hold up the hot in-memory path for other threads.
        self._delete_pending_row(master_account_id, master_ticket, follower_account_id)
        self._write_through(
            "order_pairs",
            {
                "master_account_id": master_account_id,
                "master_ticket": master_ticket,
                "follower_account_id": follower_account_id,
                "follower_ticket": follower_ticket,
                "order_type": order_type,
                "open_price": open_price,
                "dispatched_lots": dispatched_lots,
                "current_lots": dispatched_lots,
                "status": "open",
            },
            on_conflict="master_account_id,master_ticket,follower_account_id",
        )
        return True

    def get_follower_fills(self, master_account_id: str, master_ticket: str) -> dict[str, FollowerFill]:
        with self._lock:
            return dict(self._pairs.get((master_account_id, master_ticket), {}))

    def record_partial_close(self, master_account_id: str, master_ticket: str, follower_account_id: str, new_lots: float) -> None:
        with self._lock:
            fill = self._pairs.get((master_account_id, master_ticket), {}).get(follower_account_id)
            if fill:
                fill.current_lots = new_lots
        self._write_through(
            "order_pairs",
            {
                "master_account_id": master_account_id,
                "master_ticket": master_ticket,
                "follower_account_id": follower_account_id,
                "current_lots": new_lots,
            },
            on_conflict="master_account_id,master_ticket,follower_account_id",
        )

    def remove_master_trade(self, master_account_id: str, master_ticket: str) -> None:
        with self._lock:
            followers = self._pairs.pop((master_account_id, master_ticket), {})
        # Mark closed rather than delete - keeps a real audit trail of
        # completed copies instead of erasing history on every close.
        for follower_account_id in followers:
            self._mark_pair_closed(master_account_id, master_ticket, follower_account_id)

    def expire_stale_pending(self, max_age_seconds: float = 60.0) -> list[PendingCopy]:
        """Call periodically (see main.py's sweep task). A pending copy
        still unconfirmed after max_age_seconds almost always means the
        follower's EA rejected the order - MaximumOrders/MaximumLotSize
        reached, invalid symbol, broker reject, etc (see the SendError
        calls in mql/DWX_Server_MT5.mq5's OpenOrder()). Those errors don't
        carry the comment/master_ticket needed to correlate directly, so
        this timeout is the backstop that actually clears them rather than
        leaving a pending_copies row (and in-memory entry) orphaned
        forever. Returns what it expired, for logging/alerting by the caller."""
        expired: list[PendingCopy] = []
        now = time.monotonic()
        with self._lock:
            for follower_account_id, pending_list in list(self._pending.items()):
                still_pending = []
                for pending in pending_list:
                    if now - pending.created_at > max_age_seconds:
                        expired.append(pending)
                    else:
                        still_pending.append(pending)
                self._pending[follower_account_id] = still_pending

        for pending in expired:
            logger.warning(
                "Pending copy master %s#%s -> follower %s never confirmed after %.0fs - "
                "dropping it, likely rejected by the follower's EA (check that terminal's "
                "Experts log)",
                pending.master_account_id, pending.master_ticket, pending.follower_account_id, max_age_seconds,
            )
            self._delete_pending_row(pending.master_account_id, pending.master_ticket, pending.follower_account_id)

        return expired

    def find_master_ticket_by_follower_ticket(self, follower_account_id: str, follower_ticket: str) -> str | None:
        """Used when a follower's copied position gets closed - need to know
        which master trade it belonged to, e.g. for logging/cleanup."""
        with self._lock:
            for (master_account_id, master_ticket), followers in self._pairs.items():
                fill = followers.get(follower_account_id)
                if fill and fill.ticket == follower_ticket:
                    return master_ticket
            return None

    def get_all_fills_for_follower(self, follower_account_id: str) -> dict[tuple[str, str], FollowerFill]:
        """Every currently-open pair this follower is part of, keyed by
        (master_account_id, master_ticket) - used by account_lifecycle.py's
        force-close-on-pause path."""
        with self._lock:
            return {
                key: followers[follower_account_id]
                for key, followers in self._pairs.items()
                if follower_account_id in followers
            }
            