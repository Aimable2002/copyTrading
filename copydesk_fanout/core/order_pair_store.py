from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from .infra.supabase_client import execute_with_retry

logger = logging.getLogger("order_pair_store")


@dataclass
class PendingCopy:

    master_account_id: str
    master_ticket: str
    follower_account_id: str
    dispatched_lots: float
    created_at: float = field(default_factory=time.monotonic)


@dataclass
class PendingOrderCopy:

    master_account_id: str
    master_ticket: str
    follower_account_id: str
    follower_ticket: str
    dispatched_lots: float


@dataclass
class FollowerFill:

    ticket: str
    open_price: float
    order_type: str
    dispatched_lots: float
    current_lots: float  


class OrderPairStore:

    def __init__(self, supabase_client: Any | None = None) -> None:
        self._lock = threading.Lock()
        self._supabase = supabase_client
        self._pairs: dict[tuple[str, str], dict[str, FollowerFill]] = {}
        self._pending: dict[str, list[PendingCopy]] = {}
        self._pending_orders: dict[tuple[str, str], dict[str, PendingOrderCopy]] = {}

    # ------------------------------------------------------------------ #
    # Startup recovery
    # ------------------------------------------------------------------ #
    def rebuild_from_supabase(self) -> None:
        if self._supabase is None:
            logger.warning("rebuild_from_supabase called with no Supabase client configured - skipping")
            return

        pairs_response = execute_with_retry(
            lambda: self._supabase.table("order_pairs").select("*").eq("status", "open").execute()
        )
        pending_response = execute_with_retry(
            lambda: self._supabase.table("pending_copies").select("*").execute()
        )
        pending_orders_response = execute_with_retry(
            lambda: self._supabase.table("pending_order_pairs").select("*").execute()
        )

        with self._lock:
            self._pairs.clear()
            self._pending.clear()
            self._pending_orders.clear()

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

            for row in pending_orders_response.data or []:
                key = (row["master_account_id"], row["master_ticket"])
                self._pending_orders.setdefault(key, {})[row["follower_account_id"]] = PendingOrderCopy(
                    master_account_id=row["master_account_id"],
                    master_ticket=row["master_ticket"],
                    follower_account_id=row["follower_account_id"],
                    follower_ticket=row["follower_ticket"],
                    dispatched_lots=row["dispatched_lots"],
                )

        logger.info(
            "Rebuilt OrderPairStore from Supabase: %d open pair(s), %d pending copy(ies), %d resting pending order copy(ies)",
            len(pairs_response.data or []), len(pending_response.data or []), len(pending_orders_response.data or []),
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
            def _do():
                query = self._supabase.table(table).upsert(values, on_conflict=on_conflict) if on_conflict \
                    else self._supabase.table(table).insert(values)
                return query.execute()
            execute_with_retry(_do)
        except Exception:
            logger.exception("Supabase write-through to %s failed for %s", table, values)

    def _delete_pending_row(self, master_account_id: str, master_ticket: str, follower_account_id: str) -> None:
        if self._supabase is None:
            return
        try:
            execute_with_retry(
                lambda: (
                    self._supabase.table("pending_copies")
                    .delete()
                    .eq("master_account_id", master_account_id)
                    .eq("master_ticket", master_ticket)
                    .eq("follower_account_id", follower_account_id)
                    .execute()
                )
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
            execute_with_retry(
                lambda: (
                    self._supabase.table("order_pairs")
                    .update({"status": "closed"})
                    .eq("master_account_id", master_account_id)
                    .eq("master_ticket", master_ticket)
                    .eq("follower_account_id", follower_account_id)
                    .execute()
                )
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

    def remove_pending(self, master_account_id: str, master_ticket: str, follower_account_id: str) -> None:
        with self._lock:
            pending_list = self._pending.get(follower_account_id, [])
            self._pending[follower_account_id] = [
                p for p in pending_list if p.master_ticket != master_ticket
            ]
        self._delete_pending_row(master_account_id, master_ticket, follower_account_id)

    def add_pending_order(
        self, master_account_id: str, master_ticket: str, follower_account_id: str,
        follower_ticket: str, dispatched_lots: float,
    ) -> None:
        key = (master_account_id, master_ticket)
        with self._lock:
            self._pending_orders.setdefault(key, {})[follower_account_id] = PendingOrderCopy(
                master_account_id=master_account_id, master_ticket=master_ticket,
                follower_account_id=follower_account_id, follower_ticket=follower_ticket,
                dispatched_lots=dispatched_lots,
            )
        self._write_through(
            "pending_order_pairs",
            {
                "master_account_id": master_account_id,
                "master_ticket": master_ticket,
                "follower_account_id": follower_account_id,
                "follower_ticket": follower_ticket,
                "dispatched_lots": dispatched_lots,
            },
            on_conflict="master_account_id,master_ticket,follower_account_id",
        )

    def get_pending_order_followers(self, master_account_id: str, master_ticket: str) -> dict[str, PendingOrderCopy]:
        with self._lock:
            return dict(self._pending_orders.get((master_account_id, master_ticket), {}))

    def remove_pending_order(self, master_account_id: str, master_ticket: str, follower_account_id: str | None = None) -> None:
        key = (master_account_id, master_ticket)
        with self._lock:
            if follower_account_id is None:
                followers = list(self._pending_orders.pop(key, {}).keys())
            else:
                self._pending_orders.get(key, {}).pop(follower_account_id, None)
                followers = [follower_account_id]
        if self._supabase is None:
            return
        for fid in followers:
            try:
                execute_with_retry(
                    lambda fid=fid: (
                        self._supabase.table("pending_order_pairs")
                        .delete()
                        .eq("master_account_id", master_account_id)
                        .eq("master_ticket", master_ticket)
                        .eq("follower_account_id", fid)
                        .execute()
                    )
                )
            except Exception:
                logger.exception(
                    "Supabase pending_order_pairs delete failed for %s/%s/%s",
                    master_account_id, master_ticket, fid,
                )

    def confirm_fill(
        self, follower_account_id: str, master_ticket: str, follower_ticket: str, open_price: float, order_type: str
    ) -> str | None:
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
            return None

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
        return master_account_id

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
        for follower_account_id in followers:
            self._mark_pair_closed(master_account_id, master_ticket, follower_account_id)

    def expire_stale_pending(self, max_age_seconds: float = 60.0) -> list[PendingCopy]:
        expired: list[PendingCopy] = []
        now = time.monotonic()
        with self._lock:
            for follower_account_id, pending_list in list(self._pending.items()):
                still_pending = []
                for pending in pending_list:
                    is_resting_pending_order = any(
                        follower_account_id in followers
                        for (m_id, m_ticket), followers in self._pending_orders.items()
                        if m_id == pending.master_account_id and m_ticket == pending.master_ticket
                    )
                    if is_resting_pending_order:
                        still_pending.append(pending)
                    elif now - pending.created_at > max_age_seconds:
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

    def was_follower_ticket_copied(self, follower_account_id: str, follower_ticket: str) -> bool:
        if self._supabase is None:
            return False
        response = execute_with_retry(
            lambda: (
                self._supabase.table("order_pairs")
                .select("follower_ticket")
                .eq("follower_account_id", follower_account_id)
                .eq("follower_ticket", follower_ticket)
                .limit(1)
                .execute()
            )
        )
        return bool(response.data)

    def find_master_ticket_by_follower_ticket(self, follower_account_id: str, follower_ticket: str) -> str | None:
        with self._lock:
            for (master_account_id, master_ticket), followers in self._pairs.items():
                fill = followers.get(follower_account_id)
                if fill and fill.ticket == follower_ticket:
                    return master_ticket
            return None

    def get_open_master_tickets(self, master_account_id: str) -> set[str]:
        with self._lock:
            return {mt for (macc, mt) in self._pairs.keys() if macc == master_account_id}

    def get_expected_follower_tickets(self, follower_account_id: str) -> set[str]:
        with self._lock:
            return {
                fill.ticket
                for followers in self._pairs.values()
                for acc, fill in followers.items()
                if acc == follower_account_id
            }

    def get_all_fills_for_follower(self, follower_account_id: str) -> dict[tuple[str, str], FollowerFill]:
        with self._lock:
            return {
                key: followers[follower_account_id]
                for key, followers in self._pairs.items()
                if follower_account_id in followers
            }