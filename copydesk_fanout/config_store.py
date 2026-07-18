from __future__ import annotations

import asyncio
import json
import logging
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .sizing import SizingMode

logger = logging.getLogger("config_store")

# Supabase is imported lazily inside the methods that need it (rather than
# at module level) so this module still imports cleanly - and load_from_file
# still works - in environments that never installed the `supabase` package,
# e.g. quick local testing per the original isolated-testing README.


@dataclass
class FollowerSubscription:
    follower_account_id: str
    multiplier: float
    sizing_mode: SizingMode
    fixed_master_balance: float | None = None
    active: bool = True


class ConfigStore:
    """
    Holds master -> [follower subscriptions] in memory, read on every copy
    decision with zero I/O - that guarantee is unchanged.

    Supabase is the source of truth for subscriptions (see
    supabase/schema.sql). Two ways to populate this cache from it:

    - load_from_supabase(): one-shot full sync, call once at startup before
      the fanout core starts processing trade events.
    - start_realtime_sync(): spawns a background thread that keeps a
      Supabase Realtime subscription open on the `subscriptions` table and
      calls set_config() again for the affected master every time a row
      changes (multiplier edited, follower paused/resumed, new subscription
      added). Runs forever until the process exits; there is no clean
      shutdown path yet - fine for now since nothing else in this codebase
      has one either (see TerminalAgent/FollowerAgent .stop(), which only
      flips a flag).

    load_from_file() is kept as-is for local/offline testing without a
    Supabase project configured, per the original isolated-testing README.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._config: dict[str, list[FollowerSubscription]] = {}

    def set_config(self, master_account_id: str, followers: list[FollowerSubscription]) -> None:
        with self._lock:
            self._config[master_account_id] = followers

    def get_followers(self, master_account_id: str) -> list[FollowerSubscription]:
        with self._lock:
            return [f for f in self._config.get(master_account_id, []) if f.active]

    def load_from_file(self, path: str | Path) -> None:
        data = json.loads(Path(path).read_text())
        for master_account_id, followers_raw in data.get("masters", {}).items():
            followers = [
                FollowerSubscription(
                    follower_account_id=f["follower_account_id"],
                    multiplier=f["multiplier"],
                    sizing_mode=f["sizing_mode"],
                    fixed_master_balance=f.get("fixed_master_balance"),
                    active=f.get("active", True),
                )
                for f in followers_raw
            ]
            self.set_config(master_account_id, followers)

    # ------------------------------------------------------------------ #
    # Supabase-backed config sync
    # ------------------------------------------------------------------ #
    @staticmethod
    def _row_to_subscription(row: dict[str, Any]) -> FollowerSubscription:
        return FollowerSubscription(
            follower_account_id=row["follower_account_id"],
            multiplier=row["multiplier"],
            sizing_mode=row["sizing_mode"],
            fixed_master_balance=row.get("fixed_master_balance"),
            active=row.get("active", True),
        )

    def _apply_rows(self, rows: list[dict[str, Any]]) -> None:
        """Groups flat subscription rows by master_account_id and replaces
        this store's config for exactly those masters. Masters with zero
        active rows are set to an empty list rather than left untouched, so
        a subscription being deactivated actually clears it here too."""
        grouped: dict[str, list[FollowerSubscription]] = {}
        for row in rows:
            grouped.setdefault(row["master_account_id"], []).append(self._row_to_subscription(row))
        for master_account_id, followers in grouped.items():
            self.set_config(master_account_id, followers)

    def load_from_supabase(self, supabase_client: Any) -> None:
        """One-shot full sync from the `subscriptions` table. Call once at
        backend startup, before agents start dispatching trades, so
        get_followers() never returns stale/empty data for a real master."""
        response = supabase_client.table("subscriptions").select("*").eq("active", True).execute()
        self._apply_rows(response.data or [])
        logger.info("Loaded %d active subscriptions from Supabase", len(response.data or []))

    def start_realtime_sync(self) -> threading.Thread:
        """Starts a daemon thread that opens a Supabase Realtime connection
        and keeps this store in sync with the `subscriptions` table for the
        lifetime of the process. Returns the thread (already started) so the
        caller can decide whether to join it or just let it run alongside
        the agent threads."""
        thread = threading.Thread(target=self._run_realtime_loop, name="config-realtime-sync", daemon=True)
        thread.start()
        return thread

    def _run_realtime_loop(self) -> None:
        try:
            asyncio.run(self._realtime_sync_coro())
        except Exception:
            # A crashed sync thread must not be silent - the backend would
            # otherwise keep running on a frozen, increasingly stale config
            # cache with no indication anything is wrong.
            logger.exception("Supabase realtime config sync crashed - config is now frozen/stale")
            raise

    async def _realtime_sync_coro(self) -> None:
        # Local import: keeps `supabase` an optional dependency for anyone
        # only using load_from_file() for local testing.
        from .supabase_client import get_async_supabase_client

        client = await get_async_supabase_client()

        # Full sync before subscribing, so we're not relying on realtime's
        # delivery of every historical row - only on it for *changes* from
        # this point forward.
        initial = await client.table("subscriptions").select("*").eq("active", True).execute()
        self._apply_rows(initial.data or [])
        logger.info("Realtime config sync: initial load of %d active subscriptions", len(initial.data or []))

        async def on_change(payload: dict[str, Any]) -> None:
            # Re-fetch just the affected master's active followers rather
            # than trusting the payload's `record` directly - simplest way
            # to correctly handle DELETE (payload has no useful `record`)
            # and UPDATE-to-inactive (row disappears from an active-only
            # fetch) with one code path.
            record = payload.get("record") or payload.get("old_record") or {}
            master_account_id = record.get("master_account_id")
            if not master_account_id:
                logger.warning("Realtime config change with no master_account_id in payload: %s", payload)
                return

            refreshed = (
                await client.table("subscriptions")
                .select("*")
                .eq("master_account_id", master_account_id)
                .eq("active", True)
                .execute()
            )
            followers = [self._row_to_subscription(row) for row in (refreshed.data or [])]
            self.set_config(master_account_id, followers)
            logger.info(
                "Realtime config sync: %s now has %d active follower(s)",
                master_account_id, len(followers),
            )

        channel = client.channel("subscriptions-sync")
        channel.on_postgres_changes("INSERT", schema="public", table="subscriptions", callback=on_change)
        channel.on_postgres_changes("UPDATE", schema="public", table="subscriptions", callback=on_change)
        channel.on_postgres_changes("DELETE", schema="public", table="subscriptions", callback=on_change)
        await channel.subscribe()

        logger.info("Realtime config sync: subscribed to subscriptions table changes")

        # Keep this coroutine (and therefore the thread's event loop) alive
        # for the process lifetime - on_change fires via the realtime
        # client's own background listener, nothing else needed here.
        await asyncio.Event().wait()