from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .sizing import SizingMode
from ..infra.supabase_client import execute_with_retry, async_execute_with_retry, get_async_supabase_client

logger = logging.getLogger("config_store")

_INITIAL_RECONNECT_DELAY_SECONDS = 1.0
_MAX_RECONNECT_DELAY_SECONDS = 30.0
_HEALTHY_CONNECTION_SECONDS = 60.0


@dataclass
class FollowerSubscription:
    follower_account_id: str
    multiplier: float
    sizing_mode: SizingMode
    fixed_master_balance: float | None = None
    active: bool = True


class ConfigStore:
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
        grouped: dict[str, list[FollowerSubscription]] = {}
        for row in rows:
            grouped.setdefault(row["master_account_id"], []).append(self._row_to_subscription(row))
        for master_account_id, followers in grouped.items():
            self.set_config(master_account_id, followers)

    def load_from_supabase(self, supabase_client: Any) -> None:

        response = execute_with_retry(
            lambda: supabase_client.table("subscriptions").select("*").eq("active", True).execute()
        )
        self._apply_rows(response.data or [])
        logger.info("Loaded %d active subscriptions from Supabase", len(response.data or []))

    def start_realtime_sync(self) -> threading.Thread:
        thread = threading.Thread(target=self._run_realtime_loop, name="config-realtime-sync", daemon=True)
        thread.start()
        return thread

    def _run_realtime_loop(self) -> None:
        asyncio.run(self._realtime_sync_forever())

    async def _realtime_sync_forever(self) -> None:
        delay = _INITIAL_RECONNECT_DELAY_SECONDS
        while True:
            started_at = time.monotonic()
            try:
                await self._realtime_sync_coro()
                logger.warning("Realtime config sync coroutine exited unexpectedly - reconnecting")
            except Exception:
                logger.exception(
                    "Realtime config sync connection lost - config cache stays on "
                    "last-known values until reconnected"
                )

            if time.monotonic() - started_at >= _HEALTHY_CONNECTION_SECONDS:
                delay = _INITIAL_RECONNECT_DELAY_SECONDS
            else:
                delay = min(delay * 2, _MAX_RECONNECT_DELAY_SECONDS)

            logger.info("Realtime config sync: reconnecting in %.1fs", delay)
            await asyncio.sleep(delay)

    async def _realtime_sync_coro(self) -> None:

        client = await get_async_supabase_client()

        initial = await async_execute_with_retry(
            lambda: client.table("subscriptions").select("*").eq("active", True).execute()
        )
        self._apply_rows(initial.data or [])
        logger.info("Realtime config sync: initial load of %d active subscriptions", len(initial.data or []))

        async def on_change(payload: dict[str, Any]) -> None:
            data = payload.get("data") or {}
            record = data.get("record") or data.get("old_record") or {}
            master_account_id = record.get("master_account_id")
            if not master_account_id:
                logger.warning("Realtime config change with no master_account_id in payload: %s", payload)
                return

            refreshed = await async_execute_with_retry(
                lambda: (
                    client.table("subscriptions")
                    .select("*")
                    .eq("master_account_id", master_account_id)
                    .eq("active", True)
                    .execute()
                )
            )
            followers = [self._row_to_subscription(row) for row in (refreshed.data or [])]
            self.set_config(master_account_id, followers)
            logger.info(
                "Realtime config sync: %s now has %d active follower(s)",
                master_account_id, len(followers),
            )

        def on_change_scheduled(payload: dict[str, Any]) -> None:
            asyncio.create_task(on_change(payload))

        channel = client.channel("subscriptions-sync")
        channel.on_postgres_changes("INSERT", schema="public", table="subscriptions", callback=on_change_scheduled)
        channel.on_postgres_changes("UPDATE", schema="public", table="subscriptions", callback=on_change_scheduled)
        channel.on_postgres_changes("DELETE", schema="public", table="subscriptions", callback=on_change_scheduled)
        await channel.subscribe()

        logger.info("Realtime config sync: subscribed to subscriptions table changes")

        await asyncio.Event().wait()