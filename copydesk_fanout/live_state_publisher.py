"""
Throttled live-state publisher - reads each registered agent's current
balance/open positions and pushes them out on two paths:

  1. Socket.IO emit to that account's user room - instant, what the
     frontend actually renders from while connected.
  2. A write to the `live_account_state` Supabase table - not for the
     frontend to poll, but so a fresh page load / reconnect has an
     immediate last-known-state to paint before the next socket emit
     arrives, instead of a blank screen.

Runs as an asyncio background task inside the same event loop as the
Socket.IO server (started via sio.start_background_task in main.py), not a
separate thread - this keeps emit_account_state() calls on the loop that
actually owns the websocket connections, and lets the Supabase write use
run_in_executor so a slow network call never blocks the loop that's also
serving other agents' emits.

Reads agent.balance / agent.dwx.open_orders directly (see base_agent.py) -
these are the same in-memory attributes TerminalAgent/FollowerAgent already
maintain from DWX Connect's file polling, no new I/O against MT5 added
here. account_id -> user_id mapping comes from the `accounts` table, fetched
once at startup (see main.py) - accounts don't change ownership at runtime,
so no need to re-fetch this per tick.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from .base_agent import BaseAgent
from .socket_server import emit_account_state

if TYPE_CHECKING:
    from .fanout_core import FanoutCore

logger = logging.getLogger("live_state_publisher")

DEFAULT_INTERVAL_SECONDS = 1.0  # throttled on purpose - this is a display refresh rate, not the ~25ms trading poll rate


def _serialize_open_positions(agent: BaseAgent) -> list[dict[str, Any]]:
    positions = []
    for order_id, order in agent.dwx.open_orders.items():
        positions.append(
            {
                "ticket": order_id,
                "symbol": order.get("symbol"),
                "type": order.get("type"),
                "lots": order.get("lots"),
                "open_price": order.get("open_price"),
                "SL": order.get("SL"),
                "TP": order.get("TP"),
            }
        )
    return positions


def _build_state(agent: BaseAgent) -> dict[str, Any]:
    return {
        "balance": agent.dwx.account_info.get("balance"),
        "equity": agent.dwx.account_info.get("equity"),
        "open_positions": _serialize_open_positions(agent),
    }


def _write_live_state_row(supabase_client: Any, account_id: str, state: dict[str, Any]) -> None:
    try:
        supabase_client.table("live_account_state").upsert(
            {
                "account_id": account_id,
                "balance": state["balance"],
                "equity": state["equity"],
                "open_positions": state["open_positions"],
            },
            on_conflict="account_id",
        ).execute()
    except Exception:
        logger.exception("Failed to write live_account_state row for %s", account_id)


async def run_live_state_publisher(
    fanout: "FanoutCore",
    account_user_map: dict[str, str],
    supabase_client: Any,
    interval_seconds: float = DEFAULT_INTERVAL_SECONDS,
) -> None:
    """Runs forever. Reads fanout.master_agents / fanout.follower_agents
    fresh every tick (not a snapshot taken once at startup) so an agent
    registered after this loop starts - e.g. by a future dynamic
    orchestrator - gets picked up on the very next tick with no restart."""
    loop = asyncio.get_event_loop()

    while True:
        agents: dict[str, BaseAgent] = {**fanout.master_agents, **fanout.follower_agents}

        for account_id, agent in agents.items():
            if not agent.is_connected:
                continue  # EA hasn't written a first account_info payload yet - nothing to publish

            user_id = account_user_map.get(account_id)
            if not user_id:
                logger.warning("No user_id mapped for account %s - skipping publish", account_id)
                continue

            state = _build_state(agent)

            try:
                await emit_account_state(user_id, account_id, state)
            except Exception:
                logger.exception("Socket emit failed for account %s", account_id)

            # Supabase write is a blocking network call - offload it so it
            # can't stall the emit loop for every other account this tick.
            await loop.run_in_executor(None, _write_live_state_row, supabase_client, account_id, state)

        await asyncio.sleep(interval_seconds)