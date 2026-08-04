from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from .base_agent import BaseAgent
from .socket_server import emit_account_state
from .supabase_client import execute_with_retry

if TYPE_CHECKING:
    from .fanout_core import FanoutCore

logger = logging.getLogger("live_state_publisher")

DEFAULT_INTERVAL_SECONDS = 0.01 


def _serialize_open_positions(agent: BaseAgent) -> list[dict[str, Any]]:
    positions = []
    for order_id, order in agent.terminal.open_orders.items():
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
        "balance": agent.terminal.account_info.get("balance"),
        "equity": agent.terminal.account_info.get("equity"),
        "open_positions": _serialize_open_positions(agent),
    }


def _write_live_state_row(supabase_client: Any, account_id: str, state: dict[str, Any]) -> None:
    try:
        execute_with_retry(
            lambda: supabase_client.table("live_account_state").upsert(
                {
                    "account_id": account_id,
                    "balance": state["balance"],
                    "equity": state["equity"],
                    "open_positions": state["open_positions"],
                },
                on_conflict="account_id",
            ).execute()
        )
    except Exception:
        logger.exception("Failed to write live_account_state row for %s", account_id)


async def run_live_state_publisher(
    fanout: "FanoutCore",
    account_user_map: dict[str, str],
    supabase_client: Any,
    interval_seconds: float = DEFAULT_INTERVAL_SECONDS,
) -> None:
    loop = asyncio.get_event_loop()

    while True:
        agents: dict[str, BaseAgent] = {**fanout.master_agents, **fanout.follower_agents}

        for account_id, agent in agents.items():
            try:
                if not agent.is_connected:
                    continue  

                user_id = account_user_map.get(account_id)
                if not user_id:
                    logger.warning("No user_id mapped for account %s - skipping publish", account_id)
                    continue
                state = _build_state(agent)

                try:
                    await emit_account_state(user_id, account_id, state)
                except Exception:
                    logger.exception("Socket emit failed for account %s", account_id)
                await loop.run_in_executor(None, _write_live_state_row, supabase_client, account_id, state)
            except Exception:
                logger.exception("Live-state publish tick failed for account %s", account_id)

        await asyncio.sleep(interval_seconds)