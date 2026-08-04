from __future__ import annotations

import logging
from typing import Any, Literal

from . import instance_pool
from .fanout_core import FanoutCore
from .follower_agent import FollowerAgent
from .supabase_client import execute_with_retry
from .terminal_agent import TerminalAgent

logger = logging.getLogger("account_lifecycle")

Role = Literal["master", "follower"]


class LifecycleError(Exception):
    """Raised for any failure here. Message is safe to surface to an API caller."""


def _get_agent(fanout: FanoutCore, account_id: str, role: Role) -> TerminalAgent | FollowerAgent:
    registry = fanout.master_agents if role == "master" else fanout.follower_agents
    agent = registry.get(account_id)
    if agent is None:
        raise LifecycleError(f"No running agent found for account {account_id} (role={role})")
    return agent


def _set_subscriptions_active(supabase_client: Any, *, account_id: str, role: Role, active: bool) -> None:
    column = "master_account_id" if role == "master" else "follower_account_id"
    execute_with_retry(
        lambda: supabase_client.table("subscriptions").update({"active": active}).eq(column, account_id).execute()
    )


def _reactivate_current_subscriptions(supabase_client: Any, *, account_id: str, role: Role) -> int:
    slot_column = "master_account_id" if role == "master" else "follower_account_id"
    current_slots = execute_with_retry(
        lambda: (
            supabase_client.table("roster_slots")
            .select("follower_account_id,master_account_id")
            .eq(slot_column, account_id)
            .eq("is_current", True)
            .execute()
        )
    )
    slot_rows = current_slots.data or []
    reactivated = 0
    for slot in slot_rows:
        execute_with_retry(
            lambda slot=slot: (
                supabase_client.table("subscriptions")
                .update({"active": True})
                .eq("follower_account_id", slot["follower_account_id"])
                .eq("master_account_id", slot["master_account_id"])
                .execute()
            )
        )
        reactivated += 1
    if not slot_rows:
        logger.info(
            "Resume: %s account %s has no current roster_slot - nothing to reactivate in subscriptions "
            "(account was never subscribed/subscribed-to, or was already fully switched away).",
            role, account_id,
        )
    return reactivated


def _force_close_all_fills(fanout: FanoutCore, account_id: str, role: Role) -> int:
    closed_count = 0

    if role == "follower":
        follower_agent = fanout.follower_agents.get(account_id)
        if follower_agent is None:
            return 0
        fills = fanout.pair_store.get_all_fills_for_follower(account_id)
        for (master_account_id, master_ticket), fill in fills.items():
            follower_agent.execute_close(follower_ticket=fill.ticket)
            logger.info(
                "Force-closed follower %s#%s (was copying master %s#%s) on pause/close",
                account_id, fill.ticket, master_account_id, master_ticket,
            )
            closed_count += 1
    else:
        pairs_for_this_master = {
            key: followers for key, followers in fanout.pair_store._pairs.items()  
            if key[0] == account_id
        }
        for (master_account_id, master_ticket), followers in pairs_for_this_master.items():
            for follower_account_id, fill in followers.items():
                follower_agent = fanout.follower_agents.get(follower_account_id)
                if follower_agent is None:
                    continue
                follower_agent.execute_close(follower_ticket=fill.ticket)
                logger.info(
                    "Force-closed follower %s#%s (was copying master %s#%s) on master pause/close",
                    follower_account_id, fill.ticket, master_account_id, master_ticket,
                )
                closed_count += 1

    return closed_count


def pause_account(
    *, account_id: str, role: Role, force_close: bool, fanout: FanoutCore, supabase_client: Any,
) -> dict:
    _get_agent(fanout, account_id, role)  
    _set_subscriptions_active(supabase_client, account_id=account_id, role=role, active=False)

    closed_count = _force_close_all_fills(fanout, account_id, role) if force_close else 0

    execute_with_retry(
        lambda: supabase_client.table("accounts").update({"status": "paused"}).eq("account_id", account_id).execute()
    )
    logger.info("Paused %s account %s (force_close=%s, closed %d fill(s))", role, account_id, force_close, closed_count)
    return {"account_id": account_id, "status": "paused", "closed_fills": closed_count}


def resume_account(*, account_id: str, role: Role, fanout: FanoutCore, supabase_client: Any) -> dict:
    _get_agent(fanout, account_id, role)
    reactivated = _reactivate_current_subscriptions(supabase_client, account_id=account_id, role=role)
    execute_with_retry(
        lambda: supabase_client.table("accounts").update({"status": "live"}).eq("account_id", account_id).execute()
    )
    logger.info("Resumed %s account %s (%d subscription row(s) reactivated)", role, account_id, reactivated)
    return {"account_id": account_id, "status": "live"}


def close_account(
    *, account_id: str, role: Role, fanout: FanoutCore, supabase_client: Any, agents: list,
) -> dict:
    agent = _get_agent(fanout, account_id, role)

    _set_subscriptions_active(supabase_client, account_id=account_id, role=role, active=False)
    closed_count = _force_close_all_fills(fanout, account_id, role)

    agent.stop()
    if role == "master":
        fanout.unregister_master(account_id)
    else:
        fanout.unregister_follower(account_id)
    if agent in agents:
        agents.remove(agent)

    # The terminal process itself is a pool instance and is never stopped - it keeps
    # running so it can be claimed by the next account. See instance_pool.py's module
    # docstring for why there's no "log out to blank" step here beyond deleting the
    # on-disk credentials and freeing the row: agent.stop() above already tore down the
    # only thing this backend was holding for this account (the worker process talking
    # to it), so there's nothing here that can go orphaned by a backend restart the way
    # a directly-owned terminal_process handle used to.
    instance_pool.release_instance(account_id=account_id, supabase_client=supabase_client)

    execute_with_retry(
        lambda: supabase_client.table("accounts").update({"status": "closed"}).eq("account_id", account_id).execute()
    )
    logger.info("Closed %s account %s (closed %d fill(s) first)", role, account_id, closed_count)
    return {"account_id": account_id, "status": "closed", "closed_fills": closed_count}