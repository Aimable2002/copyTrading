from __future__ import annotations

import logging
from typing import Any, Literal

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
    _set_subscriptions_active(supabase_client, account_id=account_id, role=role, active=True)
    execute_with_retry(
        lambda: supabase_client.table("accounts").update({"status": "live"}).eq("account_id", account_id).execute()
    )
    logger.info("Resumed %s account %s", role, account_id)
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

    terminal_process = getattr(agent, "terminal_process", None)
    if terminal_process is not None and terminal_process.poll() is None:
        terminal_process.terminate()
        logger.info("Terminated terminal process (pid %s) for %s", terminal_process.pid, account_id)
    else:
        logger.warning(
            "No live process handle for %s - if the backend restarted since this account was "
            "provisioned, its terminal process is now an orphan needing manual cleanup.",
            account_id,
        )

    execute_with_retry(
        lambda: supabase_client.table("accounts").update({"status": "closed"}).eq("account_id", account_id).execute()
    )
    logger.info("Closed %s account %s (closed %d fill(s) first)", role, account_id, closed_count)
    return {"account_id": account_id, "status": "closed", "closed_fills": closed_count}