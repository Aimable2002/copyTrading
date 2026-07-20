"""
Entrypoint for the fanout backend.

Two modes, chosen automatically by --config vs --supabase:

  Local file mode (original isolated-testing behavior, unchanged):
      python -m copydesk_fanout.main --config copydesk_fanout/config.json
    Reads accounts/subscriptions from a local JSON file once at startup.
    No Supabase project needed. OrderPairStore runs in-memory only - state
    is lost on restart, same as the original first cut. Useful for a quick
    local test against 2 terminals without any backend infra set up.

  Supabase mode (production path):
      python -m copydesk_fanout.main --supabase
    Requires SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in the environment
    (see .env.example). Accounts come from the `accounts` table (status =
    'live'), subscriptions sync continuously via ConfigStore's Realtime
    listener, and OrderPairStore write-throughs/rebuilds against Supabase
    so an open copied trade survives a backend restart.

Regardless of mode: each master/follower account must already be logged
into its own running MT5 terminal with DWX_Server_MT5.mq5 attached to a
chart (per the official dwxconnect README, or done automatically by the
provisioning service in Supabase mode) before starting this.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path

from .config_store import ConfigStore
from .fanout_core import FanoutCore
from .follower_agent import FollowerAgent
from .order_pair_store import OrderPairStore
from .terminal_agent import TerminalAgent

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("main")

# Supabase's client logs every single HTTP call at INFO - noise, not signal.
# Our own loggers (main, fanout_core, order_pair_store, config_store) are
# untouched by this and keep printing normally.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


def _run_local_file_mode(config_path: Path) -> None:
    if not config_path.exists():
        logger.error(
            "Config file not found: %s. Copy config.example.json to config.json and fill in your "
            "terminal paths and subscriptions first.", config_path,
        )
        return

    raw = json.loads(config_path.read_text())

    config_store = ConfigStore()
    config_store.load_from_file(config_path)  # reads the "masters" section for subscriptions

    pair_store = OrderPairStore()  # no Supabase client -> in-memory only, as before
    fanout = FanoutCore(config_store, pair_store)

    agents: list[TerminalAgent] = []
    for master_cfg in raw["accounts"]["masters"]:
        agent = TerminalAgent(
            account_id=master_cfg["account_id"],
            metatrader_dir_path=master_cfg["metatrader_dir_path"],
            on_trade_event=fanout.handle_master_trade_event,
        )
        fanout.register_master(agent)
        agents.append(agent)
        logger.info("Registered master: %s", master_cfg["account_id"])

    for follower_cfg in raw["accounts"]["followers"]:
        agent = FollowerAgent(
            account_id=follower_cfg["account_id"],
            metatrader_dir_path=follower_cfg["metatrader_dir_path"],
            on_trade_event=fanout.handle_follower_trade_event,
        )
        fanout.register_follower(agent)
        agents.append(agent)
        logger.info("Registered follower: %s", follower_cfg["account_id"])

    _run_agents(agents)


def _run_supabase_mode(serve: bool) -> None:
    # Local import: keeps `supabase` an optional dependency for local-file-mode-only use.
    from .supabase_client import get_supabase_client

    supabase = get_supabase_client()

    config_store = ConfigStore()
    config_store.load_from_supabase(supabase)  # full sync before anything starts dispatching
    config_store.start_realtime_sync()  # keeps syncing in the background for the process lifetime

    pair_store = OrderPairStore(supabase_client=supabase)
    pair_store.rebuild_from_supabase()  # recovers open pairings/pending copies from a prior run

    fanout = FanoutCore(config_store, pair_store)

    # 'paused' accounts still need a running, polled agent - see
    # account_lifecycle.py's docstring: pausing only flips
    # subscriptions.active to stop NEW copies, it never stops the agent
    # itself, so existing open fills keep receiving close/modify/partial
    # propagation. Only 'live' and 'paused' accounts should have agents
    # recreated on startup; 'provisioning'/'failed'/'stopped'/'closed'
    # accounts intentionally do not.
    accounts_response = (
        supabase.table("accounts").select("*").in_("status", ["live", "paused"]).execute()
    )
    accounts = accounts_response.data or []
    if not accounts:
        logger.warning(
            "No accounts with status in ('live', 'paused') found in Supabase - nothing to do "
            "until the provisioning service marks some accounts live."
        )

    agents: list[TerminalAgent] = []
    account_user_map: dict[str, str] = {}
    for account in accounts:
        account_user_map[account["account_id"]] = account["user_id"]
        if account["role"] == "master":
            agent = TerminalAgent(
                account_id=account["account_id"],
                metatrader_dir_path=account["metatrader_dir_path"],
                on_trade_event=fanout.handle_master_trade_event,
            )
            fanout.register_master(agent)
        else:
            agent = FollowerAgent(
                account_id=account["account_id"],
                metatrader_dir_path=account["metatrader_dir_path"],
                on_trade_event=fanout.handle_follower_trade_event,
            )
            fanout.register_follower(agent)
        agents.append(agent)
        logger.info("Registered %s: %s", account["role"], account["account_id"])

    # NOTE: this still only registers agents that were `live` at startup -
    # dynamically adding an agent when a new account goes live mid-run
    # (without restarting this process) is the orchestrator piece from the
    # build plan, not yet implemented here.

    if serve:
        _run_agents_with_server(agents, fanout, supabase, account_user_map, pair_store)
    else:
        _run_agents(agents)


def _run_agents_with_server(
    agents: list[TerminalAgent],
    fanout: FanoutCore,
    supabase,
    account_user_map: dict[str, str],
    pair_store: OrderPairStore,
) -> None:
    """Starts every agent's own background polling thread (non-blocking,
    same as _run_agents), then hands the main thread over to a single
    asyncio loop running: the combined Socket.IO + provisioning-REST ASGI
    app (uvicorn), the live-state publisher, and a periodic stale-pending
    sweep (order_pair_store.py's expire_stale_pending - cleans up copies
    the follower's EA rejected, since those errors can't be correlated
    directly). This is the process meant to sit behind the ngrok tunnel.
    Ctrl+C stops the server, the agents, and the sweep together.

    `agents` is a live, mutable list (not a snapshot) - api_server.py's
    /accounts/provision endpoint appends newly provisioned agents to this
    same list at runtime, so they get started/stopped alongside everything
    registered at process startup."""
    import asyncio
    import os

    import socketio as socketio_lib
    import uvicorn

    from .api_server import create_api_app
    from .live_state_publisher import run_live_state_publisher
    from .socket_server import sio

    for agent in agents:
        agent.start()
    logger.info("%d agent(s) started, bringing up API + Socket.IO server...", len(agents))

    api_app = create_api_app(
        fanout=fanout, supabase_client=supabase, account_user_map=account_user_map, agents=agents,
    )
    combined_app = socketio_lib.ASGIApp(sio, other_asgi_app=api_app)

    async def _stale_pending_sweep(interval_seconds: float = 30.0) -> None:
        loop = asyncio.get_event_loop()
        while True:
            await asyncio.sleep(interval_seconds)
            await loop.run_in_executor(None, pair_store.expire_stale_pending)

    async def _serve_async() -> None:
        port = int(os.environ.get("PORT", "8000"))
        config = uvicorn.Config(combined_app, host="0.0.0.0", port=port, log_level="info")
        server = uvicorn.Server(config)
        logger.info(
            "API + Socket.IO server listening on 0.0.0.0:%d (POST /accounts/provision, "
            "Socket.IO at /) - point `ngrok http %d` at this port", port, port,
        )
        publisher_task = asyncio.create_task(
            run_live_state_publisher(fanout, account_user_map, supabase)
        )
        sweep_task = asyncio.create_task(_stale_pending_sweep())
        await asyncio.gather(server.serve(), publisher_task, sweep_task)

    try:
        asyncio.run(_serve_async())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        for agent in agents:
            agent.stop()


def _run_agents(agents: list[TerminalAgent]) -> None:
    for agent in agents:
        agent.start()

    logger.info("Fanout backend running. Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        for agent in agents:
            agent.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="CopyDesk fanout backend (DWX Connect based)")
    parser.add_argument("--config", default=None, help="Path to local config JSON (offline/local-testing mode)")
    parser.add_argument("--supabase", action="store_true", help="Run in Supabase-backed production mode")
    parser.add_argument(
        "--serve", action="store_true",
        help="Also start the Socket.IO server (live account data for the frontend) in this process. "
             "Requires --supabase. Run this behind ngrok - see README_NGROK.md.",
    )
    args = parser.parse_args()

    if args.serve and not args.supabase:
        parser.error("--serve requires --supabase (the socket server needs the accounts/user mapping from Supabase)")

    if args.supabase:
        _run_supabase_mode(serve=args.serve)
    else:
        _run_local_file_mode(Path(args.config or "copydesk_fanout/config.json"))


if __name__ == "__main__":
    main()