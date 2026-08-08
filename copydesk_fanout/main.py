from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path

from .core.config_store import ConfigStore
from .core.fanout_core import FanoutCore
from .core.follower_agent import FollowerAgent
from .core.order_pair_store import OrderPairStore
from .core.terminal_agent import TerminalAgent

import os

from .infra.supabase_client import execute_with_retry, get_supabase_client
from .provisioning.provisioning import ProvisioningError, read_provisioned_credentials, resolve_instance_dir
from .ctrader.master_agent import CTraderMasterAgent

import asyncio

import socketio as socketio_lib
import uvicorn

from .billing import billing, weekly_charge
from .api.api_server import create_api_app
from .api.live_state_publisher import run_live_state_publisher
from .api.socket_server import sio

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("main")

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
    config_store.load_from_file(config_path)  

    pair_store = OrderPairStore() 
    fanout = FanoutCore(config_store, pair_store)

    agents: list[TerminalAgent] = []
    for master_cfg in raw["accounts"]["masters"]:
        agent = TerminalAgent(
            account_id=master_cfg["account_id"],
            terminal_path=master_cfg["terminal_path"],
            login=master_cfg["login"],
            password=master_cfg["password"],
            server=master_cfg["server"],
            on_trade_event=fanout.handle_master_trade_event,
        )
        fanout.register_master(agent)
        agents.append(agent)
        logger.info("Registered master: %s", master_cfg["account_id"])

    for follower_cfg in raw["accounts"]["followers"]:
        agent = FollowerAgent(
            account_id=follower_cfg["account_id"],
            terminal_path=follower_cfg["terminal_path"],
            login=follower_cfg["login"],
            password=follower_cfg["password"],
            server=follower_cfg["server"],
            on_trade_event=fanout.handle_follower_trade_event,
        )
        fanout.register_follower(agent)
        agents.append(agent)
        logger.info("Registered follower: %s", follower_cfg["account_id"])


    fanout.reconcile_all()

    _run_agents(agents)


def _run_supabase_mode(serve: bool) -> None:

    supabase = get_supabase_client()

    config_store = ConfigStore()
    config_store.load_from_supabase(supabase) 
    config_store.start_realtime_sync() 

    pair_store = OrderPairStore(supabase_client=supabase)
    pair_store.rebuild_from_supabase() 

    fanout = FanoutCore(config_store, pair_store)

    accounts_response = execute_with_retry(
        lambda: supabase.table("accounts").select("*").in_("status", ["live", "paused"]).execute()
    )
    accounts = accounts_response.data or []
    if not accounts:
        logger.warning(
            "No accounts with status in ('live', 'paused') found in Supabase - nothing to do "
            "until the provisioning service marks some accounts live."
        )

    agents: list[TerminalAgent] = []
    account_user_map: dict[str, str] = {}
    skipped_accounts: list[str] = []
    for account in accounts:
        account_user_map[account["account_id"]] = account["user_id"]

        # cTrader master accounts have no local terminal instance at all - the
        # "accounts" row for one has platform="ctrader" and no
        # metatrader_dir_path (see ctrader/provisioning.py). Everything below
        # this branch is MT5-instance-specific and doesn't apply to them.
        if account.get("platform") == "ctrader":
            agent = CTraderMasterAgent(
                account_id=account["account_id"],
                on_trade_event=fanout.handle_master_trade_event,
                supabase_client=supabase,
            )
            fanout.register_master(agent)
            agents.append(agent)
            logger.info("Registered ctrader master: %s", account["account_id"])
            continue

        stored_path = account["metatrader_dir_path"]
        instance_dir = resolve_instance_dir(stored_path)

        if not instance_dir.exists():
            logger.warning(
                "Skipping account %s (%s): instance dir not found on disk: %s. "
                "This account will not be polled until its instance dir is restored "
                "or the account is re-provisioned.",
                account["account_id"], account["role"], instance_dir,
            )
            account_user_map.pop(account["account_id"], None)
            skipped_accounts.append(account["account_id"])
            continue

        try:
            login, password, server = read_provisioned_credentials(instance_dir)
        except ProvisioningError as exc:
            logger.warning(
                "Skipping account %s (%s): %s",
                account["account_id"], account["role"], exc,
            )
            account_user_map.pop(account["account_id"], None)
            skipped_accounts.append(account["account_id"])
            continue

        exe_name = os.environ.get("TERMINAL_EXECUTABLE_NAME", "terminal64.exe")
        terminal_path = str(instance_dir / exe_name)

        if account["role"] == "master":
            agent = TerminalAgent(
                account_id=account["account_id"],
                terminal_path=terminal_path,
                login=int(login),
                password=password,
                server=server,
                on_trade_event=fanout.handle_master_trade_event,
            )
            fanout.register_master(agent)
        else:
            agent = FollowerAgent(
                account_id=account["account_id"],
                terminal_path=terminal_path,
                login=int(login),
                password=password,
                server=server,
                on_trade_event=fanout.handle_follower_trade_event,
            )
            fanout.register_follower(agent)
        agents.append(agent)
        logger.info("Registered %s: %s", account["role"], account["account_id"])

    if skipped_accounts:
        logger.warning(
            "Started up with %d account(s) skipped due to missing instance data: %s",
            len(skipped_accounts), ", ".join(skipped_accounts),
        )

    fanout.reconcile_all()

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
            try:
                await loop.run_in_executor(None, pair_store.expire_stale_pending)
            except Exception:
                logger.exception("Stale-pending sweep cycle failed - will retry next interval")

    async def _close_retry_sweep(interval_seconds: float = 2.0) -> None:
        loop = asyncio.get_event_loop()
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                await loop.run_in_executor(None, fanout.retry_failed_closes)
            except Exception:
                logger.exception("Close-retry sweep cycle failed - will retry next interval")

    async def _billing_grace_sweep(interval_seconds: float = 300.0) -> None:
        loop = asyncio.get_event_loop()
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                await loop.run_in_executor(None, billing.check_grace_expirations, fanout, supabase, agents)
            except Exception:
                logger.exception("Billing-grace sweep cycle failed - will retry next interval")

    async def _weekly_charge_sweep(interval_seconds: float = 1800.0) -> None:
        loop = asyncio.get_event_loop()
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                await loop.run_in_executor(None, lambda: weekly_charge.run_weekly_charge_cycle(fanout=fanout, supabase_client=supabase))
            except Exception:
                logger.exception("Weekly-charge sweep cycle failed - will retry next interval")

    def _log_background_task_result(name: str, task: "asyncio.Task") -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error("Background task %r exited unexpectedly and will NOT restart automatically - "
                         "this likely needs a process restart to recover: %r", name, exc, exc_info=exc)

    async def _serve_async() -> None:
        port = int(os.environ.get("PORT", "8000"))
        config = uvicorn.Config(combined_app, host="0.0.0.0", port=port, log_level="info")
        server = uvicorn.Server(config)
        logger.info(
            "API + Socket.IO server listening on 0.0.0.0:%d (POST /accounts/provision, "
            "Socket.IO at /) - point `ngrok http %d` at this port", port, port,
        )

        background = {
            "live_state_publisher": asyncio.create_task(
                run_live_state_publisher(fanout, account_user_map, supabase)
            ),
            "stale_pending_sweep": asyncio.create_task(_stale_pending_sweep()),
            "close_retry_sweep": asyncio.create_task(_close_retry_sweep()),
            "billing_grace_sweep": asyncio.create_task(_billing_grace_sweep()),
            "weekly_charge_sweep": asyncio.create_task(_weekly_charge_sweep()),
        }
        for name, task in background.items():
            task.add_done_callback(lambda t, name=name: _log_background_task_result(name, t))

        try:
            await server.serve()
        finally:
            for task in background.values():
                task.cancel()
            await asyncio.gather(*background.values(), return_exceptions=True)

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
    parser = argparse.ArgumentParser(description="CopyDesk fanout backend (Connect based)")
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