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
into its own running MT5 terminal with AutoTrading on (done automatically
by the provisioning service - see provisioning.py). No EA/chart attachment
needed anymore - execution and state both go through the native MT5
connection (see mt5_terminal.py), not a file bridge.
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

    # Local mode has no Supabase-backed pair_store, so expected-open sets are
    # always empty here - this call won't detect anything "closed while
    # offline" (there's no persisted prior state to compare against, same
    # documented limitation as before), but it still seeds each agent's
    # polling baseline from a real snapshot instead of an empty dict, which
    # on its own prevents a restart from misreporting already-open local
    # test trades as new signals.
    fanout.reconcile_all()

    _run_agents(agents)


def _run_supabase_mode(serve: bool) -> None:
    # Local import: keeps `supabase` an optional dependency for local-file-mode-only use.
    import os

    from .supabase_client import execute_with_retry, get_supabase_client
    from .provisioning import ProvisioningError, read_provisioned_credentials, resolve_instance_dir

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

        # Credentials are never read from Supabase, by design - only
        # metatrader_dir_path is persisted there. login/password/server
        # live exclusively in provisioned_config.ini next to the terminal
        # itself (see provisioning.py's _write_startup_config /
        # read_provisioned_credentials), recovered here from disk on
        # every restart instead.
        #
        # metatrader_dir_path keeps its original MQL5\Files-suffixed shape
        # (frontend/existing rows depend on that convention) - it does NOT
        # point straight at the instance root or the exe. resolve_instance_dir()
        # strips that suffix back off to get the real instance root, and the
        # exe path (what mt5.initialize() actually needs) is rebuilt from there.
        stored_path = account["metatrader_dir_path"]
        instance_dir = resolve_instance_dir(stored_path)

        # A row can outlive its instance folder on THIS machine (deleted,
        # never synced here, or provisioning interrupted before
        # provisioned_config.ini got written). That's a per-account data
        # problem, not a reason to take the whole fanout process down -
        # skip just this account and keep starting up with whatever
        # accounts are actually recoverable.
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

    # NOTE: this still only registers agents that were `live` at startup -
    # dynamically adding an agent when a new account goes live mid-run
    # (without restarting this process) is the orchestrator piece from the
    # build plan, not yet implemented here.

    # Must run after every master/follower agent above is registered
    # (reconcile_all() needs the full master_agents/follower_agents maps)
    # and before _run_agents*/agent.start() below starts the live polling
    # loops - see FanoutCore.reconcile_all()'s docstring for why ordering
    # matters here: this is what makes a restart/crash/deploy safe rather
    # than something that silently loses track of open trades.
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

    from . import billing, profit_share
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
            try:
                await loop.run_in_executor(None, pair_store.expire_stale_pending)
            except Exception:
                # A sweep cycle failing (e.g. Supabase connection blip that
                # outlasted execute_with_retry's own attempts) must never
                # kill this task - that would cancel the whole gather()
                # below, taking the API/Socket.IO server down with it. Log
                # and pick back up on the next interval instead.
                logger.exception("Stale-pending sweep cycle failed - will retry next interval")

    async def _close_retry_sweep(interval_seconds: float = 2.0) -> None:
        """Backstop only - _attempt_close already retries a rejected close
        immediately, inline, several times before this ever runs (see
        fanout_core.py's _INLINE_CLOSE_RETRY_ATTEMPTS). This sweep exists
        for the case where the broker is still rejecting after all of
        those - e.g. the market/symbol itself is unavailable for a beat -
        so it stays tight (2s) rather than the 15-30s cadence of the other
        sweeps: this is real, un-hedged exposure sitting live."""
        loop = asyncio.get_event_loop()
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                await loop.run_in_executor(None, fanout.retry_failed_closes)
            except Exception:
                logger.exception("Close-retry sweep cycle failed - will retry next interval")

    async def _billing_grace_sweep(interval_seconds: float = 300.0) -> None:
        """Closes any billing_period past its 5-day grace window. 5 minutes
        is plenty for a check whose actual threshold is measured in days -
        no reason to poll this as tightly as the stale-pending sweep,
        which is cleaning up something that can go wrong within seconds."""
        loop = asyncio.get_event_loop()
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                await loop.run_in_executor(None, billing.check_grace_expirations, fanout, supabase, agents)
            except Exception:
                logger.exception("Billing-grace sweep cycle failed - will retry next interval")

    async def _profit_share_sweep(interval_seconds: float = 60.0) -> None:
        """Bills newly-closed profitable follower deals. Runs more often
        than the billing-grace sweep since this is the thing a follower
        actually sees move (their wallet balance, per the "updated the
        moment it's touched" requirement) - a full minute of lag between a
        trade closing and its charge landing is already a compromise
        against hitting the MT5/EA polling path this hard on every
        follower, every few seconds."""
        loop = asyncio.get_event_loop()
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                await loop.run_in_executor(None, lambda: profit_share.run_poll_cycle(fanout=fanout, account_user_map=account_user_map, supabase_client=supabase))
            except Exception:
                logger.exception("Profit-share sweep cycle failed - will retry next interval")

    def _log_background_task_result(name: str, task: "asyncio.Task") -> None:
        """Attached to every background task below. These tasks are meant
        to loop forever; the only way this callback fires is (a) the task
        was cancelled (normal shutdown, not an error) or (b) something
        escaped every try/except inside the loop and killed the task for
        good. Either way this must only log - it must NEVER re-raise or
        otherwise touch server.serve(), which is exactly the crash this
        replaces: previously all these tasks were passed straight into the
        same asyncio.gather() as server.serve(), so one of them dying took
        the whole API/Socket.IO server down with it."""
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
            "profit_share_sweep": asyncio.create_task(_profit_share_sweep()),
        }
        for name, task in background.items():
            task.add_done_callback(lambda t, name=name: _log_background_task_result(name, t))

        try:
            # Only server.serve() is awaited directly here - a background
            # task raising can no longer cancel it (see _log_background_task_result
            # above for why that was happening before). Ctrl+C / SIGTERM
            # still stops everything via the `finally` below.
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