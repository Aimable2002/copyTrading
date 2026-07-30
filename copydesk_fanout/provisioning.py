"""
Turns user-submitted MT5 credentials into a running, registered
TerminalAgent/FollowerAgent - the automation layer from the "how does
MetaApi do this" discussion: clone a pre-configured template terminal
folder, write per-account login + EA-parameter files, launch the terminal
in portable mode, wait for the EA to come alive, then wire it into the
already-running FanoutCore the same way main.py wires up startup accounts.

Required environment variables:
  TEMPLATE_TERMINAL_DIR
      Path to a ONE-TIME, manually prepared portable MT5 install:
      DWX_Server_MT5.ex5 already dropped in MQL5/Experts/, the terminal's
      AutoTrading toggle turned on, the EA's own "Allow Algo Trading"
      ticked, then the terminal closed normally. That on/off state is
      saved inside this folder's own files, so every clone inherits it -
      this is the actual mechanism, not a config key (see the AutoTrading
      conversation this was built from). This needs a one-time real check:
      confirm a cloned copy with swapped credentials keeps that state on
      your broker/build before relying on it for real users.
  INSTANCES_DIR
      Where per-account clones are created.
  TERMINAL_EXECUTABLE_NAME
      Filename only (not a path) of the terminal binary inside the cloned
      instance dir, e.g. "terminal64.exe" for MT5. NOTE: portable mode
      ties the data folder to wherever this exe physically lives - it is
      NOT determined by /config: or the process's working directory. That
      means every clone must run ITS OWN copy of the exe (already true,
      since _clone_template() copies the whole template dir including the
      binary) - launching one shared, fixed exe path for every account
      would make every instance silently share the same data folder.

Both EA-level order caps (MaximumOrders, MaximumLotSize) - which hard-
REJECT orders past their default values regardless of what Supabase's
subscription config says, see mql/DWX_Server_MT5.mq5 lines ~290 and ~319 -
are overridden per-instance via a generated .set file, so Supabase's
multiplier/sizing_mode stays the only real limiter.
"""

from __future__ import annotations

import logging
import os
import shutil
import stat
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any, Literal

from .fanout_core import FanoutCore
from .follower_agent import FollowerAgent
from .supabase_client import execute_with_retry
from .terminal_agent import TerminalAgent

logger = logging.getLogger("provisioning")

Role = Literal["master", "follower"]

# High enough to never be the real limiter - Supabase's sizing config
# (enforced in sizing.py) is meant to be the only thing that actually
# constrains order count/size, not these EA-level inputs.
# Passed straight to TerminalAgent/FollowerAgent's max_orders/max_lot_size
# (see mt5_terminal.py's check_order_caps) - this is the direct Python
# replacement for what used to be a generated .set file read by the EA.
_ORDER_MAX_ORDERS = 999
_ORDER_MAX_LOT_SIZE = 100.0

_CONNECT_TIMEOUT_SECONDS = 45
_CONNECT_POLL_SECONDS = 1.0

# Second phase, only entered if the terminal process is still alive past
# _CONNECT_TIMEOUT_SECONDS but hasn't connected yet - the "someone needs to
# click Later on the LiveUpdate dialog" case. Not a failure by itself; see
# _wait_fast / _wait_stalled.
_STALLED_LOG_INTERVAL_SECONDS = 30
_MAX_STALLED_WAIT_SECONDS = 1800  # 30 min upper bound - last-resort safety net, not a normal expectation


class ProvisioningError(Exception):
    """Raised for any provisioning failure. Message is safe to surface to an API caller."""


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ProvisioningError(
            f"Missing required environment variable {name}. See provisioning.py's module "
            f"docstring for what it needs to point at."
        )
    return value


def _clone_template(account_id: str) -> Path:
    template_dir = Path(_require_env("TEMPLATE_TERMINAL_DIR"))
    if not template_dir.exists():
        raise ProvisioningError(f"TEMPLATE_TERMINAL_DIR does not exist: {template_dir}")

    instances_dir = Path(_require_env("INSTANCES_DIR"))
    instances_dir.mkdir(parents=True, exist_ok=True)
    instance_dir = instances_dir / account_id
    if instance_dir.exists():
        raise ProvisioningError(f"Instance dir already exists: {instance_dir}")

    shutil.copytree(template_dir, instance_dir)
    logger.info("Cloned template terminal for %s -> %s", account_id, instance_dir)
    return instance_dir


def _unlock_and_remove(instance_dir: Path) -> None:
    """shutil.rmtree can't delete read-only files on Windows - clear the
    attribute on anything under instance_dir before removing it, or a
    failed provisioning attempt leaves an orphaned locked folder behind."""
    if not instance_dir.exists():
        return
    for path in instance_dir.rglob("*"):
        if path.is_file():
            try:
                os.chmod(path, stat.S_IWRITE)
            except OSError:
                pass
    shutil.rmtree(instance_dir, ignore_errors=True)


def _write_startup_config(instance_dir: Path, *, login: str, password: str, server: str) -> Path:
    """Login-only startup config now - no [StartUp] Expert= section, since
    there's no EA to attach. The terminal just needs to come up logged in
    with AutoTrading on (see TEMPLATE_TERMINAL_DIR's one-time setup); the
    Mt5Terminal sidecar's own mt5.initialize(login=..., password=...,
    server=...) call is what actually establishes the account session
    that matters for reads/writes - this file just gets the terminal UI
    itself logged in on launch.

    This is also, deliberately, the ONLY place login/password/server get
    persisted anywhere - never in Supabase. See read_provisioned_credentials()
    below, which is how main.py recovers them on restart instead."""
    config_path = instance_dir / "provisioned_config.ini"
    config_path.write_text(
        "[Common]\n"
        f"Login={login}\n"
        f"Password={password}\n"
        f"Server={server}\n"
    )
    return config_path


def read_provisioned_credentials(instance_dir: Path) -> tuple[str, str, str]:
    """Reads login/password/server back out of provisioned_config.ini -
    the reverse of _write_startup_config(). This is how main.py's Supabase
    restart path gets credentials WITHOUT them ever being written to
    Supabase - only terminal_path is persisted there; the instance folder
    on disk (same machine the terminal itself lives on) is the sole
    credential store, by design. Public (no leading underscore) since
    main.py needs to call this directly.
    """
    config_path = instance_dir / "provisioned_config.ini"
    if not config_path.exists():
        raise ProvisioningError(
            f"provisioned_config.ini not found in {instance_dir} - can't recover credentials "
            f"for this account without it."
        )
    values: dict[str, str] = {}
    for line in config_path.read_text().splitlines():
        if "=" in line:
            key, _, value = line.partition("=")
            values[key.strip().lower()] = value.strip()
    try:
        return values["login"], values["password"], values["server"]
    except KeyError as exc:
        raise ProvisioningError(
            f"provisioned_config.ini in {instance_dir} is missing one of Login/Password/Server."
        ) from exc


def _launch_terminal(instance_dir: Path, config_path: Path) -> subprocess.Popen:
    # Must be the copy inside instance_dir - see the TERMINAL_EXECUTABLE_NAME
    # note in the module docstring for why a shared fixed path is wrong here.
    exe_name = os.environ.get("TERMINAL_EXECUTABLE_NAME", "terminal64.exe")
    terminal_exe = instance_dir / exe_name
    if not terminal_exe.exists():
        raise ProvisioningError(
            f"{exe_name} not found in cloned instance dir {instance_dir} - "
            f"is TEMPLATE_TERMINAL_DIR the full MT5 install folder, not just its data folder?"
        )
    proc = subprocess.Popen(
        [str(terminal_exe), "/portable", f"/config:{config_path}"],
        cwd=str(instance_dir),
    )
    logger.info("Launched terminal for %s (pid %s)", instance_dir.name, proc.pid)
    return proc


def _terminal_exe_path(instance_dir: Path) -> str:
    # Mt5Terminal's worker needs the exe path directly (mt5.initialize(path=...)),
    # not a Files subfolder - there's no DWX file bridge left to point at.
    exe_name = os.environ.get("TERMINAL_EXECUTABLE_NAME", "terminal64.exe")
    return str(instance_dir / exe_name)


ConnectOutcome = Literal["connected", "stalled"]


def _wait_fast(agent: TerminalAgent, timeout: float = _CONNECT_TIMEOUT_SECONDS) -> ConnectOutcome:
    """Phase 1 - the normal-case wait, same ~45s window the original
    synchronous version always used. This is the "nice waiting" for the
    common case: connects quickly, caller gets a real success response
    without any background hand-off.

    Returns "connected" or "stalled" (terminal process still alive, just
    not connected yet within the normal window). Raises ProvisioningError
    immediately if the process has already died - that's a real, fast
    failure, handled exactly like before (caller can turn it straight
    into an HTTP error).
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if agent.is_connected:
            return "connected"
        time.sleep(_CONNECT_POLL_SECONDS)

    launched_process: subprocess.Popen | None = getattr(agent, "terminal_process", None)
    if launched_process is not None and launched_process.poll() is not None:
        raise ProvisioningError(
            f"Terminal process for {agent.account_id} exited before connecting (exit code "
            f"{launched_process.returncode}) - check the launched terminal directly: bad "
            f"credentials, AutoTrading off, or the EA failed to attach are the three usual causes."
        )
    return "stalled"


def _wait_stalled(agent: TerminalAgent) -> None:
    """Phase 2 - only reached after _wait_fast() returns "stalled". Meant
    to run OFF the request thread (see provision_account_finish /
    api_server.py's background task) - this is exactly the "someone needs
    to click Later on MT5's LiveUpdate dialog" case, and can legitimately
    take a while. Not silent: logs immediately, then again every
    _STALLED_LOG_INTERVAL_SECONDS, so it's visible in server logs the
    whole time it's waiting. Bounded by _MAX_STALLED_WAIT_SECONDS as a
    last-resort safety net, not a normal expectation.
    """
    logger.warning(
        "[%s] Not connected after the normal window - terminal process is still alive, so this "
        "is NOT being treated as a failure. Waiting for it to connect; will resume automatically.",
        agent.account_id,
    )
    stall_start = time.monotonic()
    last_log = stall_start
    while True:
        if agent.is_connected:
            logger.info(
                "[%s] Connected after %.0fs stalled wait - resuming provisioning.",
                agent.account_id, time.monotonic() - stall_start,
            )
            return
        launched_process: subprocess.Popen | None = getattr(agent, "terminal_process", None)
        if launched_process is not None and launched_process.poll() is not None:
            raise ProvisioningError(
                f"Terminal process for {agent.account_id} exited while waiting for human "
                f"intervention (exit code {launched_process.returncode}) - it was alive moments "
                f"ago, so this is likely a crash or manual close rather than the original connect timeout."
            )
        if time.monotonic() - last_log >= _STALLED_LOG_INTERVAL_SECONDS:
            logger.warning(
                "[%s] Still waiting for human admin intervention (%.0fs elapsed, terminal "
                "process alive, not yet connected).",
                agent.account_id, time.monotonic() - stall_start,
            )
            last_log = time.monotonic()
        if time.monotonic() - stall_start > _MAX_STALLED_WAIT_SECONDS:
            raise ProvisioningError(
                f"Terminal for {agent.account_id} still not connected after "
                f"{_MAX_STALLED_WAIT_SECONDS:.0f}s of waiting for human intervention - giving up. "
                f"Terminal process is still alive; check it directly."
            )
        time.sleep(_CONNECT_POLL_SECONDS)


def generate_account_id(role: Role) -> str:
    """Split out from provision_account so a caller (api_server.py) can
    generate the id up front and hand it back to the client immediately -
    needed now that provisioning can genuinely take a while (see
    _wait_stalled) rather than always finishing within one request."""
    return f"{role}_{uuid.uuid4().hex[:10]}"


def _start_terminal_and_agent(
    *, account_id: str, role: Role, login: str, password: str, server: str, fanout: FanoutCore,
) -> tuple[TerminalAgent, Path, str]:
    """Clone, write per-account login config, launch the terminal, construct
    and start the agent. Does NOT wait for connection - see _wait_fast /
    _wait_stalled. On any failure here, cleans up the cloned instance dir
    and raises ProvisioningError; either this fully succeeds or it leaves
    no partial state behind."""
    instance_dir = _clone_template(account_id)
    try:
        config_path = _write_startup_config(instance_dir, login=login, password=password, server=server)
        launched_process = _launch_terminal(instance_dir, config_path)
        terminal_path = _terminal_exe_path(instance_dir)

        agent: TerminalAgent
        if role == "master":
            agent = TerminalAgent(
                account_id=account_id,
                terminal_path=terminal_path,
                login=int(login),
                password=password,
                server=server,
                max_orders=_ORDER_MAX_ORDERS,
                max_lot_size=_ORDER_MAX_LOT_SIZE,
                on_trade_event=fanout.handle_master_trade_event,
            )
        else:
            agent = FollowerAgent(
                account_id=account_id,
                terminal_path=terminal_path,
                login=int(login),
                password=password,
                server=server,
                max_orders=_ORDER_MAX_ORDERS,
                max_lot_size=_ORDER_MAX_LOT_SIZE,
                on_trade_event=fanout.handle_follower_trade_event,
            )

        agent.start()
        agent.terminal_process = launched_process  # noqa: attribute added dynamically - see account_lifecycle.py's close_account
        return agent, instance_dir, terminal_path
    except ProvisioningError:
        _unlock_and_remove(instance_dir)
        raise
    except Exception as exc:  # noqa: BLE001 - any unexpected failure still must not leak a half-built instance
        _unlock_and_remove(instance_dir)
        raise ProvisioningError(f"Unexpected provisioning failure: {exc}") from exc


def finalize_provisioned_account(
    agent: TerminalAgent,
    terminal_path: str,
    *,
    user_id: str,
    role: Role,
    account_id: str,
    fanout: FanoutCore,
    supabase_client: Any,
    account_user_map: dict[str, str],
    agents: list[TerminalAgent],
    master_account_id: str | None = None,
    multiplier: float | None = None,
    sizing_mode: str | None = None,
) -> None:
    """Registers into fanout + writes Supabase rows. Only call once the
    terminal is actually confirmed connected - nothing partial gets
    registered before that. Public (no leading underscore) because
    api_server.py calls this directly for the fast/"connected" case
    instead of going through provision_account_finish().

    Deliberately does NOT write login/password/server to Supabase - only
    terminal_path. Credentials live exclusively in provisioned_config.ini
    on disk (see _write_startup_config/read_provisioned_credentials) -
    main.py's restart path recovers them from there, not from this table."""
    if role == "master":
        fanout.register_master(agent)
    else:
        fanout.register_follower(agent)
    agents.append(agent)
    account_user_map[account_id] = user_id

    execute_with_retry(
        lambda: supabase_client.table("accounts").insert(
            {
                "account_id": account_id,
                "user_id": user_id,
                "role": role,
                "metatrader_dir_path": terminal_path,  # same column name the frontend already reads - only the value's meaning changed (now the terminal64.exe path, not the old MQL5/Files folder)
                "status": "live",
            }
        ).execute()
    )

    if role == "follower":
        execute_with_retry(
            lambda: supabase_client.table("subscriptions").insert(
                {
                    "master_account_id": master_account_id,
                    "follower_account_id": account_id,
                    "multiplier": multiplier,
                    "sizing_mode": sizing_mode,
                    "active": True,
                }
            ).execute()
        )
        # ConfigStore's realtime sync (already running in the background,
        # see config_store.py's start_realtime_sync) picks this row up on
        # its own INSERT listener - no direct config_store call needed here.

    logger.info("Provisioned %s account %s for user %s", role, account_id, user_id)


def provision_account(
    *,
    user_id: str,
    role: Role,
    login: str,
    password: str,
    server: str,
    fanout: FanoutCore,
    supabase_client: Any,
    account_user_map: dict[str, str],
    agents: list[TerminalAgent],
    master_account_id: str | None = None,
    multiplier: float | None = None,
    sizing_mode: str | None = None,
    account_id: str | None = None,
) -> str:
    """
    End-to-end, fully synchronous, including the full (potentially long,
    human-intervention) wait if the terminal stalls: credentials in ->
    running, registered agent + Supabase rows out. Raises
    ProvisioningError on any failure. On failure, the cloned instance dir
    is removed and nothing is registered/written - either this fully
    succeeds or it leaves no partial state behind.

    For a caller that needs to respond quickly rather than block through
    a possibly-long stalled wait (i.e. the HTTP API), use
    provision_account_start() + provision_account_finish() instead - see
    api_server.py. This function is what those two are built from
    underneath, kept as one piece for any other synchronous caller.

    account_id: pass a value already generated via generate_account_id()
    if the caller needs to know it before this function returns. Generates
    one internally if not given.
    """
    if role == "follower" and (master_account_id is None or multiplier is None or sizing_mode is None):
        raise ProvisioningError("follower provisioning requires master_account_id, multiplier, sizing_mode")

    account_id = account_id or generate_account_id(role)
    agent, instance_dir, terminal_path = _start_terminal_and_agent(
        account_id=account_id, role=role, login=login, password=password, server=server, fanout=fanout,
    )

    outcome = _wait_fast(agent)
    if outcome == "stalled":
        try:
            _wait_stalled(agent)
        except ProvisioningError:
            _unlock_and_remove(instance_dir)
            raise

    finalize_provisioned_account(
        agent, terminal_path,
        user_id=user_id, role=role, account_id=account_id,
        fanout=fanout, supabase_client=supabase_client, account_user_map=account_user_map, agents=agents,
        master_account_id=master_account_id, multiplier=multiplier, sizing_mode=sizing_mode,
    )
    return account_id


def provision_account_start(
    *,
    role: Role,
    login: str,
    password: str,
    server: str,
    fanout: FanoutCore,
    master_account_id: str | None = None,
    multiplier: float | None = None,
    sizing_mode: str | None = None,
    account_id: str | None = None,
) -> tuple[TerminalAgent, Path, str, ConnectOutcome, str]:
    """Phase 1, for callers (api_server.py) that need to respond to an HTTP
    request quickly instead of blocking through a possibly-long stalled
    wait. Runs clone/launch/agent-start/normal-window wait only - the
    same ~45s "nice waiting" provision_account() always used for the
    common case. Still validates follower args and still raises
    ProvisioningError immediately (a real, fast failure, same as before)
    if the process died within that window.

    Returns (agent, instance_dir, terminal_path, outcome, account_id)
    so the caller can respond "live" right away if outcome == "connected"
    (via finalize_provisioned_account()), or "awaiting_attention" if
    outcome == "stalled" and hand the rest off to provision_account_finish()
    in a background task.
    """
    if role == "follower" and (master_account_id is None or multiplier is None or sizing_mode is None):
        raise ProvisioningError("follower provisioning requires master_account_id, multiplier, sizing_mode")

    account_id = account_id or generate_account_id(role)
    agent, instance_dir, terminal_path = _start_terminal_and_agent(
        account_id=account_id, role=role, login=login, password=password, server=server, fanout=fanout,
    )
    outcome = _wait_fast(agent)
    return agent, instance_dir, terminal_path, outcome, account_id


def provision_account_finish(
    agent: TerminalAgent,
    instance_dir: Path,
    terminal_path: str,
    *,
    user_id: str,
    role: Role,
    account_id: str,
    fanout: FanoutCore,
    supabase_client: Any,
    account_user_map: dict[str, str],
    agents: list[TerminalAgent],
    master_account_id: str | None = None,
    multiplier: float | None = None,
    sizing_mode: str | None = None,
) -> None:
    """Phase 2 - call only when provision_account_start() returned
    outcome == "stalled". Meant to run in a background task: waits
    (potentially a long time) for a human to resolve whatever's blocking
    the terminal, then finalizes registration exactly like
    provision_account()'s tail does. Raises ProvisioningError if the
    terminal dies during the wait or the max-wait is exceeded - there's no
    live HTTP request left at that point, so the caller (api_server.py) is
    responsible for logging it rather than turning it into a response."""
    try:
        _wait_stalled(agent)
    except ProvisioningError:
        _unlock_and_remove(instance_dir)
        raise

    finalize_provisioned_account(
        agent, terminal_path,
        user_id=user_id, role=role, account_id=account_id,
        fanout=fanout, supabase_client=supabase_client, account_user_map=account_user_map, agents=agents,
        master_account_id=master_account_id, multiplier=multiplier, sizing_mode=sizing_mode,
    )