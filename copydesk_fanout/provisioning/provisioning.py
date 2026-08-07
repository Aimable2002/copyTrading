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

from . import instance_pool
from ..core.fanout_core import FanoutCore
from ..core.follower_agent import FollowerAgent
from .instance_pool import PoolError
from ..infra.supabase_client import execute_with_retry
from ..core.terminal_agent import TerminalAgent

logger = logging.getLogger("provisioning")

Role = Literal["master", "follower"]

_ORDER_MAX_ORDERS = 999
_ORDER_MAX_LOT_SIZE = 100.0

_CONNECT_TIMEOUT_SECONDS = 45
_CONNECT_POLL_SECONDS = 1.0

_STALLED_LOG_INTERVAL_SECONDS = 30
_MAX_STALLED_WAIT_SECONDS = 1800 


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


# --------------------------------------------------------------------- #
# Everything below to the next section marker is used ONLY by
# scripts/register_pool_instance.py, the offline pool-registration
# script. This is deliberately the one place that still pays the
# clone+launch+wait-for-update cost - an operator runs it manually,
# ahead of demand, to grow the pool. Nothing on the request path
# (provision_account / provision_account_start below) calls these
# anymore; that path only claims an already-running instance from
# instance_pool.py.
# --------------------------------------------------------------------- #


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
    config_path = instance_dir / "provisioned_config.ini"
    config_path.write_text(
        "[Common]\n"
        f"Login={login}\n"
        f"Password={password}\n"
        f"Server={server}\n"
    )
    return config_path


def read_provisioned_credentials(instance_dir: Path) -> tuple[str, str, str]:
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
    exe_name = os.environ.get("TERMINAL_EXECUTABLE_NAME", "terminal64.exe")
    return str(instance_dir / exe_name)


def resolve_instance_dir(stored_path: str) -> Path:
    p = Path(stored_path)
    if p.name.lower() == "files" and p.parent.name.upper() == "MQL5":
        return p.parent.parent
    if p.suffix.lower() == ".exe":
        return p.parent
    return p


def _legacy_display_path(instance_dir: Path) -> str:
    return str(instance_dir / "MQL5" / "Files")


# --------------------------------------------------------------------- #
# End of offline-script-only helpers. Everything below runs on the
# request path.
# --------------------------------------------------------------------- #

ConnectOutcome = Literal["connected", "stalled"]


def _insert_placeholder_account(*, account_id: str, user_id: str, role: Role, supabase_client: Any) -> None:
    """
    Inserted before claim_instance() so instance_pool.claimed_by_account_id has a row
    in "accounts" to point at (accounts.status defaults to 'provisioning' in the
    schema, set explicitly here for clarity) - the FK on that column requires the
    account to already exist, but the real row (status="live", real
    metatrader_dir_path) can't be built until the terminal has actually connected.
    finalize_provisioned_account() upgrades this row in place via upsert;
    _mark_placeholder_account_failed() records why instead if provisioning fails
    before that point.
    """
    execute_with_retry(
        lambda: supabase_client.table("accounts").insert(
            {
                "account_id": account_id,
                "user_id": user_id,
                "role": role,
                "status": "provisioning",
            }
        ).execute()
    )


def _mark_placeholder_account_failed(*, account_id: str, reason: str, supabase_client: Any) -> None:
    """
    Updates the row _insert_placeholder_account() created to status="failed" with
    provisioning_error=reason, for when provisioning fails before
    finalize_provisioned_account() gets to upgrade it to "live". Scoped to
    status="provisioning" in the WHERE clause so this can never overwrite a real,
    already-finalized account row even in a pathological account_id collision.
    """
    execute_with_retry(
        lambda: supabase_client.table("accounts").update(
            {"status": "failed", "provisioning_error": reason}
        ).eq("account_id", account_id).eq("status", "provisioning").execute()
    )


def _wait_fast(agent: TerminalAgent, timeout: float = _CONNECT_TIMEOUT_SECONDS) -> ConnectOutcome:
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
            f"credentials or the terminal's AutoTrading toggle being off are the usual causes."
        )
    return "stalled"


def _wait_stalled(agent: TerminalAgent) -> None:
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
    return f"{role}_{uuid.uuid4().hex[:10]}"


def _start_terminal_and_agent(
    *, account_id: str, user_id: str, role: Role, login: str, password: str, server: str, fanout: FanoutCore,
    supabase_client: Any,
) -> tuple[TerminalAgent, Path, str]:
    _insert_placeholder_account(account_id=account_id, user_id=user_id, role=role, supabase_client=supabase_client)

    try:
        pool_row = instance_pool.claim_instance(role=role, account_id=account_id, supabase_client=supabase_client)
    except PoolError as exc:
        _mark_placeholder_account_failed(account_id=account_id, reason=str(exc), supabase_client=supabase_client)
        raise ProvisioningError(str(exc)) from exc

    instance_dir = Path(pool_row["instance_dir"])
    terminal_path = pool_row["terminal_path"]

    try:
        _write_startup_config(instance_dir, login=login, password=password, server=server)

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
        # No launched_process handle here on purpose. This terminal was not spawned by
        # this request - it's a pool instance that's been running since
        # scripts/register_pool_instance.py started it, and it keeps running after this
        # account closes too. _wait_fast's early-exit-detection is skipped as a result
        # (it degrades to relying on the stalled-wait timeout only); see that function.
        agent.terminal_process = None
        return agent, instance_dir, terminal_path
    except ProvisioningError as exc:
        instance_pool.release_instance(account_id=account_id, supabase_client=supabase_client)
        _mark_placeholder_account_failed(account_id=account_id, reason=str(exc), supabase_client=supabase_client)
        raise
    except Exception as exc:  
        instance_pool.release_instance(account_id=account_id, supabase_client=supabase_client)
        _mark_placeholder_account_failed(
            account_id=account_id, reason=f"Unexpected provisioning failure: {exc}", supabase_client=supabase_client,
        )
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
    if role == "master":
        fanout.register_master(agent)
    else:
        fanout.register_follower(agent)
    agents.append(agent)
    account_user_map[account_id] = user_id

    execute_with_retry(
        lambda: supabase_client.table("accounts").upsert(
            {
                "account_id": account_id,
                "user_id": user_id,
                "role": role,
                "metatrader_dir_path": _legacy_display_path(Path(terminal_path).parent),
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
    if role == "follower" and (master_account_id is None or multiplier is None or sizing_mode is None):
        raise ProvisioningError("follower provisioning requires master_account_id, multiplier, sizing_mode")

    account_id = account_id or generate_account_id(role)
    agent, instance_dir, terminal_path = _start_terminal_and_agent(
        account_id=account_id, user_id=user_id, role=role, login=login, password=password, server=server,
        fanout=fanout, supabase_client=supabase_client,
    )

    outcome = _wait_fast(agent)
    if outcome == "stalled":
        try:
            _wait_stalled(agent)
        except ProvisioningError as exc:
            instance_pool.release_instance(account_id=account_id, supabase_client=supabase_client)
            _mark_placeholder_account_failed(account_id=account_id, reason=str(exc), supabase_client=supabase_client)
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
    user_id: str,
    role: Role,
    login: str,
    password: str,
    server: str,
    fanout: FanoutCore,
    supabase_client: Any,
    master_account_id: str | None = None,
    multiplier: float | None = None,
    sizing_mode: str | None = None,
    account_id: str | None = None,
) -> tuple[TerminalAgent, Path, str, ConnectOutcome, str]:
    if role == "follower" and (master_account_id is None or multiplier is None or sizing_mode is None):
        raise ProvisioningError("follower provisioning requires master_account_id, multiplier, sizing_mode")

    account_id = account_id or generate_account_id(role)
    agent, instance_dir, terminal_path = _start_terminal_and_agent(
        account_id=account_id, user_id=user_id, role=role, login=login, password=password, server=server,
        fanout=fanout, supabase_client=supabase_client,
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
    try:
        _wait_stalled(agent)
    except ProvisioningError as exc:
        instance_pool.release_instance(account_id=account_id, supabase_client=supabase_client)
        _mark_placeholder_account_failed(account_id=account_id, reason=str(exc), supabase_client=supabase_client)
        raise

    finalize_provisioned_account(
        agent, terminal_path,
        user_id=user_id, role=role, account_id=account_id,
        fanout=fanout, supabase_client=supabase_client, account_user_map=account_user_map, agents=agents,
        master_account_id=master_account_id, multiplier=multiplier, sizing_mode=sizing_mode,
    )