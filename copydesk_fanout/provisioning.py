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
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any, Literal

from .fanout_core import FanoutCore
from .follower_agent import FollowerAgent
from .terminal_agent import TerminalAgent

logger = logging.getLogger("provisioning")

Role = Literal["master", "follower"]

# High enough to never be the real limiter - Supabase's sizing config
# (enforced in sizing.py) is meant to be the only thing that actually
# constrains order count/size, not these EA-level inputs.
_EA_MAX_ORDERS_OVERRIDE = 999
_EA_MAX_LOT_SIZE_OVERRIDE = 100.0

_CONNECT_TIMEOUT_SECONDS = 45
_CONNECT_POLL_SECONDS = 1.0


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


def _write_expert_parameters(instance_dir: Path) -> Path:
    """Plain Name=Value per line - overrides the EA's hard-coded order
    caps so they stop being the real limiter. NOTE: exact folder MT5
    expects an ExpertParameters file in isn't 100% pinned down here -
    verify this against a real launch before relying on it; if MT5 can't
    find it, it silently falls back to the EA's compiled defaults
    (MaximumOrders=1, MaximumLotSize=0.01), which is exactly the failure
    mode we're trying to avoid."""
    set_path = instance_dir / "provisioned.set"
    set_path.write_text(
        f"MaximumOrders={_EA_MAX_ORDERS_OVERRIDE}\n"
        f"MaximumLotSize={_EA_MAX_LOT_SIZE_OVERRIDE}\n"
    )
    return set_path


def _write_startup_config(
    instance_dir: Path, *, login: str, password: str, server: str, set_file: Path
) -> Path:
    config_path = instance_dir / "provisioned_config.ini"
    config_path.write_text(
        "[Common]\n"
        f"Login={login}\n"
        f"Password={password}\n"
        f"Server={server}\n"
        "\n"
        "[StartUp]\n"
        "Expert=DWX_Server_MT5\n"
        "Symbol=EURUSD\n"
        "Period=M1\n"
        f"ExpertParameters={set_file.name}\n"
    )
    return config_path


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


def _metatrader_files_path(instance_dir: Path) -> str:
    # Portable mode keeps the terminal's data folder inside its own install
    # dir, so this is the same path dwx_client.py expects, just per-clone.
    return str(instance_dir / "MQL5" / "Files")


def _wait_until_connected(agent: TerminalAgent, timeout: float = _CONNECT_TIMEOUT_SECONDS) -> None:
    """Polls the same is_connected property base_agent.py already exposes
    (True once the EA has written its first account_info payload)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if agent.is_connected:
            return
        time.sleep(_CONNECT_POLL_SECONDS)
    raise ProvisioningError(
        f"Terminal for {agent.account_id} did not report account_info within {timeout}s - "
        "check the launched terminal directly: bad credentials, AutoTrading off, or the EA "
        "failed to attach are the three usual causes."
    )


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
) -> str:
    """
    End-to-end: credentials in -> running, registered agent + Supabase rows
    out. Raises ProvisioningError on any failure - the caller (api_server.py)
    turns that into an HTTP error response. On failure, the cloned instance
    dir is removed and nothing is registered/written - either this fully
    succeeds or it leaves no partial state behind.
    """
    if role == "follower" and (master_account_id is None or multiplier is None or sizing_mode is None):
        raise ProvisioningError("follower provisioning requires master_account_id, multiplier, sizing_mode")

    account_id = f"{role}_{uuid.uuid4().hex[:10]}"
    instance_dir = _clone_template(account_id)

    try:
        set_path = _write_expert_parameters(instance_dir)
        config_path = _write_startup_config(
            instance_dir, login=login, password=password, server=server, set_file=set_path
        )
        _launch_terminal(instance_dir, config_path)

        metatrader_dir_path = _metatrader_files_path(instance_dir)

        agent: TerminalAgent
        if role == "master":
            agent = TerminalAgent(
                account_id=account_id,
                metatrader_dir_path=metatrader_dir_path,
                on_trade_event=fanout.handle_master_trade_event,
            )
        else:
            agent = FollowerAgent(
                account_id=account_id,
                metatrader_dir_path=metatrader_dir_path,
                on_trade_event=fanout.handle_follower_trade_event,
            )

        agent.start()
        _wait_until_connected(agent)

    except ProvisioningError:
        shutil.rmtree(instance_dir, ignore_errors=True)
        raise
    except Exception as exc:  # noqa: BLE001 - any unexpected failure still must not leak a half-built instance
        shutil.rmtree(instance_dir, ignore_errors=True)
        raise ProvisioningError(f"Unexpected provisioning failure: {exc}") from exc

    # Only touch shared state (fanout registration, Supabase rows) once the
    # terminal is actually confirmed alive - nothing partial gets registered.
    if role == "master":
        fanout.register_master(agent)
    else:
        fanout.register_follower(agent)
    agents.append(agent)
    account_user_map[account_id] = user_id

    supabase_client.table("accounts").insert(
        {
            "account_id": account_id,
            "user_id": user_id,
            "role": role,
            "metatrader_dir_path": metatrader_dir_path,
            "status": "live",
        }
    ).execute()

    if role == "follower":
        supabase_client.table("subscriptions").insert(
            {
                "master_account_id": master_account_id,
                "follower_account_id": account_id,
                "multiplier": multiplier,
                "sizing_mode": sizing_mode,
                "active": True,
            }
        ).execute()
        # ConfigStore's realtime sync (already running in the background,
        # see config_store.py's start_realtime_sync) picks this row up on
        # its own INSERT listener - no direct config_store call needed here.

    logger.info("Provisioned %s account %s for user %s", role, account_id, user_id)
    return account_id