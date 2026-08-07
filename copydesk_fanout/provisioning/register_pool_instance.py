from __future__ import annotations

import argparse
import logging
import subprocess
import time
import uuid
from pathlib import Path

from . import instance_pool
from ..provisioning.provisioning import (
    ProvisioningError,
    _clone_template,
    _terminal_exe_path,
    _unlock_and_remove,
)
from ..infra.supabase_client import get_supabase_client

logger = logging.getLogger("register_pool_instance")

_DEFAULT_SETTLE_SECONDS = 90.0


def _launch_blank_terminal(instance_dir: Path) -> subprocess.Popen:
    """
    Launches the cloned terminal with no login config at all - it comes
    up at the login screen. Deliberately does not reuse
    provisioning._launch_terminal, which requires a config path with
    Login/Password/Server: a pool instance has no account yet.
    """
    terminal_exe = Path(_terminal_exe_path(instance_dir))
    if not terminal_exe.exists():
        raise ProvisioningError(f"{terminal_exe} not found in cloned instance dir {instance_dir}")
    proc = subprocess.Popen([str(terminal_exe), "/portable"], cwd=str(instance_dir))
    logger.info("Launched blank terminal at %s (pid %s)", instance_dir, proc.pid)
    return proc


def register_one(*, role: instance_pool.Role, settle_seconds: float, supabase_client) -> bool:
    name = f"pool_{role}_{uuid.uuid4().hex[:10]}"
    instance_dir = _clone_template(name)

    proc = _launch_blank_terminal(instance_dir)
    logger.info(
        "Waiting %.0fs for %s to get through startup/update before registering it...",
        settle_seconds, instance_dir,
    )
    time.sleep(settle_seconds)

    if proc.poll() is not None:
        logger.error(
            "Terminal at %s exited during the settle window (exit code %s) - not registering. "
            "Check it directly (bad template, update stuck, license/activation prompt, etc).",
            instance_dir, proc.returncode,
        )
        _unlock_and_remove(instance_dir)
        return False

    instance_pool.register_instance(
        instance_dir=str(instance_dir),
        terminal_path=_terminal_exe_path(instance_dir),
        role=role,
        supabase_client=supabase_client,
    )
    logger.info("Registered %s as an available %s pool instance.", instance_dir, role)
    return True


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--role", choices=["master", "follower"], required=True)
    parser.add_argument("--count", type=int, required=True, help="How many new instances to add to the pool")
    parser.add_argument(
        "--settle-seconds", type=float, default=_DEFAULT_SETTLE_SECONDS,
        help=f"How long to wait after launch before trusting the terminal is stable (default {_DEFAULT_SETTLE_SECONDS:.0f}s)",
    )
    args = parser.parse_args()

    supabase_client = get_supabase_client()

    succeeded = 0
    for i in range(args.count):
        logger.info("Registering instance %d/%d (%s)...", i + 1, args.count, args.role)
        try:
            if register_one(role=args.role, settle_seconds=args.settle_seconds, supabase_client=supabase_client):
                succeeded += 1
        except ProvisioningError:
            logger.exception("Failed to register instance %d/%d", i + 1, args.count)

    status = instance_pool.pool_status(supabase_client)
    logger.info(
        "Done: %d/%d new %s instance(s) registered this run. Current pool status: %s",
        succeeded, args.count, args.role, status,
    )


if __name__ == "__main__":
    main()