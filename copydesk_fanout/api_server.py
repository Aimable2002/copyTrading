"""
REST API the frontend calls - and the exact same thing curl/wscat calls
during testing, no separate test-only code path. Mounted alongside the
existing Socket.IO app (see main.py's _run_agents_with_server) so there's
one process, one port, one auth mechanism for both.

Auth reuses socket_server.verify_supabase_jwt - the same Supabase access
token the frontend already holds from Supabase Auth, passed as a normal
Authorization: Bearer header here (Socket.IO's client sends it differently,
via its own auth payload, but it's the same token type against the same
Supabase JWT secret).
"""

from __future__ import annotations

import logging
from typing import Any, Literal

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from .fanout_core import FanoutCore
from .provisioning import ProvisioningError, provision_account
from .sizing import SizingMode
from .socket_server import verify_supabase_jwt

logger = logging.getLogger("api_server")


class ProvisionRequest(BaseModel):
    role: Literal["master", "follower"]
    login: str
    password: str
    server: str
    # Only required when role == "follower":
    master_account_id: str | None = None
    multiplier: float | None = None
    sizing_mode: SizingMode | None = None


def _authenticate(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    token = authorization[len("Bearer "):]
    try:
        return verify_supabase_jwt(token)
    except Exception as exc:  # jwt.InvalidTokenError and subclasses
        raise HTTPException(status_code=401, detail="Invalid or expired token") from exc


def create_api_app(
    *,
    fanout: FanoutCore,
    supabase_client: Any,
    account_user_map: dict[str, str],
    agents: list,
) -> FastAPI:
    """Factory rather than a module-level app: main.py builds fanout/
    supabase/account_user_map/agents at startup and hands them in here,
    same objects the Socket.IO live-state publisher already reads from."""
    app = FastAPI(title="CopyDesk provisioning API")

    @app.post("/accounts/provision")
    def provision(body: ProvisionRequest, authorization: str | None = Header(default=None)):
        user_id = _authenticate(authorization)
        try:
            account_id = provision_account(
                user_id=user_id,
                role=body.role,
                login=body.login,
                password=body.password,
                server=body.server,
                fanout=fanout,
                supabase_client=supabase_client,
                account_user_map=account_user_map,
                agents=agents,
                master_account_id=body.master_account_id,
                multiplier=body.multiplier,
                sizing_mode=body.sizing_mode,
            )
        except ProvisioningError as exc:
            logger.exception("Provisioning failed for user %s", user_id)
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        return {"account_id": account_id, "status": "live"}

    return app