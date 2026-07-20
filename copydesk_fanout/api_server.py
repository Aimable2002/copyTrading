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

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from . import account_lifecycle, master_profiles, trade_history
from .account_lifecycle import LifecycleError
from .fanout_core import FanoutCore
from .master_profiles import MasterProfileError
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


class PauseRequest(BaseModel):
    force_close: bool  # required, not defaulted - "let the user choose each time" means no silent default


class MasterProfileRequest(BaseModel):
    display_name: str
    bio: str = ""
    is_public: bool = False


def _authenticate(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    token = authorization[len("Bearer "):]
    try:
        return verify_supabase_jwt(token)
    except Exception as exc:  # jwt.InvalidTokenError and subclasses
        raise HTTPException(status_code=401, detail="Invalid or expired token") from exc


def _resolve_owned_account(
    fanout: FanoutCore, account_user_map: dict[str, str], account_id: str, user_id: str,
) -> Literal["master", "follower"]:
    """Every account-scoped route (pause/resume/close/profile/trades) needs
    both: does this account exist, and does it belong to the caller. Reused
    everywhere instead of duplicating this check per route."""
    owner_id = account_user_map.get(account_id)
    if owner_id is None:
        raise HTTPException(status_code=404, detail=f"Unknown account {account_id}")
    if owner_id != user_id:
        raise HTTPException(status_code=403, detail="This account does not belong to you")

    if account_id in fanout.master_agents:
        return "master"
    if account_id in fanout.follower_agents:
        return "follower"
    raise HTTPException(status_code=409, detail=f"Account {account_id} has no running agent (already closed?)")


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

    # Allow-all for now, same reasoning as socket_server.py: Lovable's
    # preview URL changes across sessions/deploys same as ngrok's does.
    # TIGHTEN THIS before real signups - see socket_server.py's
    # ALLOWED_ORIGINS handling for the pattern to switch to.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.exception_handler(RequestValidationError)
    async def _log_validation_errors(request: Request, exc: RequestValidationError):
        # Default FastAPI behavior already returns 422 with this detail -
        # this handler only adds console visibility, doesn't change the
        # response the frontend sees.
        body = await request.body()
        logger.warning(
            "422 on %s - validation errors: %s | raw body sent: %s",
            request.url.path, exc.errors(), body.decode(errors="replace"),
        )
        return JSONResponse(status_code=422, content={"detail": exc.errors()})

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

    @app.post("/accounts/{account_id}/pause")
    def pause(account_id: str, body: PauseRequest, authorization: str | None = Header(default=None)):
        user_id = _authenticate(authorization)
        role = _resolve_owned_account(fanout, account_user_map, account_id, user_id)
        try:
            return account_lifecycle.pause_account(
                account_id=account_id, role=role, force_close=body.force_close,
                fanout=fanout, supabase_client=supabase_client,
            )
        except LifecycleError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.post("/accounts/{account_id}/resume")
    def resume(account_id: str, authorization: str | None = Header(default=None)):
        user_id = _authenticate(authorization)
        role = _resolve_owned_account(fanout, account_user_map, account_id, user_id)
        try:
            return account_lifecycle.resume_account(
                account_id=account_id, role=role, fanout=fanout, supabase_client=supabase_client,
            )
        except LifecycleError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.post("/accounts/{account_id}/close")
    def close(account_id: str, authorization: str | None = Header(default=None)):
        user_id = _authenticate(authorization)
        role = _resolve_owned_account(fanout, account_user_map, account_id, user_id)
        try:
            return account_lifecycle.close_account(
                account_id=account_id, role=role, fanout=fanout,
                supabase_client=supabase_client, agents=agents,
            )
        except LifecycleError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.post("/masters/{account_id}/profile")
    def upsert_master_profile(
        account_id: str, body: MasterProfileRequest, authorization: str | None = Header(default=None),
    ):
        user_id = _authenticate(authorization)
        role = _resolve_owned_account(fanout, account_user_map, account_id, user_id)
        if role != "master":
            raise HTTPException(status_code=422, detail="Only master accounts can have a profile")
        try:
            return master_profiles.upsert_profile(
                account_id=account_id, user_id=user_id, display_name=body.display_name,
                bio=body.bio, is_public=body.is_public, supabase_client=supabase_client,
            )
        except MasterProfileError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/masters/directory")
    def directory(authorization: str | None = Header(default=None)):
        _authenticate(authorization)  # any logged-in user can browse, just not anonymous scraping
        return master_profiles.list_public_masters(supabase_client)

    @app.get("/accounts/{account_id}/trades")
    def trades(account_id: str, authorization: str | None = Header(default=None)):
        user_id = _authenticate(authorization)
        role = _resolve_owned_account(fanout, account_user_map, account_id, user_id)
        agent = fanout.master_agents.get(account_id) if role == "master" else fanout.follower_agents.get(account_id)
        return trade_history.get_account_trade_history(agent)

    @app.get("/masters/{account_id}/trades")
    def public_master_trades(account_id: str, authorization: str | None = Header(default=None)):
        """The gap that made the directory useless for actually deciding
        who to follow: /accounts/{id}/trades only ever worked for the
        account's OWNER. This is the same underlying data, gated instead
        by the master's own public opt-in (master_profiles.is_public) -
        any authenticated user can call this for any master who's chosen
        to be visible, nobody else's data is reachable through it."""
        _authenticate(authorization)
        if not master_profiles.is_public_master(account_id, supabase_client):
            raise HTTPException(status_code=404, detail=f"No public master profile for {account_id}")
        agent = fanout.master_agents.get(account_id)
        if agent is None:
            raise HTTPException(status_code=409, detail=f"Master {account_id} has no running agent right now")
        return trade_history.get_account_trade_history(agent)

    return app

    