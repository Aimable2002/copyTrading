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
import os
from typing import Any, Literal
from urllib.parse import urlencode

from fastapi import BackgroundTasks, FastAPI, Header, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel

from ..ctrader import oauth as ctrader_oauth
from ..ctrader import pending_consent as ctrader_pending_consent
from ..ctrader.provisioning import CTraderProvisioningError, provision_ctrader_account

# from . import account_lifecycle, billing, challenges, master_profiles, master_rate, payouts, profit_share, roster, trade_history, wallet
from ..provisioning import account_lifecycle
from ..masters import challenges, master_profiles, roster
from ..billing import billing, master_rate, payouts, profit_share, wallet
from ..core import trade_history
from ..provisioning.account_lifecycle import LifecycleError
from ..billing.billing import BillingError
from ..masters.challenges import ChallengeError
from ..core.fanout_core import FanoutCore
from ..masters.master_profiles import MasterProfileError
from ..billing.master_rate import MasterRateError
from ..provisioning.provisioning import (
    ProvisioningError,
    finalize_provisioned_account,
    provision_account_finish,
    provision_account_start,
)
from ..masters.roster import RosterError
from ..core.sizing import SizingMode
from .socket_server import verify_supabase_jwt
from ..billing.wallet import WalletError

from .admin_routes import build_admin_router
from .payments_routes import build_payments_router, build_webhooks_router

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
    force_close: bool  


class MasterProfileRequest(BaseModel):
    display_name: str
    bio: str = ""


class TopUpRequest(BaseModel):
    amount: float


class SelectPackageRequest(BaseModel):
    package_code: str


class SwitchMasterRequest(BaseModel):
    master_account_id: str


class SetRateRequest(BaseModel):
    rate_percent: float
    platform_cut_percent: float


class EnrollChallengeRequest(BaseModel):
    challenge_id: str


class RequestPayoutRequest(BaseModel):
    amount: float
    recipient_name: str
    recipient_phone: str


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


def _resolve_account_owner(
    account_user_map: dict[str, str], account_id: str, user_id: str,
) -> Literal["master", "follower"]:
    owner_id = account_user_map.get(account_id)
    if owner_id is None:
        raise HTTPException(status_code=404, detail=f"Unknown account {account_id}")
    if owner_id != user_id:
        raise HTTPException(status_code=403, detail="This account does not belong to you")
    if account_id.startswith("master_"):
        return "master"
    if account_id.startswith("follower_"):
        return "follower"
    raise HTTPException(status_code=500, detail=f"Account {account_id} has an unrecognized id format")


def create_api_app(
    *,
    fanout: FanoutCore,
    supabase_client: Any,
    account_user_map: dict[str, str],
    agents: list,
) -> FastAPI:
    app = FastAPI(title="CopyDesk provisioning API")

    # Own prefix (/admin), own auth check (_authenticate_admin) - isolated
    # from every existing route above so this addition can't change the
    # behavior of anything that was already working.
    app.include_router(build_admin_router(fanout=fanout, supabase_client=supabase_client))

    # Own prefixes (/payments, /webhooks), same isolation reasoning as the
    # admin router above. /webhooks has no bearer-token auth at all -
    # Flutterwave calls it directly, so payments_routes.build_webhooks_router
    # authenticates the request itself via the HMAC signature header instead.
    app.include_router(build_payments_router(account_user_map=account_user_map, supabase_client=supabase_client))
    app.include_router(build_webhooks_router(fanout=fanout, supabase_client=supabase_client))

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.exception_handler(RequestValidationError)
    async def _log_validation_errors(request: Request, exc: RequestValidationError):
        body = await request.body()
        logger.warning(
            "422 on %s - validation errors: %s | raw body sent: %s",
            request.url.path, exc.errors(), body.decode(errors="replace"),
        )
        return JSONResponse(status_code=422, content={"detail": exc.errors()})

    @app.post("/accounts/provision")
    def provision(body: ProvisionRequest, background_tasks: BackgroundTasks, authorization: str | None = Header(default=None)):
        user_id = _authenticate(authorization)
        try:
            agent, instance_dir, terminal_path, outcome, account_id = provision_account_start(
                user_id=user_id,
                role=body.role,
                login=body.login,
                password=body.password,
                server=body.server,
                fanout=fanout,
                supabase_client=supabase_client,
                master_account_id=body.master_account_id,
                multiplier=body.multiplier,
                sizing_mode=body.sizing_mode,
            )
        except ProvisioningError as exc:
            logger.exception("Provisioning failed for user %s", user_id)
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        if outcome == "connected":
            try:
                finalize_provisioned_account(
                    agent, terminal_path,
                    user_id=user_id, role=body.role, account_id=account_id,
                    fanout=fanout, supabase_client=supabase_client,
                    account_user_map=account_user_map, agents=agents,
                    master_account_id=body.master_account_id, multiplier=body.multiplier, sizing_mode=body.sizing_mode,
                )
            except ProvisioningError as exc:
                logger.exception("Provisioning failed for user %s (account_id %s)", user_id, account_id)
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            return {"account_id": account_id, "status": "live"}

        def _resume_after_stall() -> None:
            try:
                provision_account_finish(
                    agent, instance_dir, terminal_path,
                    user_id=user_id, role=body.role, account_id=account_id,
                    fanout=fanout, supabase_client=supabase_client,
                    account_user_map=account_user_map, agents=agents,
                    master_account_id=body.master_account_id, multiplier=body.multiplier, sizing_mode=body.sizing_mode,
                )
            except ProvisioningError:
                logger.exception("Provisioning failed for user %s (account_id %s) during stalled wait", user_id, account_id)
            except Exception:  # noqa: BLE001 - a background task with no caller to reraise to must not die silently
                logger.exception("Unexpected provisioning failure for user %s (account_id %s) during stalled wait", user_id, account_id)

        background_tasks.add_task(_resume_after_stall)
        return {"account_id": account_id, "status": "awaiting_attention"}

    @app.post("/accounts/ctrader/start")
    def ctrader_start(authorization: str | None = Header(default=None)):
        """Returns a Spotware authorization_url. Frontend does a full-page
        redirect to it - not a fetch - since the consent step is Spotware's own
        hosted page, not something this API can proxy. See ctrader/oauth.py.

        cTrader's OAuth implementation does not round-trip a `state` param back
        to the callback (confirmed against a live run - only `code` arrives) -
        so who-initiated-this is tracked server-side via pending_consent.py
        instead of a signed state token. See that module's docstring for the
        current single-flight limitation."""
        user_id = _authenticate(authorization)
        ctrader_pending_consent.set_pending_user(user_id)
        try:
            url = ctrader_oauth.build_authorization_url()
        except ctrader_oauth.OAuthError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return {"authorization_url": url}

    @app.get("/accounts/ctrader/callback")
    def ctrader_callback(code: str):
        """
        Hit by the user's BROWSER as a top-level navigation (Spotware redirects
        here after consent) - not by the frontend via fetch, so there's no
        Authorization header to check, and no `state` param either (see
        ctrader_start above). Always resolves to a redirect back into the
        frontend, success or failure, so the SPA can resume - see
        _app.onboarding.tsx's ctrader_status search param.
        """
        # FRONTEND_URL should be the bare origin (e.g. https://copydesk1.netlify.app),
        # no path - this handler appends /onboarding itself.
        frontend_url = os.environ.get("FRONTEND_URL", "").rstrip("/")

        def _redirect(**params: str) -> RedirectResponse:
            return RedirectResponse(url=f"{frontend_url}/onboarding?{urlencode(params)}")

        user_id = ctrader_pending_consent.consume_pending_user()
        if user_id is None:
            return _redirect(
                ctrader_status="error",
                message="This cTrader connection attempt expired or wasn't recognized - please try connecting again.",
            )

        try:
            account_id = provision_ctrader_account(
                user_id=user_id, authorization_code=code, fanout=fanout,
                supabase_client=supabase_client, account_user_map=account_user_map, agents=agents,
            )
        except CTraderProvisioningError as exc:
            logger.exception("cTrader provisioning failed for user %s", user_id)
            return _redirect(ctrader_status="error", message=str(exc))

        return _redirect(ctrader_status="success", account_id=account_id)

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
                bio=body.bio, supabase_client=supabase_client,
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
        # print("investigating this route /accounts/{account_id}/trades :", trade_history.get_account_trade_history(agent))
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
        # print("testing what is returned for master :", trade_history.get_account_trade_history(agent))
        return trade_history.get_account_trade_history(agent)

    # ----------------------------------------------------------------
    # Wallet
    # ----------------------------------------------------------------

    @app.get("/accounts/{account_id}/wallet")
    def get_wallet(account_id: str, authorization: str | None = Header(default=None)):
        user_id = _authenticate(authorization)
        _resolve_account_owner(account_user_map, account_id, user_id)
        result = wallet.get_wallet(account_id, supabase_client)
        if result is None:
            return {"account_id": account_id, "exists": False}
        return {**result, "exists": True}

    @app.post("/accounts/{account_id}/wallet/topup")
    def topup_wallet(account_id: str, body: TopUpRequest, authorization: str | None = Header(default=None)):
        user_id = _authenticate(authorization)
        _resolve_account_owner(account_user_map, account_id, user_id)
        try:
            return wallet.top_up(account_id, body.amount, supabase_client)
        except WalletError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/accounts/{account_id}/wallet/transactions")
    def wallet_transactions(account_id: str, authorization: str | None = Header(default=None)):
        user_id = _authenticate(authorization)
        _resolve_account_owner(account_user_map, account_id, user_id)
        print("/accounts/{account_id}/wallet/transactions :", wallet.list_transactions(account_id, supabase_client))
        return wallet.list_transactions(account_id, supabase_client)

    # ----------------------------------------------------------------
    # Billing (infra + slot fee, duration-based packages)
    # ----------------------------------------------------------------

    @app.get("/accounts/{account_id}/billing")
    def get_billing(account_id: str, authorization: str | None = Header(default=None)):
        user_id = _authenticate(authorization)
        _resolve_account_owner(account_user_map, account_id, user_id)
        period = billing.get_active_period(account_id, supabase_client)
        return period if period is not None else {"account_id": account_id, "status": "none"}

    @app.post("/accounts/{account_id}/billing/select-package")
    def select_package(account_id: str, body: SelectPackageRequest, authorization: str | None = Header(default=None)):
        user_id = _authenticate(authorization)
        role = _resolve_account_owner(account_user_map, account_id, user_id)
        try:
            return billing.select_package(
                account_id=account_id, package_code=body.package_code, role=role,
                fanout=fanout, supabase_client=supabase_client,
            )
        except BillingError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.post("/accounts/{account_id}/billing/reactivate")
    def reactivate_billing(account_id: str, body: SelectPackageRequest, authorization: str | None = Header(default=None)):
        user_id = _authenticate(authorization)
        role = _resolve_account_owner(account_user_map, account_id, user_id)
        try:
            return billing.select_package(
                account_id=account_id, package_code=body.package_code, role=role,
                fanout=fanout, supabase_client=supabase_client,
            )
        except BillingError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    # ----------------------------------------------------------------
    # Roster / switch slots
    # ----------------------------------------------------------------

    @app.get("/accounts/{account_id}/roster")
    def get_roster(account_id: str, authorization: str | None = Header(default=None)):
        user_id = _authenticate(authorization)
        _resolve_account_owner(account_user_map, account_id, user_id)
        period = billing.get_active_period(account_id, supabase_client)
        if period is None:
            return {"account_id": account_id, "roster": []}
        return {"account_id": account_id, "billing_period_id": period["id"], "roster": roster.get_roster(period["id"], account_id, supabase_client)}

    @app.post("/accounts/{account_id}/roster/switch")
    def switch_master(account_id: str, body: SwitchMasterRequest, authorization: str | None = Header(default=None)):
        user_id = _authenticate(authorization)
        _resolve_account_owner(account_user_map, account_id, user_id)
        period = billing.get_active_period(account_id, supabase_client)
        if period is None:
            raise HTTPException(status_code=422, detail="No active subscription - select a package first")
        try:
            return roster.switch_master(
                billing_period_id=period["id"], follower_account_id=account_id,
                new_master_account_id=body.master_account_id, supabase_client=supabase_client,
            )
        except (RosterError, MasterRateError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    # ----------------------------------------------------------------
    # Master rate + earnings
    # ----------------------------------------------------------------

    @app.get("/masters/{account_id}/rate")
    def get_master_rate(account_id: str, authorization: str | None = Header(default=None)):
        _authenticate(authorization)  # public rate - any authenticated user, no ownership check, same as /masters/directory
        rate = master_rate.get_public_rate(account_id, supabase_client)
        if rate is None:
            raise HTTPException(status_code=404, detail=f"Master {account_id} has not set a rate yet")
        return rate

    @app.post("/masters/{account_id}/rate")
    def set_master_rate(account_id: str, body: SetRateRequest, authorization: str | None = Header(default=None)):
        user_id = _authenticate(authorization)
        role = _resolve_account_owner(account_user_map, account_id, user_id)
        if role != "master":
            raise HTTPException(status_code=422, detail="Only master accounts can set a rate")
        try:
            return master_rate.set_rate(account_id, body.rate_percent, body.platform_cut_percent, supabase_client)
        except MasterRateError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/masters/{account_id}/earnings")
    def get_master_earnings(account_id: str, authorization: str | None = Header(default=None)):
        user_id = _authenticate(authorization)
        role = _resolve_account_owner(account_user_map, account_id, user_id)
        if role != "master":
            raise HTTPException(status_code=422, detail="Only master accounts have earnings")
        return profit_share.get_master_earnings(account_id, supabase_client)

    @app.get("/masters/{account_id}/payouts")
    def list_own_payouts(account_id: str, authorization: str | None = Header(default=None)):
        user_id = _authenticate(authorization)
        role = _resolve_account_owner(account_user_map, account_id, user_id)
        if role != "master":
            raise HTTPException(status_code=422, detail="Only master accounts have payouts")
        return payouts.list_payouts_for_master(account_id, supabase_client)

    @app.post("/masters/{account_id}/payouts")
    def create_payout_request(
        account_id: str, body: RequestPayoutRequest, authorization: str | None = Header(default=None),
    ):
        user_id = _authenticate(authorization)
        role = _resolve_account_owner(account_user_map, account_id, user_id)
        if role != "master":
            raise HTTPException(status_code=422, detail="Only master accounts have payouts")
        try:
            return payouts.request_payout(
                account_id, body.amount, body.recipient_name, body.recipient_phone, supabase_client,
            )
        except payouts.PayoutError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    # ----------------------------------------------------------------
    # Challenges - browsing/enrolling/status/history. Challenge CRUD
    # itself (creating/editing a challenge) is NOT here - that's a direct-
    # Supabase admin surface, not a backend route, per the "admin uses
    # backend where necessary, plain config CRUD doesn't need it" decision.
    # ----------------------------------------------------------------

    @app.get("/challenges")
    def list_challenges_route(authorization: str | None = Header(default=None)):
        _authenticate(authorization)  # any authenticated user - same as /masters/directory
        return {"challenges": challenges.list_challenges(supabase_client)}

    @app.post("/masters/{account_id}/challenges/enroll")
    def enroll_challenge(account_id: str, body: EnrollChallengeRequest, authorization: str | None = Header(default=None)):
        user_id = _authenticate(authorization)
        role = _resolve_account_owner(account_user_map, account_id, user_id)
        if role != "master":
            raise HTTPException(status_code=422, detail="Only master accounts can enroll in challenges")
        try:
            return challenges.enroll(master_account_id=account_id, challenge_id=body.challenge_id, supabase_client=supabase_client)
        except ChallengeError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.post("/masters/{account_id}/challenges/{challenge_id}/leave")
    def leave_challenge(account_id: str, challenge_id: str, authorization: str | None = Header(default=None)):
        user_id = _authenticate(authorization)
        role = _resolve_account_owner(account_user_map, account_id, user_id)
        if role != "master":
            raise HTTPException(status_code=422, detail="Only master accounts can leave challenges")
        try:
            return challenges.leave(master_account_id=account_id, challenge_id=challenge_id, supabase_client=supabase_client)
        except ChallengeError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/masters/{account_id}/challenges/status")
    def get_challenge_status(account_id: str, authorization: str | None = Header(default=None)):
        user_id = _authenticate(authorization)
        role = _resolve_account_owner(account_user_map, account_id, user_id)
        if role != "master":
            raise HTTPException(status_code=422, detail="Only master accounts have challenge status")
        return challenges.get_status(account_id, supabase_client)

    @app.get("/masters/{account_id}/challenges/history")
    def get_challenge_history(account_id: str, authorization: str | None = Header(default=None)):
        user_id = _authenticate(authorization)
        role = _resolve_account_owner(account_user_map, account_id, user_id)
        if role != "master":
            raise HTTPException(status_code=422, detail="Only master accounts have challenge history")
        return challenges.get_history(account_id, supabase_client)

    return app