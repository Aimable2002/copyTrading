from __future__ import annotations

import logging
import os
from typing import Any

import jwt
from fastapi import APIRouter, Header, HTTPException

# from . import master_profiles, master_rate, payouts, profit_share, roster, trade_history
from . import admin_analytics

from ..masters import master_profiles, roster
from ..billing import payouts, profit_share, master_rate
from ..core import trade_history
from .socket_server import _get_jwks_client

logger = logging.getLogger("admin_routes")


def _authenticate_admin(authorization: str | None) -> str:
    """Deliberately separate from api_server._authenticate: that function
    only ever needs to return the caller's user id, so it discards the
    rest of the JWT payload. Admin routes need `app_metadata.is_admin`
    off the same token, so this does its own decode rather than changing
    the shared helper (and everything that already depends on its return
    shape - regular /accounts, /masters routes, the socket server auth).
    """
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    token = authorization[len("Bearer "):]

    try:
        header = jwt.get_unverified_header(token)
        algorithm = header.get("alg", "HS256")
        if algorithm == "HS256":
            secret = os.environ.get("SUPABASE_JWT_SECRET")
            if not secret:
                raise RuntimeError("SUPABASE_JWT_SECRET is not set")
            payload = jwt.decode(token, secret, algorithms=["HS256"], audience="authenticated", leeway=1200)
        else:
            signing_key = _get_jwks_client().get_signing_key_from_jwt(token)
            payload = jwt.decode(token, signing_key.key, algorithms=[algorithm], audience="authenticated")
    except Exception as exc:  # jwt.InvalidTokenError and subclasses
        raise HTTPException(status_code=401, detail="Invalid or expired token") from exc

    # app_metadata can only be written server-side (Supabase dashboard or
    # the admin API with the service-role key) - a signed-in user has no
    # way to set this on themselves, same trust assumption the frontend's
    # useIsAdmin hook already relies on.
    if not payload.get("app_metadata", {}).get("is_admin"):
        raise HTTPException(status_code=403, detail="Admin access required")

    return payload["sub"]


def build_admin_router(*, fanout: Any, supabase_client: Any) -> APIRouter:
    router = APIRouter(prefix="/admin")

    @router.get("/masters")
    def list_masters(authorization: str | None = Header(default=None)):
        _authenticate_admin(authorization)
        masters = master_profiles.list_all_masters(supabase_client)
        for m in masters:
            m["follower_count"] = roster.count_active_followers(m["account_id"], supabase_client)
        return masters

    @router.post("/masters/{account_id}/public")
    def set_master_public(
        account_id: str, body: dict, authorization: str | None = Header(default=None),
    ):
        _authenticate_admin(authorization)
        is_public = body.get("is_public")
        if not isinstance(is_public, bool):
            raise HTTPException(status_code=422, detail="is_public must be a boolean")
        master_profiles.set_public_status(account_id, is_public, supabase_client)
        return {"account_id": account_id, "is_public": is_public}

    @router.get("/masters/{account_id}")
    def get_master_detail(account_id: str, authorization: str | None = Header(default=None)):
        _authenticate_admin(authorization)

        profile = master_profiles.get_own_profile(account_id, supabase_client)
        if profile is None:
            raise HTTPException(status_code=404, detail=f"No master profile for {account_id}")

        # Performance-fee rate feature disabled - platform is
        # subscription-only now. Kept commented rather than deleted.
        # rate = master_rate.get_current_rate(account_id, supabase_client)
        earnings = profit_share.get_master_earnings(account_id, supabase_client)
        follower_count = roster.count_active_followers(account_id, supabase_client)

        agent = fanout.master_agents.get(account_id)
        trades = trade_history.get_account_trade_history(agent) if agent is not None else []

        return {
            **profile,
            "earnings": earnings,
            "follower_count": follower_count,
            "trades": trades,
            "agent_running": agent is not None,
        }

    @router.get("/payouts")
    def list_pending_payouts(authorization: str | None = Header(default=None)):
        _authenticate_admin(authorization)
        return payouts.list_pending_payouts(supabase_client)

    @router.post("/payouts/{payout_id}/approve")
    def approve_payout(payout_id: str, authorization: str | None = Header(default=None)):
        _authenticate_admin(authorization)
        try:
            return payouts.approve_payout(payout_id, supabase_client)
        except payouts.PayoutError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @router.post("/payouts/{payout_id}/reject")
    def reject_payout(payout_id: str, body: dict, authorization: str | None = Header(default=None)):
        _authenticate_admin(authorization)
        reason = body.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            raise HTTPException(status_code=422, detail="reason is required")
        try:
            return payouts.reject_payout(payout_id, reason, supabase_client)
        except payouts.PayoutError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @router.get("/summary")
    def summary(authorization: str | None = Header(default=None)):
        _authenticate_admin(authorization)
        return admin_analytics.get_admin_summary(supabase_client)

    @router.get("/analytics/revenue")
    def revenue(authorization: str | None = Header(default=None)):
        _authenticate_admin(authorization)
        return admin_analytics.get_revenue_by_month(supabase_client)

    @router.get("/analytics/growth")
    def growth(authorization: str | None = Header(default=None)):
        _authenticate_admin(authorization)
        return admin_analytics.get_growth_by_month(supabase_client)

    @router.get("/analytics/symbol-exposure")
    def symbol_exposure(authorization: str | None = Header(default=None)):
        _authenticate_admin(authorization)
        return admin_analytics.get_symbol_exposure(supabase_client)

    @router.get("/analytics/top-masters")
    def top_masters(authorization: str | None = Header(default=None)):
        _authenticate_admin(authorization)
        return admin_analytics.get_top_masters(supabase_client, fanout)

    @router.get("/users")
    def users(authorization: str | None = Header(default=None)):
        _authenticate_admin(authorization)
        return admin_analytics.list_all_users(supabase_client)

    return router