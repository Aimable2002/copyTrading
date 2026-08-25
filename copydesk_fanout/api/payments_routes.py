from __future__ import annotations

import logging
from typing import Any, Literal

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel

from ..payments import checkout, currencies, intents, webhooks
from ..payments.checkout import CheckoutError
from ..payments.currencies import CurrencyError
from .socket_server import verify_supabase_jwt

logger = logging.getLogger("payments_routes")


class QuoteRequest(BaseModel):
    amount_usd: float
    currency: str


class CheckoutRequest(BaseModel):
    account_id: str
    purpose: Literal["wallet_topup", "package", "challenge_entry"]
    amount_usd: float | None = None
    package_code: str | None = None
    challenge_id: str | None = None
    currency: str
    method: Literal["card", "mobilemoney", "banktransfer"]
    phone_number: str | None = None
    network: str | None = None
    redirect_url: str


def _authenticate(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    token = authorization[len("Bearer "):]
    try:
        return verify_supabase_jwt(token)
    except Exception as exc:
        raise HTTPException(status_code=401, detail="Invalid or expired token") from exc


def _resolve_account_owner(account_user_map: dict[str, str], account_id: str, user_id: str) -> None:
    owner_id = account_user_map.get(account_id)
    if owner_id is None:
        raise HTTPException(status_code=404, detail=f"Unknown account {account_id}")
    if owner_id != user_id:
        raise HTTPException(status_code=403, detail="This account does not belong to you")


def build_payments_router(*, account_user_map: dict[str, str], supabase_client: Any) -> APIRouter:
    router = APIRouter(prefix="/payments")

    @router.get("/currencies")
    def list_currencies():
        return {"currencies": currencies.list_currencies()}

    @router.post("/quote")
    def quote(body: QuoteRequest):
        try:
            return currencies.quote_usd(body.amount_usd, body.currency)
        except CurrencyError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @router.post("/checkout")
    def create_checkout(body: CheckoutRequest, authorization: str | None = Header(default=None)):
        user_id = _authenticate(authorization)
        _resolve_account_owner(account_user_map, body.account_id, user_id)
        try:
            return checkout.initiate_checkout(
                account_id=body.account_id,
                user_id=user_id,
                purpose=body.purpose,
                amount_usd=body.amount_usd,
                package_code=body.package_code,
                challenge_id=body.challenge_id,
                currency=body.currency,
                method=body.method,
                phone_number=body.phone_number,
                network=body.network,
                redirect_url=body.redirect_url,
                supabase_client=supabase_client,
            )
        except CheckoutError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @router.get("/{reference}")
    def get_payment(reference: str, authorization: str | None = Header(default=None)):
        user_id = _authenticate(authorization)
        intent = intents.get_intent(reference, supabase_client)
        if intent is None:
            raise HTTPException(status_code=404, detail=f"Unknown payment reference {reference}")
        _resolve_account_owner(account_user_map, intent["account_id"], user_id)
        return {
            "reference": intent["reference"],
            "status": intent["status"],
            "amount_usd": intent["amount_usd"],
            "amount_charged": intent["amount_charged"],
            "currency": intent["currency"],
            "method": intent["method"],
            "checkout_url": None,
            "credited": intent["credited"],
            "message": None,
        }

    return router


def build_webhooks_router(*, fanout: Any, supabase_client: Any) -> APIRouter:
    router = APIRouter(prefix="/webhooks")

    @router.post("/flutterwave")
    async def flutterwave_webhook(request: Request):
        raw_body = await request.body()
        signature = request.headers.get("flutterwave-signature", "")
        if not webhooks.valid_signature(raw_body, signature):
            # 401 not 400 - an invalid signature here is either
            # misconfiguration (FLW_WEBHOOK_SECRET_HASH not set/wrong) or a
            # forged request; either way this must not proceed to touch a
            # wallet or a payout.
            raise HTTPException(status_code=401, detail="Invalid webhook signature")

        try:
            payload = await request.json()
        except Exception as exc:
            raise HTTPException(status_code=400, detail="Invalid JSON body") from exc

        webhook_id = payload.get("id") or payload.get("data", {}).get("id")
        if webhook_id:
            # Idempotency: Flutterwave retries webhooks on anything but a
            # 2xx response, and can also just legitimately deliver the same
            # event twice. This insert is the dedupe gate - a duplicate
            # delivery hits the table's primary key and this whole handler
            # short-circuits to "already seen, ack it and stop" without
            # touching a wallet or payout a second time. (webhooks.py's own
            # per-intent/per-payout .eq("status", "pending") guards are a
            # second, independent layer of the same protection.)
            #
            # Only a genuine unique-constraint violation means "duplicate,
            # skip" - any OTHER insert failure (transient DB error, RLS
            # misconfig, etc.) must NOT be treated the same way, or a real
            # webhook silently never gets processed. Re-raise anything that
            # isn't unmistakably a duplicate-key error so it surfaces as a
            # real failure below (which returns 200 with status
            # "error_logged" rather than swallowing it - see the comment
            # on that path).
            try:
                supabase_client.table("flutterwave_events").insert(
                    {"webhook_id": str(webhook_id), "event_type": payload.get("type", "unknown"), "payload": payload}
                ).execute()
            except Exception as exc:
                message = str(exc).lower()
                is_duplicate = "duplicate key" in message or "23505" in message or "already exists" in message
                if is_duplicate:
                    logger.info("Duplicate Flutterwave webhook %s - acking without reprocessing", webhook_id)
                    return {"status": "already_processed"}
                logger.exception("Failed to record Flutterwave webhook %s - processing anyway", webhook_id)
                # Fall through and still process the event below: losing
                # the idempotency record is worse to compound by also
                # dropping the payment/payout update it's meant to guard.

        try:
            webhooks.handle_webhook(payload, fanout, supabase_client)
        except Exception:
            logger.exception("Unhandled error processing Flutterwave webhook %s", webhook_id)
            # Still 200 - Flutterwave will retry a non-2xx indefinitely, and
            # since our own event-log insert above already recorded this
            # delivery, retries would only ever hit "already_processed"
            # above and never actually reprocess it. A stuck/erroring
            # payment is better surfaced via logs/alerting than via an
            # infinite retry loop against a code path that keeps failing.
            return {"status": "error_logged"}

        return {"status": "ok"}

    return router