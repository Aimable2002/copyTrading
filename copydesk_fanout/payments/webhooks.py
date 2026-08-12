"""Flutterwave webhook handling.

This is the ONLY place a wallet gets credited or a package gets activated
from a payment - /payments/checkout only ever initiates a charge and never
gives value, exactly so a person can't get a free top-up by hitting the
checkout endpoint without ever actually paying.

Signature verification: HMAC-SHA256 of the raw request body, keyed by the
dashboard-configured secret hash, base64-encoded, compared against the
`flutterwave-signature` header - see
https://developer.flutterwave.com/docs/webhooks#verifying-webhook-signatures
(the docs page has an inconsistency where a later code sample does a bare
string comparison instead - the HMAC method is the one spelled out in prose
and matches the crypto example, so that's what's implemented here).
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import os
from typing import Any

from ..billing import billing, payouts, wallet
from . import intents
from .flutterwave_client import FlutterwaveError, flutterwave

logger = logging.getLogger("payments.webhooks")


def valid_signature(raw_body: bytes, signature: str) -> bool:
    secret = os.environ.get("FLW_WEBHOOK_SECRET_HASH")
    if not signature or not secret:
        return False
    computed = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).digest()
    computed_b64 = base64.b64encode(computed).decode("utf-8")
    return hmac.compare_digest(computed_b64, signature)


def handle_webhook(payload: dict, fanout: Any, supabase_client: Any) -> None:
    """Caller (the route) has already verified the signature and the
    webhook_id idempotency insert - this only dispatches by event type.
    `fanout` is the live FanoutCore, needed because a package activation
    that lands the account in a grace period must be able to actually pause
    the running agent (billing.select_package -> _enter_grace -> pause_account
    all require a real fanout, not a stand-in)."""
    event_type = payload.get("type")
    data = payload.get("data", {})

    if event_type == "charge.completed":
        _handle_charge_completed(data, fanout, supabase_client)
    elif event_type == "transfer.disburse":
        _handle_transfer_disburse(data, supabase_client)
    # Unhandled event types (refunds, chargebacks, etc.) are logged via the
    # flutterwave_events row the route already inserted, but don't move any
    # balance yet.


def _handle_charge_completed(data: dict, fanout: Any, supabase_client: Any) -> None:
    reference = data.get("reference")
    charge_id = data.get("id")
    if not reference:
        return

    intent = intents.get_intent(reference, supabase_client)
    if intent is None:
        logger.warning("charge.completed for unknown reference %s - ignoring", reference)
        return
    if intent["status"] != "pending":
        return  # already finalized - duplicate delivery, see intents.finalize_intent

    # Best practice per Flutterwave's own docs: re-verify against their API
    # rather than trusting the webhook body alone.
    verified_status = data.get("status")
    verified_amount = data.get("amount")
    verified_currency = data.get("currency")
    try:
        verify_resp = flutterwave.get_charge(charge_id)
        verify_data = verify_resp.get("data", {})
        verified_status = verify_data.get("status", verified_status)
        verified_amount = verify_data.get("amount", verified_amount)
        verified_currency = verify_data.get("currency", verified_currency)
    except FlutterwaveError:
        # Fall back to the webhook's own body if the verification call
        # itself fails - we still only ever trust a signed webhook to get
        # here at all, so this is a reasonable degrade-gracefully path
        # rather than silently dropping a real payment.
        logger.exception("Charge verification call failed for %s - using webhook body", reference)

    if verified_status != "succeeded":
        if verified_status in ("failed", "cancelled"):
            intents.finalize_intent(reference, "failed", supabase_client)
        return  # still pending/requires_action - leave pending, wait for a later webhook

    if (
        verified_amount is not None
        and abs(float(verified_amount) - float(intent["amount_charged"])) > 0.01
    ) or (verified_currency is not None and verified_currency != intent["currency"]):
        # Amount/currency mismatch between what we asked for and what
        # Flutterwave confirms is a sign of tampering or a serious bug -
        # do NOT give value, leave pending for manual investigation.
        logger.error(
            "charge.completed amount/currency MISMATCH for %s: expected %s %s, got %s %s - "
            "not crediting, needs manual review",
            reference, intent["amount_charged"], intent["currency"], verified_amount, verified_currency,
        )
        return

    account_id = intent["account_id"]
    try:
        wallet.top_up(account_id, float(intent["amount_usd"]), supabase_client)
        if intent["purpose"] == "package" and intent.get("package_code"):
            # Best-effort: the wallet credit above already succeeded (money
            # is safely reflected), so a failure here shouldn't be treated
            # as "the payment failed" - it's an activation issue the person
            # can retry from billing.select_package() directly, with funds
            # already in their wallet.
            try:
                billing.select_package(
                    account_id=account_id, package_code=intent["package_code"], role="follower",
                    fanout=fanout, supabase_client=supabase_client,
                )
            except Exception:
                logger.exception(
                    "Package activation failed for %s after successful payment %s - "
                    "wallet was credited, person can retry select-package",
                    account_id, reference,
                )
        intents.finalize_intent(reference, "successful", supabase_client, credited=True)
    except Exception:
        logger.exception("Failed to credit wallet for %s after confirmed payment - needs manual review", reference)
        # Deliberately don't finalize as failed - the payment DID succeed on
        # Flutterwave's side, leaving it pending (not failed, not credited)
        # keeps this visible for manual reconciliation instead of quietly
        # losing a real payment.


def _handle_transfer_disburse(data: dict, supabase_client: Any) -> None:
    """Finalizes a master payout that was sent via a real Flutterwave
    transfer - see billing/payouts.py's approve_payout(), which only sets
    transfer_id when it actually calls Flutterwave (falls back to the old
    DB-only 'paid' flip otherwise, which never sets transfer_id and so is
    never touched by this handler)."""
    reference = data.get("reference")
    status = data.get("status")
    if not reference:
        return
    if status == "SUCCESSFUL":
        payouts.finalize_transfer(reference, "paid", supabase_client)
    elif status == "FAILED":
        payouts.finalize_transfer(reference, "rejected", supabase_client, reason="Flutterwave transfer failed")
    # PENDING: leave as-is, wait for a terminal webhook.