"""Drives /payments/checkout: resolves what's being paid for (wallet top-up
vs. a package), quotes it in the payer's currency, builds a Flutterwave
payment_method, and initiates the charge. Nothing here credits the wallet or
activates a package - that only ever happens in webhooks.py once Flutterwave
confirms the charge actually succeeded.
"""
from __future__ import annotations

import uuid
from typing import Any

from ..billing.billing import BillingError, get_package
from ..masters.challenges import ChallengeError, assert_can_enroll
from . import currencies, intents
from .currencies import CurrencyError
from .flutterwave_client import FlutterwaveError, flutterwave
from .intents import PaymentIntentError

# Card and bank-transfer are deliberately NOT implemented yet - see
# _build_payment_method below for exactly why each is blocked, rather than
# silently sending Flutterwave a payload that would fail (or worse, one that
# happens to "work" against a sandbox scenario but mishandles real card data).
SUPPORTED_METHODS = ("mobilemoney",)


class CheckoutError(Exception):
    """Raised for any failure here. Message is safe to surface to an API caller."""


def _resolve_amount_usd(*, purpose: str, amount_usd: float | None, package_code: str | None,
                         challenge_id: str | None, account_id: str, supabase_client: Any,
                         ) -> tuple[float, str | None, str | None]:
    if purpose == "wallet_topup":
        if not amount_usd or amount_usd <= 0:
            raise CheckoutError("amount_usd must be a positive number for wallet_topup")
        return amount_usd, None, None

    if purpose == "package":
        if not package_code:
            raise CheckoutError("package_code is required for purpose=package")
        try:
            package = get_package(package_code, supabase_client)
        except BillingError as exc:
            raise CheckoutError(str(exc)) from exc
        # infra_fee is the package's USD price - see billing.py's get_package
        # docstring: pricing lives in Supabase, this is the one source of
        # truth the frontend's pricing page also reads directly.
        return float(package["infra_fee"]), package_code, None

    if purpose == "challenge_entry":
        if not challenge_id:
            raise CheckoutError("challenge_id is required for purpose=challenge_entry")
        # Pre-checks the fee, the active flag, the already-enrolled guard, and
        # the challenge-1 gate BEFORE ever taking payment - no point charging
        # someone for an enrollment that was always going to be rejected.
        try:
            challenge = assert_can_enroll(account_id, challenge_id, supabase_client)
        except ChallengeError as exc:
            raise CheckoutError(str(exc)) from exc
        return float(challenge["fee"]), None, challenge_id

    raise CheckoutError(f"Unknown purpose {purpose!r}")


def _build_payment_method(*, method: str, currency: str, phone_number: str | None,
                           network: str | None) -> dict:
    if method == "mobilemoney":
        if not phone_number:
            raise CheckoutError("phone_number is required for method=mobilemoney")
        if not network:
            raise CheckoutError("network is required for method=mobilemoney")
        try:
            country_code = currencies.calling_code_for(currency)
        except CurrencyError as exc:
            raise CheckoutError(str(exc)) from exc
        return {
            "type": "mobile_money",
            "mobile_money": {
                "country_code": country_code,
                "network": network,
                "phone_number": phone_number,
            },
        }

    if method == "card":
        # Flutterwave's orchestrator requires field-level ENCRYPTED card data
        # (nonce + encrypted_card_number/expiry/cvv - see
        # payment-orchestrator-flow docs) produced by Flutterwave's
        # client-side encryption SDK running in the payer's browser. Our
        # CreatePaymentInput contract has no field for this at all, and this
        # backend never sees a raw card number - by design, that's PCI scope
        # we don't want. Rather than send Flutterwave a payload missing
        # required encrypted fields (which would just fail with a confusing
        # validation error), fail clearly here: card needs the frontend
        # wired up with Flutterwave's card SDK first.
        raise CheckoutError(
            "Card payments require Flutterwave's client-side card encryption SDK, which "
            "isn't wired up in the frontend yet - use mobilemoney for now, or add the SDK "
            "integration and pass the resulting encrypted card fields through here."
        )

    if method == "banktransfer":
        # Pay-with-bank-transfer (PWBT) is NOT part of the direct-charges
        # orchestrator payload at all - it's a separate flow (create a
        # customer, then a virtual account, collect BVN/NIN for NGN,
        # NGN/GHS only) documented at
        # https://developer.flutterwave.com/docs/pay-with-bank-transfer.
        # CreatePaymentInput has no fields for that (BVN/NIN, virtual
        # account display), so this isn't wired up yet either.
        raise CheckoutError(
            "Bank transfer requires Flutterwave's separate virtual-account flow (customer + "
            "BVN/NIN, NGN/GHS only), which isn't wired up yet - use mobilemoney for now."
        )

    raise CheckoutError(f"Unknown payment method {method!r}")


def initiate_checkout(
    *, account_id: str, user_id: str, purpose: str, amount_usd: float | None, package_code: str | None,
    challenge_id: str | None, currency: str, method: str, phone_number: str | None, network: str | None,
    redirect_url: str, supabase_client: Any,
) -> dict:
    resolved_amount_usd, resolved_package_code, resolved_challenge_id = _resolve_amount_usd(
        purpose=purpose, amount_usd=amount_usd, package_code=package_code, challenge_id=challenge_id,
        account_id=account_id, supabase_client=supabase_client,
    )

    try:
        quote = currencies.quote_usd(resolved_amount_usd, currency)
    except CurrencyError as exc:
        raise CheckoutError(str(exc)) from exc

    payment_method = _build_payment_method(
        method=method, currency=currency, phone_number=phone_number, network=network,
    )

    reference = f"pay{uuid.uuid4().hex}"
    intents.record_intent(
        reference=reference, account_id=account_id, user_id=user_id, purpose=purpose,
        package_code=resolved_package_code, challenge_id=resolved_challenge_id, amount_usd=resolved_amount_usd,
        currency=currency, amount_charged=quote["amount_charged"], method=method, supabase_client=supabase_client,
    )

    try:
        charge = flutterwave.create_direct_charge(
            reference=reference,
            currency=currency,
            amount=quote["amount_charged"],
            payment_method=payment_method,
            customer={"phone": {"number": phone_number}} if phone_number else {},
            redirect_url=redirect_url,
            meta={"user_id": user_id, "account_id": account_id, "purpose": purpose},
        )
    except FlutterwaveError as exc:
        intents.finalize_intent(reference, "failed", supabase_client)
        raise CheckoutError(f"Flutterwave error: {exc}") from exc

    data = charge.get("data", {})
    charge_id = data.get("id")
    if charge_id:
        intents.set_charge_id(reference, charge_id, supabase_client)

    next_action = data.get("next_action") or {}
    checkout_url = None
    if next_action.get("type") == "redirect_url":
        checkout_url = (next_action.get("redirect_url") or {}).get("url")

    return {
        "reference": reference,
        "status": data.get("status", "pending"),
        "amount_usd": resolved_amount_usd,
        "amount_charged": quote["amount_charged"],
        "currency": currency,
        "method": method,
        "checkout_url": checkout_url,
        "credited": False,
        "message": (next_action.get("payment_instruction") or {}).get("note"),
    }