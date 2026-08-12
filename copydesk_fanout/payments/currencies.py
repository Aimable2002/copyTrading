"""Supported payment currencies + USD quoting.

Prices in this product are denominated in USD (see billing.py's `infra_fee`
and the frontend's payments.ts comment: "The backend owns the FX conversion
... always the source of truth"). This mirrors the frontend's own
FALLBACK_CURRENCIES list (src/lib/payments.ts) exactly - country codes and
mobile_money flags must stay in sync between the two if either changes,
since /payments/currencies is meant to supersede that hardcoded fallback,
not diverge from it.
"""
from __future__ import annotations

from typing import Any

from .flutterwave_client import FlutterwaveError, flutterwave

# code -> (name, mobile_money supported, ISO-3166 alpha-2 country)
SUPPORTED_CURRENCIES: dict[str, tuple[str, bool, str]] = {
    "USD": ("US Dollar", False, "US"),
    "RWF": ("Rwandan Franc", True, "RW"),
    "KES": ("Kenyan Shilling", True, "KE"),
    "UGX": ("Ugandan Shilling", True, "UG"),
    "TZS": ("Tanzanian Shilling", True, "TZ"),
    "GHS": ("Ghanaian Cedi", True, "GH"),
    "XAF": ("Central African CFA", True, "CM"),
    "XOF": ("West African CFA", True, "CI"),
    "NGN": ("Nigerian Naira", False, "NG"),
    "ZAR": ("South African Rand", False, "ZA"),
}

# ISO-3166 alpha-2 -> ITU-T calling code, used to build the `country_code`
# field Flutterwave's mobile_money payment_method requires (see
# payment-orchestrator-flow docs) - standard dialing codes, not
# Flutterwave-specific, safe to hardcode.
_CALLING_CODE: dict[str, str] = {
    "US": "1", "RW": "250", "KE": "254", "UG": "256", "TZ": "255",
    "GH": "233", "CM": "237", "CI": "225", "NG": "234", "ZA": "27",
}


class CurrencyError(Exception):
    """Raised for any failure here. Message is safe to surface to an API caller."""


def list_currencies() -> list[dict]:
    return [
        {"code": code, "name": name, "rate_per_usd": 1 if code == "USD" else None,
         "mobile_money": mobile_money, "country": country}
        for code, (name, mobile_money, country) in SUPPORTED_CURRENCIES.items()
    ]


def calling_code_for(currency: str) -> str:
    entry = SUPPORTED_CURRENCIES.get(currency)
    if entry is None:
        raise CurrencyError(f"Unsupported currency {currency}")
    country = entry[2]
    code = _CALLING_CODE.get(country)
    if code is None:
        raise CurrencyError(f"No dialing code configured for {currency} ({country})")
    return code


def quote_usd(amount_usd: float, currency: str) -> dict[str, Any]:
    """How much `currency` a payer needs to pay to cover `amount_usd`.

    Returns {"amount_usd", "amount_charged", "currency", "rate"}. USD is a
    no-op (amount_charged == amount_usd, rate == 1) - Flutterwave's rate
    endpoint doesn't need to be called for a currency-to-itself quote.
    """
    if currency not in SUPPORTED_CURRENCIES:
        raise CurrencyError(f"Unsupported currency {currency}")
    if amount_usd <= 0:
        raise CurrencyError("amount_usd must be positive")

    if currency == "USD":
        return {"amount_usd": amount_usd, "amount_charged": amount_usd, "currency": "USD", "rate": 1.0}

    try:
        resp = flutterwave.get_rate(
            source_currency=currency, destination_currency="USD", destination_amount=amount_usd,
        )
    except FlutterwaveError as exc:
        raise CurrencyError(f"Could not fetch a live rate for {currency}: {exc}") from exc

    data = resp.get("data", {})
    try:
        amount_charged = float(data["source"]["amount"])
    except (KeyError, TypeError, ValueError) as exc:
        raise CurrencyError(f"Unexpected rate response shape for {currency}: {data}") from exc

    return {
        "amount_usd": amount_usd,
        "amount_charged": amount_charged,
        "currency": currency,
        "rate": amount_charged / amount_usd,
    }