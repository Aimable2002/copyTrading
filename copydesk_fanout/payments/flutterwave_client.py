"""Thin client for the Flutterwave v4 API.

Ported from the reference `reserve-backend` integration (same product owner
built that against Flutterwave v4 already and confirmed the shapes below
against Flutterwave's own docs at https://developer.flutterwave.com/docs -
this is not a guess at the schema, every payload shape here matches either
that reference implementation or the current v4 docs directly).

- Auth: OAuth2 client_credentials against FLW_TOKEN_URL, token cached in
  memory and refreshed a little before it actually expires.
- Collections: the orchestrator `direct-charges` endpoint (creates customer +
  payment method + charge in one call) - see
  https://developer.flutterwave.com/docs/payment-orchestrator-flow
- Payouts: the `direct-transfers` endpoint - see
  https://developer.flutterwave.com/docs/direct-transfer-flow
- FX quoting: the `transfers/rates` endpoint - see
  https://developer.flutterwave.com/docs/real-time-fx-conversion
- Every mutating call gets a fresh X-Trace-Id and the caller-supplied
  X-Idempotency-Key (we pass our own `reference` for this, so a retried
  request - e.g. from our own network-error retry, or a person double
  tapping "Pay" - can't double-charge/double-pay).
"""
from __future__ import annotations

import os
import time
import uuid
from typing import Any

import requests


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"Missing required environment variable {name}. Set FLW_CLIENT_ID, "
            f"FLW_CLIENT_SECRET, and (optionally) FLW_ENV/FLW_WEBHOOK_SECRET_HASH."
        )
    return value


_FLW_TOKEN_URL = os.environ.get(
    "FLW_TOKEN_URL", "https://idp.flutterwave.com/realms/flutterwave/protocol/openid-connect/token"
)
_FLW_API_BASE_SANDBOX = os.environ.get(
    "FLW_API_BASE_SANDBOX", "https://developersandbox-api.flutterwave.com"
)
_FLW_API_BASE_LIVE = os.environ.get("FLW_API_BASE_LIVE")


def _api_base() -> str:
    env = os.environ.get("FLW_ENV", "sandbox").lower()
    return _FLW_API_BASE_LIVE if env == "live" else _FLW_API_BASE_SANDBOX


class FlutterwaveError(Exception):
    def __init__(self, message: str, status_code: int | None = None, payload: dict | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload or {}


class FlutterwaveClient:
    def __init__(self):
        self._access_token: str | None = None
        self._expires_at: float = 0

    # -- auth -----------------------------------------------------------------
    def _get_access_token(self) -> str:
        if self._access_token and time.time() < self._expires_at - 30:
            return self._access_token

        resp = requests.post(
            _FLW_TOKEN_URL,
            data={
                "client_id": _require_env("FLW_CLIENT_ID"),
                "client_secret": _require_env("FLW_CLIENT_SECRET"),
                "grant_type": "client_credentials",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=15,
        )
        if resp.status_code != 200:
            raise FlutterwaveError(
                f"Failed to obtain Flutterwave access token: {resp.status_code}",
                status_code=resp.status_code,
                payload=_safe_json(resp),
            )
        data = resp.json()
        self._access_token = data["access_token"]
        self._expires_at = time.time() + data.get("expires_in", 300)
        return self._access_token

    # -- low-level request ------------------------------------------------------
    def _request(self, method: str, path: str, idempotency_key: str | None = None, **kwargs):
        url = f"{_api_base()}{path}"
        headers = kwargs.pop("headers", {})
        headers.update(
            {
                "Authorization": f"Bearer {self._get_access_token()}",
                "Content-Type": "application/json",
                "X-Trace-Id": str(uuid.uuid4()),
            }
        )
        if idempotency_key:
            headers["X-Idempotency-Key"] = idempotency_key

        resp = requests.request(method, url, headers=headers, timeout=30, **kwargs)
        body = _safe_json(resp)
        if resp.status_code >= 400:
            # v4 nests error detail under `error`, e.g.
            # {"status": "failed", "error": {"type": ..., "code": ..., "message": ..., "validation_errors": [...]}}
            error_obj = body.get("error") if isinstance(body, dict) else None
            message = None
            if isinstance(error_obj, dict):
                message = error_obj.get("message")
                if not message and error_obj.get("validation_errors"):
                    message = "; ".join(
                        f"{e.get('field_name')}: {e.get('message')}" for e in error_obj["validation_errors"]
                    )
            if not message and isinstance(body, dict):
                message = body.get("message")
            if not message:
                message = f"Flutterwave error {resp.status_code}"

            raise FlutterwaveError(message, status_code=resp.status_code, payload=body)
        return body

    # -- collections (deposits) --------------------------------------------------
    def create_direct_charge(
        self, *, reference: str, currency: str, amount, payment_method: dict, customer: dict,
        redirect_url: str | None = None, meta: dict | None = None,
    ):
        """POST /orchestration/direct-charges"""
        payload: dict[str, Any] = {
            "reference": reference,
            "currency": currency,
            "amount": amount,
            "payment_method": payment_method,
            "customer": customer,
        }
        if redirect_url:
            payload["redirect_url"] = redirect_url
        if meta:
            payload["meta"] = meta
        return self._request(
            "POST", "/orchestration/direct-charges", idempotency_key=reference, json=payload
        )

    def get_charge(self, charge_id: str):
        return self._request("GET", f"/charges/{charge_id}")

    # -- payouts (withdrawals) ------------------------------------------------------
    def create_direct_transfer(
        self, *, reference: str, source_currency: str, destination_currency: str, amount_value,
        transfer_type: str, recipient: dict, action: str = "instant",
    ):
        """POST /direct-transfers. transfer_type: 'bank' | 'mobile_money' | 'wallet'."""
        payload = {
            "action": action,
            "type": transfer_type,
            "reference": reference,
            "payment_instruction": {
                "source_currency": source_currency,
                "destination_currency": destination_currency,
                "amount": {"applies_to": "destination_currency", "value": amount_value},
                "recipient": recipient,
            },
        }
        return self._request("POST", "/direct-transfers", idempotency_key=reference, json=payload)

    def get_transfer(self, transfer_id: str):
        return self._request("GET", f"/transfers/{transfer_id}")

    # -- FX quoting ---------------------------------------------------------------
    def get_rate(self, *, source_currency: str, destination_currency: str, destination_amount):
        """POST /transfers/rates. Returns how much `source_currency` is needed
        to produce `destination_amount` of `destination_currency` - i.e. call
        with destination_currency="USD" to find out what a USD price costs in
        a payer's local currency. See
        https://developer.flutterwave.com/docs/real-time-fx-conversion
        """
        payload = {
            "source": {"currency": source_currency},
            "destination": {"currency": destination_currency, "amount": destination_amount},
        }
        return self._request("POST", "/transfers/rates", json=payload)


def _safe_json(resp: requests.Response):
    try:
        return resp.json()
    except ValueError:
        return {"raw": resp.text}


flutterwave = FlutterwaveClient()