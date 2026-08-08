"""
cTrader account-level OAuth consent flow.

This is separate from proto_client.py's app-level auth (ProtoOAApplicationAuthReq,
the shared kk5 client_id/secret) - this module is about the per-account consent
step: sending a user (you, when provisioning a new master account) to Spotware's
own hosted login/consent page, then exchanging the authorization code that comes
back for an access/refresh token pair scoped to that one cTrader account.

Flow (see api/api_server.py's /accounts/ctrader/start and /accounts/ctrader/callback):
  1. build_authorization_url() -> a Spotware URL.
  2. Frontend does a full-page redirect (not a fetch) to that URL.
  3. Spotware handles login+consent, then redirects the browser to our
     redirect_uri with ?code=... only - confirmed against a live run that
     cTrader does NOT round-trip a `state` param, unlike most OAuth providers,
     so who-initiated-this is tracked separately (see pending_consent.py), not
     via a signed state token here as originally designed.
  4. exchange_code(code) -> (access_token, refresh_token, expires_at)
  5. refresh_access_token(refresh_token) -> new (access_token, expires_at) pair,
     called by token_store.py when a stored token is close to expiry.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from urllib.parse import urlencode

import requests
from ctrader_open_api import EndPoints

logger = logging.getLogger("ctrader.oauth")


class OAuthError(Exception):
    """Raised for any failure in the consent/token-exchange flow. Safe to surface to an API caller."""


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise OAuthError(f"Missing required environment variable {name}.")
    return value


def build_authorization_url(scope: str = "accounts") -> str:
    """
    scope="accounts" grants read-only account/trade data access (execution
    events, deal history, balance) - NOT the full "trading" scope, which would
    also let this app place orders on the account. We only ever need to read
    the master account, never write to it, so "accounts" is intentionally the
    narrower request.
    """
    client_id = _require_env("CTRADER_CLIENT_ID")
    redirect_uri = _require_env("CTRADER_REDIRECT_URI")
    params = {"client_id": client_id, "redirect_uri": redirect_uri, "scope": scope}
    return f"{EndPoints.AUTH_URI}?{urlencode(params)}"


@dataclass
class TokenPair:
    access_token: str
    refresh_token: str
    expires_at: float  # unix timestamp


def _parse_token_response(data: dict) -> TokenPair:
    # cTrader's token response includes an `errorCode` KEY even on success
    # (with value None) - checking key presence instead of its value treats
    # every successful exchange as an error. Confirmed against a live
    # response: {'errorCode': None, 'access_token': '...', ...} is success.
    if data.get("errorCode") is not None or "access_token" not in data:
        raise OAuthError(f"cTrader token endpoint returned an error: {data}")
    expires_in = float(data.get("expires_in", 2_628_000))  # ~30 days, per the docs
    return TokenPair(
        access_token=data["access_token"],
        refresh_token=data["refresh_token"],
        expires_at=time.time() + expires_in,
    )


def exchange_code(authorization_code: str) -> TokenPair:
    client_id = _require_env("CTRADER_CLIENT_ID")
    client_secret = _require_env("CTRADER_CLIENT_SECRET")
    redirect_uri = _require_env("CTRADER_REDIRECT_URI")

    response = requests.get(
        EndPoints.TOKEN_URI,
        params={
            "grant_type": "authorization_code",
            "code": authorization_code,
            "redirect_uri": redirect_uri,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=15,
    )
    response.raise_for_status()
    return _parse_token_response(response.json())


def refresh_access_token(refresh_token: str) -> TokenPair:
    client_id = _require_env("CTRADER_CLIENT_ID")
    client_secret = _require_env("CTRADER_CLIENT_SECRET")

    response = requests.get(
        EndPoints.TOKEN_URI,
        params={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=15,
    )
    response.raise_for_status()
    data = response.json()
    result = _parse_token_response(data)
    # Per the FAQ, the refresh token itself doesn't expire on its own - but the
    # endpoint may still rotate it, so always store whatever comes back rather
    # than assuming the old refresh_token is still valid.
    if "refresh_token" not in data:
        result.refresh_token = refresh_token
    return result