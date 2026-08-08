"""
Reads/writes the `ctrader_accounts` table - the cTrader-specific credential
store, kept separate from `accounts` (which stays generic/platform-agnostic;
see schema.sql). Mirrors what read_provisioned_credentials()/
_write_startup_config() do for MT5 in provisioning/provisioning.py, but against
Supabase instead of a local provisioned_config.ini file, since there's no local
instance directory for a cTrader account.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from . import oauth
from ..infra.supabase_client import execute_with_retry

logger = logging.getLogger("ctrader.token_store")

# Refresh proactively once a stored token is within this window of expiring,
# rather than waiting for a request to fail with an auth error.
_REFRESH_MARGIN_SECONDS = 24 * 60 * 60  # 1 day


class TokenStoreError(Exception):
    """Raised for any failure here. Message is safe to surface to an API caller."""


def save_new_account(
    *, account_id: str, ctid_trader_account_id: int, token_pair: oauth.TokenPair, supabase_client: Any,
) -> None:
    execute_with_retry(
        lambda: supabase_client.table("ctrader_accounts").upsert(
            {
                "account_id": account_id,
                "ctid_trading_account_id": ctid_trader_account_id,
                "access_token": token_pair.access_token,
                "refresh_token": token_pair.refresh_token,
                "token_expires_at": token_pair.expires_at,
            }
        ).execute()
    )


def _get_row(account_id: str, supabase_client: Any) -> dict:
    response = execute_with_retry(
        lambda: (
            supabase_client.table("ctrader_accounts")
            .select("account_id, ctid_trading_account_id, access_token, refresh_token, token_expires_at")
            .eq("account_id", account_id)
            .execute()
        )
    )
    rows = response.data or []
    if not rows:
        raise TokenStoreError(f"No cTrader credentials stored for account {account_id}")
    return rows[0]


def get_ctid_trading_account_id(account_id: str, supabase_client: Any) -> int:
    return int(_get_row(account_id, supabase_client)["ctid_trading_account_id"])


def get_valid_access_token(account_id: str, supabase_client: Any) -> str:
    """
    Returns a currently-valid access token for this account, transparently
    refreshing (and persisting the refreshed pair) if the stored one is at or
    past `_REFRESH_MARGIN_SECONDS` from expiry.
    """
    row = _get_row(account_id, supabase_client)
    expires_at = float(row["token_expires_at"])

    if time.time() < expires_at - _REFRESH_MARGIN_SECONDS:
        return row["access_token"]

    logger.info("[%s] Stored cTrader access token is near expiry - refreshing", account_id)
    try:
        refreshed = oauth.refresh_access_token(row["refresh_token"])
    except oauth.OAuthError as exc:
        raise TokenStoreError(
            f"Failed to refresh cTrader token for {account_id}: {exc}. The account's "
            f"OAuth consent likely needs to be redone from scratch."
        ) from exc

    execute_with_retry(
        lambda: supabase_client.table("ctrader_accounts").update(
            {
                "access_token": refreshed.access_token,
                "refresh_token": refreshed.refresh_token,
                "token_expires_at": refreshed.expires_at,
            }
        ).eq("account_id", account_id).execute()
    )
    return refreshed.access_token