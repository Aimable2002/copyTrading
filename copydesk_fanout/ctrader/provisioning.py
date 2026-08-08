"""
provision_ctrader_account() is the cTrader equivalent of
provisioning.provision_account(role="master") - same end state (a row in
`accounts`, an agent registered with fanout.register_master()), different
mechanism underneath. Unlike MT5 provisioning, there's no local terminal
process to launch and no 30-60s "wait for a terminal to connect" phase - the
whole thing (token exchange, account-level auth handshake) normally completes
in a couple of seconds, so this doesn't need the start/finish async split or
stalled-wait handling provisioning/provisioning.py has for MT5.

This is called from the OAuth callback route (api/api_server.py's
/accounts/ctrader/callback), after oauth.exchange_code() has already turned the
`code` query param into a token pair.
"""

from __future__ import annotations

import logging
from typing import Any

from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOAGetAccountListByAccessTokenReq

from ..core.fanout_core import FanoutCore
from ..provisioning.provisioning import _insert_placeholder_account, _mark_placeholder_account_failed
from . import oauth, token_store
from .master_agent import CTraderMasterAgent
from .proto_client import get_connection

logger = logging.getLogger("ctrader.provisioning")


class CTraderProvisioningError(Exception):
    """Raised for any provisioning failure. Message is safe to surface to an API caller."""


def generate_account_id() -> str:
    import uuid

    return f"master_ctrader_{uuid.uuid4().hex[:10]}"


def _resolve_ctid_trading_account_id(access_token: str) -> int:
    """
    The authorization code exchange gives an access token scoped to the user's
    cTID, which may have more than one trading account linked to it (demo +
    live, or several live accounts). We need the specific ctidTraderAccountId
    to do anything further. Since you're provisioning ONE specific cTrader
    account per master (the one you've already subscribed to a strategy on cTrader
    Copy), and cTID auth doesn't let you specify which account up front, take the
    first one returned - if this ever needs to support a cTID with multiple
    linked accounts, this is the point to add an account-picker step in the UI
    between OAuth callback and provisioning.
    """
    response = get_connection().send_and_wait(
        ProtoOAGetAccountListByAccessTokenReq(accessToken=access_token)
    )
    if not response.ctidTraderAccount:
        raise CTraderProvisioningError(
            "This cTrader login has no trading accounts linked to it - nothing to provision."
        )
    return response.ctidTraderAccount[0].ctidTraderAccountId


def provision_ctrader_account(
    *,
    user_id: str,
    authorization_code: str,
    fanout: FanoutCore,
    supabase_client: Any,
    account_user_map: dict[str, str],
    agents: list,
) -> str:
    account_id = generate_account_id()
    _insert_placeholder_account(account_id=account_id, user_id=user_id, role="master", supabase_client=supabase_client)

    try:
        token_pair = oauth.exchange_code(authorization_code)
        ctid = _resolve_ctid_trading_account_id(token_pair.access_token)
        token_store.save_new_account(
            account_id=account_id, ctid_trader_account_id=ctid, token_pair=token_pair,
            supabase_client=supabase_client,
        )

        agent = CTraderMasterAgent(
            account_id=account_id, on_trade_event=fanout.handle_master_trade_event,
            supabase_client=supabase_client,
        )
        agent.start()

    except (oauth.OAuthError, CTraderProvisioningError) as exc:
        _mark_placeholder_account_failed(account_id=account_id, reason=str(exc), supabase_client=supabase_client)
        raise CTraderProvisioningError(str(exc)) from exc
    except Exception as exc:
        _mark_placeholder_account_failed(
            account_id=account_id, reason=f"Unexpected cTrader provisioning failure: {exc}",
            supabase_client=supabase_client,
        )
        raise CTraderProvisioningError(f"Unexpected cTrader provisioning failure: {exc}") from exc

    fanout.register_master(agent)
    agents.append(agent)
    account_user_map[account_id] = user_id

    from ..infra.supabase_client import execute_with_retry

    execute_with_retry(
        lambda: supabase_client.table("accounts").upsert(
            {
                "account_id": account_id,
                "user_id": user_id,
                "role": "master",
                "platform": "ctrader",
                "status": "live",
            }
        ).execute()
    )

    logger.info("Provisioned cTrader master account %s for user %s (ctid %s)", account_id, user_id, ctid)
    return account_id