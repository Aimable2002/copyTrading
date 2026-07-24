"""
Wallet - the follower's prepaid balance. Not their trading/broker balance;
a separate, non-withdrawable pool that funds the infra fee, slot fees, and
per-trade profit-share deductions (see billing.py, roster.py,
profit_share.py - none of them touch a wallet row directly, they all go
through debit()/top_up() here).

Deliberately NOT created at account provisioning (provisioning.py is
untouched). A wallet only comes into existence the first time an account
owner buys a package - see billing.py's ensure_wallet() call. Before that,
get_wallet() returning None is the normal, expected state for a freshly
provisioned account that hasn't bought anything yet - not an error.

Balance updates the instant it's touched (every debit/credit is a direct
UPDATE, not a batched/scheduled recompute) - there's no realtime
push wired up here (that's a frontend subscription concern, deliberately
left for later per the "not necessarily realtime, just updated the moment
it's touched" decision), but the row itself is never stale.

Debt (negative balance) is tracked with a single timestamp,
`debt_started_at`, set the moment balance first goes negative and cleared
the moment a top-up brings it back to >= 0. This is what a 5-day-warning
sweep (see main.py) reads to decide when to close an account for an
unresolved wallet shortfall - and it's a genuinely different signal from
billing_periods.status/grace_started_at (subscription non-payment), which
is why the two are separate fields on separate tables rather than one
shared "in trouble" flag: they have different causes and different fixes.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Literal

from .supabase_client import execute_with_retry

logger = logging.getLogger("wallet")

TransactionType = Literal[
    "topup", "infra_fee", "slot_fee",
    "profit_share_platform", "profit_share_master", "debt_recovery",
]


class WalletError(Exception):
    """Raised for any failure here. Message is safe to surface to an API caller."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_wallet(account_id: str, supabase_client: Any) -> dict | None:
    """None means this account has never bought a package yet - a valid
    state, not an error. Callers (API routes) should render that as
    "no wallet yet / buy a package to get started", not a 404."""
    response = execute_with_retry(
        lambda: (
            supabase_client.table("wallets")
            .select("account_id, balance, debt_started_at, updated_at")
            .eq("account_id", account_id)
            .execute()
        )
    )
    rows = response.data or []
    if not rows:
        return None
    row = rows[0]
    row["in_debt"] = float(row["balance"]) < 0
    return row


def ensure_wallet(account_id: str, supabase_client: Any) -> dict:
    """Idempotent create-if-missing. Called by billing.py on a follower's
    first-ever package purchase - never by provisioning.py."""
    existing = get_wallet(account_id, supabase_client)
    if existing is not None:
        return existing

    execute_with_retry(
        lambda: supabase_client.table("wallets").insert(
            {"account_id": account_id, "balance": 0, "debt_started_at": None}
        ).execute()
    )
    logger.info("Created wallet for %s", account_id)
    return {"account_id": account_id, "balance": 0, "debt_started_at": None, "in_debt": False}


def _record_transaction(
    supabase_client: Any, *, account_id: str, type: TransactionType, amount: float,
    related_master_account_id: str | None = None, related_deal_ticket: str | None = None,
) -> None:
    execute_with_retry(
        lambda: supabase_client.table("wallet_transactions").insert(
            {
                "account_id": account_id,
                "type": type,
                "amount": amount,
                "related_master_account_id": related_master_account_id,
                "related_deal_ticket": related_deal_ticket,
            }
        ).execute()
    )


def top_up(account_id: str, amount: float, supabase_client: Any) -> dict:
    """Credits the wallet. If the account was in debt, this naturally
    clears some or all of it first - there's no separate "pay off debt"
    step, a top-up is a top-up, the balance is just one number."""
    if amount <= 0:
        raise WalletError("Top-up amount must be positive")

    wallet = ensure_wallet(account_id, supabase_client)
    new_balance = float(wallet["balance"]) + amount
    debt_started_at = wallet["debt_started_at"] if new_balance < 0 else None

    execute_with_retry(
        lambda: supabase_client.table("wallets").update(
            {"balance": new_balance, "debt_started_at": debt_started_at, "updated_at": _now_iso()}
        ).eq("account_id", account_id).execute()
    )
    _record_transaction(supabase_client, account_id=account_id, type="topup", amount=amount)

    logger.info("Top-up %.2f for %s -> new balance %.2f", amount, account_id, new_balance)
    return {"account_id": account_id, "balance": new_balance, "debt_started_at": debt_started_at, "in_debt": new_balance < 0}


def debit(
    account_id: str, amount: float, type: TransactionType, supabase_client: Any, *,
    related_master_account_id: str | None = None, related_deal_ticket: str | None = None,
) -> dict:
    """Charges the wallet. Allowed to go negative - callers never block on
    insufficient funds here (per "master still gets paid regardless, the
    follower's wallet just goes negative"); it's the caller's job to react
    to the resulting in_debt=True (start a grace window, etc.), not this
    function's. Raises only if there's no wallet row to debit at all,
    which should never happen for a real charge - both billing.py and
    profit_share.py only ever debit an account that already has an active
    billing_period, which is exactly what creates the wallet row."""
    if amount <= 0:
        raise WalletError("Debit amount must be positive")

    wallet = get_wallet(account_id, supabase_client)
    if wallet is None:
        raise WalletError(f"No wallet exists for {account_id} - cannot debit an account with no package purchased")

    was_in_debt = wallet["in_debt"]
    new_balance = float(wallet["balance"]) - amount
    if new_balance < 0 and not was_in_debt:
        debt_started_at = _now_iso()
    elif new_balance < 0:
        debt_started_at = wallet["debt_started_at"]
    else:
        debt_started_at = None

    execute_with_retry(
        lambda: supabase_client.table("wallets").update(
            {"balance": new_balance, "debt_started_at": debt_started_at, "updated_at": _now_iso()}
        ).eq("account_id", account_id).execute()
    )
    _record_transaction(
        supabase_client, account_id=account_id, type=type, amount=-amount,
        related_master_account_id=related_master_account_id, related_deal_ticket=related_deal_ticket,
    )

    logger.info("Debit %.2f (%s) for %s -> new balance %.2f (in_debt=%s)", amount, type, account_id, new_balance, new_balance < 0)
    return {"account_id": account_id, "balance": new_balance, "debt_started_at": debt_started_at, "in_debt": new_balance < 0}


def list_transactions(account_id: str, supabase_client: Any, limit: int = 50) -> list[dict]:
    response = execute_with_retry(
        lambda: (
            supabase_client.table("wallet_transactions")
            .select("id, type, amount, related_master_account_id, related_deal_ticket, created_at")
            .eq("account_id", account_id)
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )
    )
    return response.data or []