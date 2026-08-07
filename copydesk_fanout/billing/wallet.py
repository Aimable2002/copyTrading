from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Literal

from ..infra.supabase_client import execute_with_retry

logger = logging.getLogger("wallet")

TransactionType = Literal[
    "topup", "infra_fee", "slot_fee",
    "profit_share_platform", "profit_share_master", "debt_recovery",
    "platform_weekly_charge",
    "challenge_reward",
]


class WalletError(Exception):
    """Raised for any failure here. Message is safe to surface to an API caller."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_wallet(account_id: str, supabase_client: Any) -> dict | None:
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