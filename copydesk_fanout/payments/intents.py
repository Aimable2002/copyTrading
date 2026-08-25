"""Tracks a Flutterwave charge from "initiated" through to "confirmed",
independently of `wallets`/`wallet_transactions` - the wallet must NOT be
credited until the webhook confirms the charge actually succeeded (crediting
on the initial checkout call would let anyone fabricate a balance just by
hitting /payments/checkout and never actually paying). This table is the
"pending" staging area that wallet_transactions never had, since
wallet.top_up() was written to just directly credit on call - see its
docstring in wallet.py.

Requires the `payment_intents` table - see payments/migration.sql.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Literal

from ..infra.supabase_client import execute_with_retry

logger = logging.getLogger("payment_intents")

PaymentStatus = Literal["pending", "successful", "failed", "cancelled"]


class PaymentIntentError(Exception):
    """Raised for any failure here. Message is safe to surface to an API caller."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def record_intent(
    *, reference: str, account_id: str, user_id: str, purpose: str, package_code: str | None,
    challenge_id: str | None, amount_usd: float, currency: str, amount_charged: float, method: str,
    supabase_client: Any,
) -> dict:
    response = execute_with_retry(
        lambda: (
            supabase_client.table("payment_intents")
            .insert(
                {
                    "reference": reference,
                    "account_id": account_id,
                    "user_id": user_id,
                    "purpose": purpose,
                    "package_code": package_code,
                    "challenge_id": challenge_id,
                    "amount_usd": amount_usd,
                    "currency": currency,
                    "amount_charged": amount_charged,
                    "method": method,
                    "status": "pending",
                    "credited": False,
                }
            )
            .execute()
        )
    )
    return response.data[0]


def set_charge_id(reference: str, charge_id: str, supabase_client: Any) -> None:
    execute_with_retry(
        lambda: (
            supabase_client.table("payment_intents")
            .update({"charge_id": charge_id, "updated_at": _now_iso()})
            .eq("reference", reference)
            .execute()
        )
    )


def get_intent(reference: str, supabase_client: Any) -> dict | None:
    response = execute_with_retry(
        lambda: (
            supabase_client.table("payment_intents").select("*").eq("reference", reference).execute()
        )
    )
    rows = response.data or []
    return rows[0] if rows else None


def get_intent_by_charge_id(charge_id: str, supabase_client: Any) -> dict | None:
    response = execute_with_retry(
        lambda: (
            supabase_client.table("payment_intents").select("*").eq("charge_id", charge_id).execute()
        )
    )
    rows = response.data or []
    return rows[0] if rows else None


def finalize_intent(
    reference: str, status: PaymentStatus, supabase_client: Any, *, credited: bool | None = None,
) -> dict | None:
    """Idempotent: only updates a row that's still 'pending', so a webhook
    delivered twice (or racing with a manual /payments/{reference} check)
    can't finalize the same intent twice or flip a terminal status back."""
    update: dict[str, Any] = {"status": status, "updated_at": _now_iso()}
    if credited is not None:
        update["credited"] = credited
    response = execute_with_retry(
        lambda: (
            supabase_client.table("payment_intents")
            .update(update)
            .eq("reference", reference)
            .eq("status", "pending")
            .execute()
        )
    )
    rows = response.data or []
    return rows[0] if rows else None