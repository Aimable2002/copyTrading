from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

from . import profit_share
from ..infra.supabase_client import execute_with_retry
from ..payments.currencies import CurrencyError, calling_code_for
from ..payments.flutterwave_client import FlutterwaveError, flutterwave

logger = logging.getLogger("payouts")

# Currency our platform ledger (and hence master_payouts.amount) is
# denominated in - same USD basis as billing.py's infra_fee and the
# payments module's amount_usd. Overridable via env in case that ever
# changes without a code deploy.
_PAYOUT_SOURCE_CURRENCY = os.environ.get("FLW_PAYOUT_SOURCE_CURRENCY", "USD")


class PayoutError(Exception):
    """Raised for any failure here. Message is safe to surface to an API caller."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_available_balance(master_account_id: str, supabase_client: Any) -> float:
    """Lifetime earnings minus everything already claimed against them.
    Rejected requests don't count - that's what makes the money available
    again. Pending DOES count, so the same earnings can't be requested
    twice while a first request is still awaiting review."""
    earnings = profit_share.get_master_earnings(master_account_id, supabase_client)
    claimed_response = execute_with_retry(
        lambda: (
            supabase_client.table("master_payouts")
            .select("amount, status")
            .eq("master_account_id", master_account_id)
            .in_("status", ["pending", "paid"])
            .execute()
        )
    )
    already_claimed = sum(float(r["amount"]) for r in (claimed_response.data or []))
    return earnings["total_earned"] - already_claimed


def _latest_period_end(master_account_id: str, supabase_client: Any) -> str | None:
    response = execute_with_retry(
        lambda: (
            supabase_client.table("master_payouts")
            .select("period_end")
            .eq("master_account_id", master_account_id)
            .order("period_end", desc=True)
            .limit(1)
            .execute()
        )
    )
    rows = response.data or []
    return rows[0]["period_end"] if rows else None


def request_payout(
    master_account_id: str, amount: float, recipient_name: str, recipient_phone: str, supabase_client: Any,
    *, currency: str | None = None, network: str | None = None,
) -> dict:
    if amount <= 0:
        raise PayoutError("Amount must be greater than zero")

    available = get_available_balance(master_account_id, supabase_client)
    if amount > available:
        raise PayoutError(f"Requested {amount:.2f} exceeds available balance {available:.2f}")

    period_start = _latest_period_end(master_account_id, supabase_client)
    now = _now_iso()

    row = {
        "master_account_id": master_account_id,
        "period_start": period_start or now,
        "period_end": now,
        "amount": amount,
        "recipient_name": recipient_name,
        "recipient_phone": recipient_phone,
        "status": "pending",
    }
    # Both optional and nullable - see approve_payout()'s docstring for why:
    # only present when the caller (currently nothing does yet - the
    # existing /masters/{id}/payouts request body has no such fields) opts
    # into a real Flutterwave transfer instead of the manual-payout flow.
    if currency:
        row["currency"] = currency
    if network:
        row["network"] = network

    response = execute_with_retry(
        lambda: supabase_client.table("master_payouts").insert(row).execute()
    )
    return response.data[0]


def list_payouts_for_master(master_account_id: str, supabase_client: Any) -> list[dict]:
    response = execute_with_retry(
        lambda: (
            supabase_client.table("master_payouts")
            .select("*")
            .eq("master_account_id", master_account_id)
            .order("period_end", desc=True)
            .execute()
        )
    )
    return response.data or []


def list_pending_payouts(supabase_client: Any) -> list[dict]:
    """Admin queue. master_payouts.master_account_id has no declared FK
    to master_profiles (only to accounts), so PostgREST can't embed the
    join automatically - fetch display names separately and merge here,
    same pattern as master_profiles.list_all_masters uses for
    accounts.status."""
    payouts_response = execute_with_retry(
        lambda: (
            supabase_client.table("master_payouts")
            .select("*")
            .eq("status", "pending")
            .order("period_end", desc=True)
            .execute()
        )
    )
    rows = payouts_response.data or []
    if not rows:
        return []

    account_ids = list({r["master_account_id"] for r in rows})
    profiles_response = execute_with_retry(
        lambda: (
            supabase_client.table("master_profiles")
            .select("master_account_id, display_name")
            .in_("master_account_id", account_ids)
            .execute()
        )
    )
    name_by_id = {p["master_account_id"]: p["display_name"] for p in (profiles_response.data or [])}

    return [{**r, "master_display_name": name_by_id.get(r["master_account_id"])} for r in rows]


def _get_payout(payout_id: str, supabase_client: Any) -> dict | None:
    response = execute_with_retry(
        lambda: supabase_client.table("master_payouts").select("*").eq("id", payout_id).execute()
    )
    rows = response.data or []
    return rows[0] if rows else None


def approve_payout(payout_id: str, supabase_client: Any) -> dict:
    """Two paths, chosen by whether the payout row has `currency`+`network`
    set (added via payments/migration.sql - nullable, so existing rows and
    the current /masters/{id}/payouts request body, which only collects
    amount/recipient_name/recipient_phone, keep working exactly as before):

    - Both present: send a REAL Flutterwave mobile-money transfer. Status
      stays 'pending' (not 'paid' yet) until webhooks.py's
      transfer.disburse handler confirms it actually landed - approving
      here only means "an admin authorized this", not "the money moved".
    - Either missing: the original DB-only behavior, unchanged. There's no
      safe way to guess a mobile money network from a phone number alone,
      so this path is kept rather than risk sending real money to the
      wrong network's rails.
    """
    row = _get_payout(payout_id, supabase_client)
    if row is None or row["status"] != "pending":
        raise PayoutError(f"No pending payout {payout_id} to approve")

    currency = row.get("currency")
    network = row.get("network")
    if not currency or not network:
        response = execute_with_retry(
            lambda: (
                supabase_client.table("master_payouts")
                .update({"status": "paid", "paid_at": _now_iso()})
                .eq("id", payout_id)
                .eq("status", "pending")
                .execute()
            )
        )
        if not response.data:
            raise PayoutError(f"No pending payout {payout_id} to approve")
        return response.data[0]

    try:
        country_code = calling_code_for(currency)
    except CurrencyError as exc:
        raise PayoutError(str(exc)) from exc

    transfer_reference = f"pyt{uuid.uuid4().hex}"
    name_parts = (row["recipient_name"] or "").split(maxsplit=1)
    first_name = name_parts[0] if name_parts else row["recipient_name"]
    last_name = name_parts[1] if len(name_parts) > 1 else ""

    try:
        transfer = flutterwave.create_direct_transfer(
            reference=transfer_reference,
            source_currency=_PAYOUT_SOURCE_CURRENCY,
            destination_currency=currency,
            amount_value=row["amount"],
            transfer_type="mobile_money",
            recipient={
                "name": {"first": first_name, "last": last_name},
                "mobile_money": {
                    "network": network,
                    # Flutterwave's mobile-money payout recipient uses
                    # `msisdn`, NOT `phone_number` (that's the charge-side
                    # field name) - full number including country_code,
                    # per the "Handling Mobile Number" note in
                    # https://developer.flutterwave.com/docs/mobile-money-1
                    "msisdn": f"{country_code}{row['recipient_phone'].lstrip('0')}",
                },
            },
        )
    except FlutterwaveError as exc:
        raise PayoutError(f"Flutterwave transfer failed: {exc}") from exc

    data = transfer.get("data", {})
    response = execute_with_retry(
        lambda: (
            supabase_client.table("master_payouts")
            .update({"transfer_reference": transfer_reference, "transfer_id": data.get("id")})
            .eq("id", payout_id)
            .eq("status", "pending")
            .execute()
        )
    )
    if not response.data:
        raise PayoutError(f"No pending payout {payout_id} to approve")
    logger.info(
        "Initiated real Flutterwave transfer %s (id=%s) for payout %s - awaiting transfer.disburse webhook",
        transfer_reference, data.get("id"), payout_id,
    )
    return response.data[0]


def finalize_transfer(transfer_reference: str, status: str, supabase_client: Any, *, reason: str | None = None) -> dict | None:
    """Called from payments/webhooks.py's transfer.disburse handler once
    Flutterwave confirms a real transfer (see approve_payout above) actually
    succeeded or failed. Idempotent via the .eq("status", "pending") guard,
    same pattern as reject_payout/approve_payout below."""
    update: dict[str, Any] = {"status": status}
    if status == "paid":
        update["paid_at"] = _now_iso()
    elif status == "rejected" and reason:
        update["rejection_reason"] = reason
    response = execute_with_retry(
        lambda: (
            supabase_client.table("master_payouts")
            .update(update)
            .eq("transfer_reference", transfer_reference)
            .eq("status", "pending")
            .execute()
        )
    )
    rows = response.data or []
    if not rows:
        logger.warning(
            "transfer.disburse for %s: no matching pending payout (already finalized, or unknown reference)",
            transfer_reference,
        )
        return None
    return rows[0]


def reject_payout(payout_id: str, reason: str, supabase_client: Any) -> dict:
    if not reason.strip():
        raise PayoutError("A rejection reason is required")
    response = execute_with_retry(
        lambda: (
            supabase_client.table("master_payouts")
            .update({"status": "rejected", "rejection_reason": reason})
            .eq("id", payout_id)
            .eq("status", "pending")
            .execute()
        )
    )
    if not response.data:
        raise PayoutError(f"No pending payout {payout_id} to reject")
    return response.data[0]