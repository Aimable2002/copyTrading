"""
Runs the new wallet/billing/roster/master_rate/profit_share modules
against an in-memory fake Supabase client - no live Supabase project or
MT5 terminal needed. Plain assert-based, run directly:

    python -m copydesk_fanout.test.test_wallet_billing
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from copydesk_fanout import billing, master_rate, profit_share, roster, wallet
from copydesk_fanout.test.fake_supabase import FakeFanout, FakeSupabase


def _fresh_db() -> FakeSupabase:
    return FakeSupabase()


# ------------------------------------------------------------------
# wallet.py
# ------------------------------------------------------------------

def test_wallet_no_row_until_touched():
    db = _fresh_db()
    assert wallet.get_wallet("follower_1", db) is None
    print("PASS test_wallet_no_row_until_touched")


def test_wallet_topup_and_debit_and_debt():
    db = _fresh_db()
    w = wallet.top_up("follower_1", 100.0, db)
    assert w["balance"] == 100.0 and w["in_debt"] is False

    w = wallet.debit("follower_1", 30.0, "infra_fee", db)
    assert w["balance"] == 70.0 and w["in_debt"] is False

    w = wallet.debit("follower_1", 90.0, "profit_share_platform", db)
    assert round(w["balance"], 2) == -20.0
    assert w["in_debt"] is True
    first_debt_ts = w["debt_started_at"]
    assert first_debt_ts is not None

    # Further debit while already in debt keeps the ORIGINAL debt_started_at
    w = wallet.debit("follower_1", 5.0, "profit_share_master", db)
    assert round(w["balance"], 2) == -25.0
    assert w["debt_started_at"] == first_debt_ts

    # Balance updates the instant it's touched - verify get_wallet reflects
    # every single debit immediately, not batched.
    assert wallet.get_wallet("follower_1", db)["balance"] == -25.0

    # Top-up clears debt once balance crosses back to >= 0
    w = wallet.top_up("follower_1", 25.0, db)
    assert w["balance"] == 0.0 and w["in_debt"] is False and w["debt_started_at"] is None

    txns = wallet.list_transactions("follower_1", db)
    assert len(txns) == 5  # topup, infra_fee debit, 2x profit_share debit, topup
    print("PASS test_wallet_topup_and_debit_and_debt")


def test_wallet_debit_without_existing_wallet_raises():
    db = _fresh_db()
    try:
        wallet.debit("follower_ghost", 10.0, "infra_fee", db)
        raise AssertionError("expected WalletError")
    except wallet.WalletError:
        pass
    print("PASS test_wallet_debit_without_existing_wallet_raises")


# ------------------------------------------------------------------
# master_rate.py
# ------------------------------------------------------------------

def test_master_rate_set_and_public_hides_platform_cut():
    db = _fresh_db()
    result = master_rate.set_rate("master_1", 25.0, 10.0, db)
    assert result["master_net_percent"] == 15.0

    public = master_rate.get_public_rate("master_1", db)
    assert public == {"master_account_id": "master_1", "rate_percent": 25.0}
    assert "platform_cut_percent" not in public

    full = master_rate.get_current_rate("master_1", db)
    assert full["platform_cut_percent"] == 10.0
    print("PASS test_master_rate_set_and_public_hides_platform_cut")


def test_master_rate_invalid_values_rejected():
    db = _fresh_db()
    for rate, cut in [(0, 0), (101, 0), (20, 25)]:  # zero, >100, cut > rate
        try:
            master_rate.set_rate("master_1", rate, cut, db)
            raise AssertionError(f"expected MasterRateError for rate={rate} cut={cut}")
        except master_rate.MasterRateError:
            pass
    print("PASS test_master_rate_invalid_values_rejected")


def test_snapshot_immune_to_later_rate_change():
    db = _fresh_db()
    master_rate.set_rate("master_1", 20.0, 8.0, db)
    snap1 = master_rate.snapshot_rate_for_copy(
        follower_account_id="follower_1", master_account_id="master_1", roster_slot_id="slot-A", supabase_client=db,
    )
    assert snap1["rate_percent"] == 20.0

    # Master raises their rate afterward
    master_rate.set_rate("master_1", 35.0, 12.0, db)

    # The OLD slot's locked-in rate must not move
    locked = master_rate.get_copy_rate_for_slot("slot-A", db)
    assert locked["rate_percent"] == 20.0 and locked["platform_cut_percent"] == 8.0

    # A NEW copy snapshots the NEW rate
    snap2 = master_rate.snapshot_rate_for_copy(
        follower_account_id="follower_2", master_account_id="master_1", roster_slot_id="slot-B", supabase_client=db,
    )
    assert snap2["rate_percent"] == 35.0
    print("PASS test_snapshot_immune_to_later_rate_change")


# ------------------------------------------------------------------
# roster.py
# ------------------------------------------------------------------

def _seed_billing_period(db, account_id="follower_1", base_roster_size=2, slot_fee=2.0) -> str:
    resp = db.table("billing_periods").insert(
        {
            "account_id": account_id, "package_code": "1m", "duration_days": 30,
            "infra_fee": 10.0, "slot_fee_per_slot": slot_fee, "base_roster_size": base_roster_size,
            "purchased_extra_slots": 0, "status": "active", "renews_at": "2026-08-21T00:00:00+00:00",
            "started_at": "2026-07-21T00:00:00+00:00",
        }
    ).execute()
    return resp.data[0]["id"]


def test_roster_free_switch_under_capacity_and_free_return():
    db = _fresh_db()
    wallet.top_up("follower_1", 100.0, db)
    master_rate.set_rate("master_A", 25.0, 10.0, db)
    master_rate.set_rate("master_B", 30.0, 10.0, db)
    period_id = _seed_billing_period(db, base_roster_size=2)

    r1 = roster.switch_master(billing_period_id=period_id, follower_account_id="follower_1", new_master_account_id="master_A", supabase_client=db)
    assert r1["charged"] is False  # first master, well under capacity

    r2 = roster.switch_master(billing_period_id=period_id, follower_account_id="follower_1", new_master_account_id="master_B", supabase_client=db)
    assert r2["charged"] is False  # second master, still within base_roster_size=2

    balance_before_return = wallet.get_wallet("follower_1", db)["balance"]
    r3 = roster.switch_master(billing_period_id=period_id, follower_account_id="follower_1", new_master_account_id="master_A", supabase_client=db)
    assert r3["charged"] is False  # returning to A - already used this period, free
    assert r3["roster_slot_id"] == r1["roster_slot_id"]  # same slot row, not a new one
    assert wallet.get_wallet("follower_1", db)["balance"] == balance_before_return  # no wallet movement

    roster_rows = roster.get_roster(period_id, "follower_1", db)
    assert len(roster_rows) == 2  # still only 2 distinct masters ever touched
    current = [r for r in roster_rows if r["is_current"]]
    assert len(current) == 1 and current[0]["master_account_id"] == "master_A"
    print("PASS test_roster_free_switch_under_capacity_and_free_return")


def test_roster_overflow_buys_a_slot():
    db = _fresh_db()
    wallet.top_up("follower_1", 100.0, db)
    for m, rate in [("master_A", 20.0), ("master_B", 20.0), ("master_C", 20.0)]:
        master_rate.set_rate(m, rate, 5.0, db)
    period_id = _seed_billing_period(db, base_roster_size=2, slot_fee=2.0)

    roster.switch_master(billing_period_id=period_id, follower_account_id="follower_1", new_master_account_id="master_A", supabase_client=db)
    roster.switch_master(billing_period_id=period_id, follower_account_id="follower_1", new_master_account_id="master_B", supabase_client=db)
    balance_before = wallet.get_wallet("follower_1", db)["balance"]

    r3 = roster.switch_master(billing_period_id=period_id, follower_account_id="follower_1", new_master_account_id="master_C", supabase_client=db)
    assert r3["charged"] is True  # capacity was 2, this is the 3rd distinct master

    balance_after = wallet.get_wallet("follower_1", db)["balance"]
    assert round(balance_before - balance_after, 2) == 2.0  # slot_fee_per_slot debited

    period = db.table("billing_periods").select("*").eq("id", period_id).execute().data[0]
    assert period["purchased_extra_slots"] == 1  # capacity permanently grew by 1 for this period
    print("PASS test_roster_overflow_buys_a_slot")


def test_roster_rejects_when_period_closed():
    db = _fresh_db()
    master_rate.set_rate("master_A", 20.0, 5.0, db)
    period_id = _seed_billing_period(db)
    db.table("billing_periods").update({"status": "closed"}).eq("id", period_id).execute()
    try:
        roster.switch_master(billing_period_id=period_id, follower_account_id="follower_1", new_master_account_id="master_A", supabase_client=db)
        raise AssertionError("expected RosterError")
    except roster.RosterError:
        pass
    print("PASS test_roster_rejects_when_period_closed")


# ------------------------------------------------------------------
# billing.py
# ------------------------------------------------------------------

def _seed_package(db, code="1m", infra_fee=10.0, slot_fee=2.0, base_roster_size=2, duration_days=30, is_active=True):
    db.table("packages").insert(
        {
            "code": code, "duration_days": duration_days, "infra_fee": infra_fee,
            "slot_fee_per_slot": slot_fee, "base_roster_size": base_roster_size, "is_active": is_active,
        }
    ).execute()


def test_billing_select_package_active_when_funded():
    db = _fresh_db()
    _seed_package(db, code="1m", infra_fee=10.0)
    fanout = FakeFanout()
    wallet.top_up("follower_1", 50.0, db)  # fund BEFORE selecting, same as the real "deposit -> auto deduct" flow
    period = billing.select_package(account_id="follower_1", package_code="1m", role="follower", fanout=fanout, supabase_client=db)
    assert period["status"] == "active"
    assert wallet.get_wallet("follower_1", db)["balance"] == 40.0  # 50 - 10 infra fee
    print("PASS test_billing_select_package_active_when_funded")


def test_billing_select_package_grace_when_underfunded():
    db = _fresh_db()
    _seed_package(db, code="1m", infra_fee=10.0)
    fanout = FakeFanout()
    wallet.top_up("follower_2", 3.0, db)  # not enough to cover the $10 infra fee
    period = billing.select_package(account_id="follower_2", package_code="1m", role="follower", fanout=fanout, supabase_client=db)
    assert period["status"] == "grace"
    assert period["grace_started_at"] is not None
    w = wallet.get_wallet("follower_2", db)
    assert w["in_debt"] is True
    print("PASS test_billing_select_package_grace_when_underfunded")


def test_billing_rejects_double_selection():
    db = _fresh_db()
    _seed_package(db, code="1m", infra_fee=10.0)
    _seed_package(db, code="3m", infra_fee=27.0)
    fanout = FakeFanout()
    wallet.top_up("follower_1", 50.0, db)
    billing.select_package(account_id="follower_1", package_code="1m", role="follower", fanout=fanout, supabase_client=db)
    try:
        billing.select_package(account_id="follower_1", package_code="3m", role="follower", fanout=fanout, supabase_client=db)
        raise AssertionError("expected BillingError")
    except billing.BillingError:
        pass
    print("PASS test_billing_rejects_double_selection")


def test_billing_grace_expiry_closes_after_window():
    db = _fresh_db()
    _seed_package(db, code="1m", infra_fee=10.0)
    fanout = FakeFanout()
    wallet.top_up("follower_3", 1.0, db)
    period = billing.select_package(account_id="follower_3", package_code="1m", role="follower", fanout=fanout, supabase_client=db)
    assert period["status"] == "grace"

    # Simulate the grace window already having started 6 days ago (past GRACE_PERIOD_DAYS=5)
    db.table("billing_periods").update({"grace_started_at": "2020-01-01T00:00:00+00:00"}).eq("id", period["id"]).execute()
    db.table("accounts").insert({"account_id": "follower_3", "role": "follower"}).execute()

    closed = billing.check_grace_expirations(fanout, db, agents=[])
    assert "follower_3" in closed
    final = db.table("billing_periods").select("status").eq("id", period["id"]).execute().data[0]
    assert final["status"] == "closed"
    print("PASS test_billing_grace_expiry_closes_after_window")


def test_billing_unknown_or_inactive_package_rejected():
    db = _fresh_db()
    fanout = FakeFanout()
    try:
        billing.select_package(account_id="follower_9", package_code="1m", role="follower", fanout=fanout, supabase_client=db)
        raise AssertionError("expected BillingError for a package that was never seeded")
    except billing.BillingError:
        pass

    _seed_package(db, code="1m", infra_fee=10.0, is_active=False)
    try:
        billing.select_package(account_id="follower_9", package_code="1m", role="follower", fanout=fanout, supabase_client=db)
        raise AssertionError("expected BillingError for an inactive package")
    except billing.BillingError:
        pass
    print("PASS test_billing_unknown_or_inactive_package_rejected")


# ------------------------------------------------------------------
# profit_share.py
# ------------------------------------------------------------------

class _FakeAgent:
    def __init__(self, deals):
        self._deals = deals

    def fetch_historic_trades(self, lookback_days=30):
        return self._deals


def test_profit_share_bills_wins_skips_losses_and_is_idempotent():
    db = _fresh_db()
    wallet.top_up("follower_1", 100.0, db)
    master_rate.set_rate("master_A", 25.0, 10.0, db)  # 25% total, 10 platform / 15 master
    period_id = _seed_billing_period(db)
    roster.switch_master(billing_period_id=period_id, follower_account_id="follower_1", new_master_account_id="master_A", supabase_client=db)

    raw_deals = {
        "1001": {"entry": "entry_in", "pnl": 0.0, "symbol": "BTCUSD", "type": "buy", "lots": 0.1, "magic": 0, "deal_time": "", "deal_price": 0, "commission": 0, "swap": 0, "comment": ""},
        "1002": {"entry": "entry_out", "pnl": 40.0, "symbol": "BTCUSD", "type": "sell", "lots": 0.1, "magic": 0, "deal_time": "", "deal_price": 0, "commission": 0, "swap": 0, "comment": ""},  # winning close
        "1003": {"entry": "entry_out", "pnl": -12.0, "symbol": "BTCUSD", "type": "sell", "lots": 0.1, "magic": 0, "deal_time": "", "deal_price": 0, "commission": 0, "swap": 0, "comment": ""},  # losing close - no charge
    }
    agent = _FakeAgent(raw_deals)

    charges = profit_share.process_follower_deals(follower_account_id="follower_1", billing_period_id=period_id, agent=agent, supabase_client=db)
    assert len(charges) == 1  # only the winning deal billed
    assert charges[0]["deal_ticket"] == "1002"
    assert round(charges[0]["platform_amount"], 2) == 4.00   # 40 * 10%
    assert round(charges[0]["master_amount"], 2) == 6.00     # 40 * 15%

    w = wallet.get_wallet("follower_1", db)
    assert round(w["balance"], 2) == 90.00  # 100 - 4 - 6, loss untouched, entry_in untouched

    # Re-run the exact same poll - nothing new should be billed (idempotency)
    charges_again = profit_share.process_follower_deals(follower_account_id="follower_1", billing_period_id=period_id, agent=agent, supabase_client=db)
    assert charges_again == []
    assert wallet.get_wallet("follower_1", db)["balance"] == 90.00
    print("PASS test_profit_share_bills_wins_skips_losses_and_is_idempotent")


def test_profit_share_can_push_wallet_negative_master_still_gets_full_cut():
    db = _fresh_db()
    wallet.top_up("follower_1", 5.0, db)  # not enough to cover the coming charge
    master_rate.set_rate("master_A", 50.0, 20.0, db)  # 50% total, 20 platform / 30 master
    period_id = _seed_billing_period(db)
    roster.switch_master(billing_period_id=period_id, follower_account_id="follower_1", new_master_account_id="master_A", supabase_client=db)

    agent = _FakeAgent({"2001": {"entry": "entry_out", "pnl": 100.0, "symbol": "BTCUSD", "type": "sell", "lots": 0.1, "magic": 0, "deal_time": "", "deal_price": 0, "commission": 0, "swap": 0, "comment": ""}})
    charges = profit_share.process_follower_deals(follower_account_id="follower_1", billing_period_id=period_id, agent=agent, supabase_client=db)

    assert round(charges[0]["platform_amount"], 2) == 20.0
    assert round(charges[0]["master_amount"], 2) == 30.0  # master gets the FULL cut regardless of follower's wallet state
    w = wallet.get_wallet("follower_1", db)
    assert round(w["balance"], 2) == -45.0  # 5 - 20 - 30
    assert w["in_debt"] is True
    print("PASS test_profit_share_can_push_wallet_negative_master_still_gets_full_cut")


if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_") and callable(v)]
    failures = 0
    for t in tests:
        try:
            t()
        except Exception as exc:  # noqa: BLE001 - test runner, want to see every failure not just the first
            failures += 1
            print(f"FAIL {t.__name__}: {exc}")
    print(f"\n{len(tests) - failures}/{len(tests)} passed")
    sys.exit(1 if failures else 0)