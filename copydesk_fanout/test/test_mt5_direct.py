"""
Layer A isolation test - talks DIRECTLY to the MT5 terminal via the
official `MetaTrader5` package's IPC connection. This bypasses the EA
AND the file bridge entirely. If this doesn't show your closed trades,
the problem is in the terminal/broker sync itself, not in any of this
repo's code - meaning DWX_Server_MT5.mq5, dwx_client.py, and the FastAPI
backend are all innocent.

Install first (Windows only, must match the terminal's bitness - MT5 is
always 64-bit these days so this is normally fine):
    pip install MetaTrader5

IMPORTANT: multiple terminal instances are running on this machine (one
per provisioned account, per provisioning.py). You MUST pass the exact
path to THIS account's terminal64.exe, or MetaTrader5 will silently
attach to whichever terminal it finds first / was last configured,
which could easily be the wrong account - that alone can look exactly
like "missing data".
"""

import MetaTrader5 as mt5
from datetime import datetime, timedelta

# ---- EDIT THIS: exact terminal64.exe for the SAME account_id you tested ----
# INSTANCES_DIR / {account_id} / terminal64.exe
TERMINAL_PATH = r"C:\Users\ISO\Desktop\automation\instances\master_bf7d4c322a\terminal64.exe"

if not mt5.initialize(path=TERMINAL_PATH):
    print("initialize() failed, error code =", mt5.last_error())
    quit()

print("Terminal info:", mt5.terminal_info())
print("Account info:", mt5.account_info())

# Ask for the same 30-day window the backend uses, to compare apples to apples
utc_to = datetime.now()
utc_from = utc_to - timedelta(days=30)

deals = mt5.history_deals_get(utc_from, utc_to)
print(f"\nhistory_deals_get(last 30 days): {len(deals) if deals else 0} deals")
if deals:
    for d in deals:
        print(d)

orders = mt5.history_orders_get(utc_from, utc_to)
print(f"\nhistory_orders_get(last 30 days): {len(orders) if orders else 0} orders")

positions = mt5.positions_get()
print(f"\npositions_get() (currently open): {len(positions) if positions else 0} open positions")
if positions:
    for p in positions:
        print(p)

# Also try a much wider window in case your closed trades are older than 30 days
wide_from = utc_to - timedelta(days=365)
wide_deals = mt5.history_deals_get(wide_from, utc_to)
print(f"\nhistory_deals_get(last 365 days): {len(wide_deals) if wide_deals else 0} deals")

mt5.shutdown()