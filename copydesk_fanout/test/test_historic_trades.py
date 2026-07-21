"""
Standalone isolation test - talks DIRECTLY to the MT5 EA via the file bridge,
bypassing FastAPI / ngrok / Supabase entirely. If this prints trades, the
problem is in the API layer (lookback_days, timeout, account_user_map).
If this prints nothing, the problem is between Python and the MT5 EA itself
(EA not attached/running, wrong Files path, or MT5's own History tab not
showing enough history).
"""

import sys
from pathlib import Path
from time import sleep

# This script lives in dwxconnect/copydesk_fanout/test/. The dwx_client
# package lives in dwxconnect/python/api/. Compute that path relative to
# THIS file (not relative to whatever directory you happen to run from) -
# a plain string like "python" only works if you run it from dwxconnect/,
# which is fragile. Adjust the number of .parent calls if you move this file.
_DWXCONNECT_PYTHON_DIR = Path(__file__).resolve().parent.parent.parent / "python"
sys.path.insert(0, str(_DWXCONNECT_PYTHON_DIR))

from api.dwx_client import dwx_client  # plain import, NOT relative (from ...) -
# relative imports (from ...python.api...) only work when the file is run as
# part of a package via `python -m`, not with a direct `python script.py` call.


# ---- EDIT THIS: point at the exact MetaTrader Files dir your EA is using ----
# Portable-mode instances (this repo's provisioning.py) keep the data folder
# INSIDE the cloned instance dir, not under AppData/MetaQuotes:
#   INSTANCES_DIR / {account_id} / MQL5 / Files
# Confirm the exact value straight from Supabase's accounts.metatrader_dir_path
# column for this account_id if unsure - it's stored verbatim there.
MT5_FILES_DIR = r"C:\Users\ISO\Desktop\automation\instances\master_bf7d4c322a\MQL5\Files"


class Listener:
    def __init__(self):
        self.captured = None  # snapshot taken INSIDE the callback, not read later

    def on_historic_trades(self):
        # Snapshot immediately - avoids any race with the background thread
        # touching self.dwx.historic_trades again before we read it.
        self.captured = dict(self.dwx.historic_trades)
        print(f"\n>>> on_historic_trades fired: {len(self.captured)} deals (snapshot taken)")

    def on_message(self, message):
        print("MESSAGE:", message)

    # required no-ops
    def on_order_event(self): pass
    def on_tick(self, *a, **kw): pass
    def on_bar_data(self, *a, **kw): pass
    def on_historic_data(self, *a, **kw): pass


listener = Listener()
dwx = dwx_client(listener, MT5_FILES_DIR, verbose=True)
listener.dwx = dwx
sleep(1)
dwx.start()

print("Account info:", dwx.account_info)
print("Requesting historic trades, lookback_days=30 ...")
dwx.historic_trades = {}
dwx.get_historic_trades(lookback_days=30)

# poll for up to 15s (longer than the API's 10s, to see if it's just slow)
for _ in range(150):
    if listener.captured is not None:
        break
    sleep(0.1)

print("\nSnapshot captured inside the callback:")
print(listener.captured)
print(f"\nTotal deals returned: {len(listener.captured) if listener.captured else 0}")

print("\nFor comparison, current (possibly stale) dwx.historic_trades attribute:")
print(dwx.historic_trades)

dwx.ACTIVE = False