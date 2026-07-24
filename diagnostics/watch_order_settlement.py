"""
Read-only diagnostic: watches a DWX Connect terminal (point it at the
master's MQL5/Files directory) and logs, with millisecond timestamps, every
change to each open order's open_price/SL/TP - specifically to observe and
quantify the delay between a ticket first appearing with open_price=0.0 and
the terminal reporting its real, settled fill price/SL/TP.

This does NOT open, close, or modify any order. It only reads
DWX_Orders.txt via the same dwx_client class the production TerminalAgent
uses (imported directly from python/api/, not reimplemented) - so it's safe
to run against a live account alongside the real backend, with zero risk of
interfering with it.

Usage (run on the Windows box, in the same venv used for the backend):
    python diagnostics/watch_order_settlement.py "C:\\Users\\ISO\\AppData\\Roaming\\MetaQuotes\\Terminal\\<terminal-id>\\MQL5\\Files"
    python diagnostics/watch_order_settlement.py "<path>" --poll-ms 10 --settle-checks 5

Then open a trade on that terminal (or wait for the master to open one) and
watch the output. What to look for:

    [12:03:41.201] NEW    #1821507322 open_price=0.00000    SL=0.00000     TP=0.00000
    [12:03:41.215] CHANGE #1821507322 open_price=65096.92000 SL=64108.89000 TP=66098.49000  (+14ms, 3 polls since first seen)
    [12:03:41.230] STABLE #1821507322 - unchanged for 5 consecutive polls, treating as settled

- If NEW and the first CHANGE line show the same real values (i.e. NEW never
  shows 0.0 for a filled market order), the terminal isn't actually racing
  and the polling-retry fix targets the wrong thing - worth re-checking
  where else a 0 could be creeping in before concluding that.
- If there IS a real gap like the July 24 01:52 log, the "+Nms, M polls"
  number tells you exactly how long a retry loop needs to cover, and how
  many poll cycles it should allow for, with real numbers instead of a
  guess.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

# Reuse the actual DWX client the production backend uses, rather than
# re-parsing DWX_Orders.txt ourselves and risking this diagnostic drifting
# from what the real TerminalAgent sees.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "python" / "api"))
from dwx_client import dwx_client  # noqa: E402


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


class _NullEventHandler:
    """dwx_client requires all six of these hooks to exist on whatever you
    pass as event_handler. We only care about orders and EA error messages
    for this diagnostic - ticks/bars/history are no-ops."""

    def on_order_event(self) -> None:
        pass

    def on_message(self, message: dict) -> None:
        if message.get("type") == "ERROR":
            print(f"[{_ts()}] EA ERROR: {message.get('error_type')} | {message.get('description')}")

    def on_tick(self, symbol, bid, ask) -> None:
        pass

    def on_bar_data(self, symbol, time_frame, time, open_price, high, low, close_price, tick_volume) -> None:
        pass

    def on_historic_data(self, symbol, time_frame, data) -> None:
        pass

    def on_historic_trades(self) -> None:
        pass


# The fields _apply_initial_sl_tp actually depends on - not tracking
# anything else (like pnl/swap) since those change continuously and would
# just be noise here.
_TRACKED_FIELDS = ("open_price", "SL", "TP", "lots")


def watch(metatrader_dir_path: str, poll_seconds: float, settle_checks: int) -> None:
    dwx = dwx_client(
        event_handler=_NullEventHandler(),
        metatrader_dir_path=metatrader_dir_path,
        verbose=False,
    )
    time.sleep(1)  # let dwx_client's own background threads do their first file read

    print(f"Watching {metatrader_dir_path}")
    print(
        f"Polling every {poll_seconds * 1000:.0f}ms, calling a ticket settled after "
        f"{settle_checks} consecutive unchanged reads. Ctrl+C to stop.\n"
    )

    last_seen: dict[str, dict] = {}
    first_seen_at: dict[str, float] = {}
    poll_count_since_new: dict[str, int] = {}
    stable_count: dict[str, int] = {}
    reported_stable: set[str] = set()

    try:
        while True:
            current = dict(dwx.open_orders)
            now = time.monotonic()

            for ticket, order in current.items():
                snapshot = {field: order.get(field) for field in _TRACKED_FIELDS}

                if ticket not in last_seen:
                    print(
                        f"[{_ts()}] NEW    #{ticket} "
                        f"open_price={snapshot['open_price']:.5f} SL={snapshot['SL']:.5f} "
                        f"TP={snapshot['TP']:.5f} lots={snapshot['lots']}"
                    )
                    last_seen[ticket] = snapshot
                    first_seen_at[ticket] = now
                    poll_count_since_new[ticket] = 0
                    stable_count[ticket] = 0
                    continue

                poll_count_since_new[ticket] += 1

                if snapshot != last_seen[ticket]:
                    elapsed_ms = (now - first_seen_at[ticket]) * 1000
                    print(
                        f"[{_ts()}] CHANGE #{ticket} "
                        f"open_price={snapshot['open_price']:.5f} SL={snapshot['SL']:.5f} "
                        f"TP={snapshot['TP']:.5f}  (+{elapsed_ms:.0f}ms, "
                        f"{poll_count_since_new[ticket]} polls since first seen)"
                    )
                    last_seen[ticket] = snapshot
                    stable_count[ticket] = 0
                    reported_stable.discard(ticket)
                else:
                    stable_count[ticket] += 1
                    if stable_count[ticket] >= settle_checks and ticket not in reported_stable:
                        print(
                            f"[{_ts()}] STABLE #{ticket} - unchanged for {settle_checks} "
                            f"consecutive polls, treating as settled"
                        )
                        reported_stable.add(ticket)

            for ticket in list(last_seen):
                if ticket not in current:
                    print(f"[{_ts()}] GONE   #{ticket}")
                    del last_seen[ticket]
                    first_seen_at.pop(ticket, None)
                    poll_count_since_new.pop(ticket, None)
                    stable_count.pop(ticket, None)
                    reported_stable.discard(ticket)

            time.sleep(poll_seconds)
    except KeyboardInterrupt:
        print("\nStopped.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "metatrader_dir_path",
        help=r'The terminal\'s MQL5\Files directory, e.g. '
             r'"C:\Users\ISO\AppData\Roaming\MetaQuotes\Terminal\<terminal-id>\MQL5\Files"',
    )
    parser.add_argument("--poll-ms", type=float, default=10, help="Poll interval in milliseconds (default: 10)")
    parser.add_argument(
        "--settle-checks", type=int, default=5,
        help="Consecutive unchanged reads before a ticket is reported as settled (default: 5)",
    )
    args = parser.parse_args()
    watch(args.metatrader_dir_path, args.poll_ms / 1000, args.settle_checks)


if __name__ == "__main__":
    main()