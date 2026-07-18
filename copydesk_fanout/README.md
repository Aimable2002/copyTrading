# copydesk_fanout

This folder is an **addition** to the official [dwxconnect](https://github.com/darwinex/dwxconnect)
repo - nothing outside this folder was modified or deleted. It adds the one
thing DWX Connect doesn't have: master -> many-follower fan-out with
per-follower position sizing, running entirely self-hosted, with zero
database calls in the actual copy path.

## What's reused vs. what's new

**Reused, completely untouched**: `../mql/DWX_Server_MT5.mq5` (the EA) and
`../python/api/dwx_client.py` (the official Python client). This folder
imports `dwx_client` directly from the sibling `python/` folder - see
`base_agent.py` for the (small) `sys.path` setup that makes that import work
without copying or vendoring any of the official code.

**New** (everything in this folder): the fan-out orchestration logic -
diffing a master's trades, computing per-follower size, dispatching to
followers, tracking which follower ticket corresponds to which master trade,
and propagating the master's SL/TP changes and partial closes to followers.
None of this exists in DWX Connect (a pure terminal bridge) or in the
tetratensor repo evaluated earlier (which expects an mtapi.io-shaped HTTP
bridge, not DWX Connect). The position-sizing formulas are ported from
tetratensor's proven three-mode calculation; everything else here is new.

### SL/TP handling - distance-based, not absolute-price copying

A follower's fill price will rarely match the master's exactly (different
broker, spread, slippage, timing lag) - copying the master's SL/TP as an
absolute price can put a follower's stop on the wrong side of their own
entry. Instead (`sltp.py`), we compute the *distance* between the master's
entry and their SL/TP, and reapply that same distance to the follower's real
entry price once their fill is confirmed. This is standard practice for
serious copy trading systems, not an invented detail.

### Modifications and partial closes ARE now detected and propagated

Confirmed directly from `dwx_client`'s source: its `on_order_event` callback
only fires when a ticket is added or removed from `open_orders` - never when
an existing ticket's SL/TP or lot size changes in place. So `TerminalAgent`
runs a second, independent polling loop (`_modification_poll_loop`,
~1s interval) purely to catch these in-place changes, since the event system
genuinely can't. When it detects a master's SL/TP change, `FanoutCore`
recomputes distance and re-applies it to every subscribed follower's real
entry price. When it detects a lot-size reduction (partial close), the
reduction *ratio* (not absolute lots) is applied to whatever we actually
dispatched to each follower, since a follower's position size independently
differs from the master's.

## Files

| File | Purpose |
|---|---|
| `base_agent.py` | Shared dwx_client callback stubs + the sys.path setup importing the official package |
| `terminal_agent.py` | Watches one terminal: event-driven open/close detection (via dwx_client's on_order_event) plus a separate polling loop for SL/TP-modify and partial-close detection, which the event system can't catch |
| `follower_agent.py` | Extends TerminalAgent with `execute_open`/`execute_close`/`execute_modify`, tags every copy with a `comment` for debugging/correlation |
| `sizing.py` | The three position-sizing modes, ported from tetratensor, pure functions, independently tested |
| `sltp.py` | Distance-based SL/TP math - compute distance from one entry price, reapply to another. Direction-aware (buy vs sell) |
| `order_pair_store.py` | In-memory only - maps master trades to confirmed follower fills, now including each follower's real entry price, order type, and current lot size (needed for SL/TP distance math and proportional partial closes) |
| `config_store.py` | In-memory master -> follower subscription config. Loaded from a local JSON file for this isolated-testing cut - see "Supabase integration point" below |
| `fanout_core.py` | Ties it all together: open -> size -> dispatch -> pair; fill confirmed -> inherit distance-based SL/TP; master modifies -> propagate distance-based; master partial-closes -> propagate proportionally; master closes -> close all followers |
| `main.py` | Entrypoint - reads `config.json`, wires up agents, starts everything |
| `config.example.json` | Copy to `config.json` and fill in your real terminal paths |

## Running this (isolated backend testing, no frontend/Supabase/payment)

1. Have MT5 terminal(s) running - one for the master, one per follower -
   each with `DWX_Server_MT5.mq5` (from `../mql/`) attached to a chart, per
   the official dwxconnect README's setup instructions.
2. `cp copydesk_fanout/config.example.json copydesk_fanout/config.json` and
   fill in each terminal's real `MQL5/Files/` directory path and your
   desired follower multiplier/sizing mode.
3. From the `dwxconnect/` repo root: `python -m copydesk_fanout.main --config copydesk_fanout/config.json`
4. Open a trade on the master's terminal (**use a demo account** - same
   warning the official repo gives). Watch the logs; the follower's
   terminal should show the scaled position appear shortly after.

## What's verified vs. what still needs a real terminal to prove out

Verified with real tests (see the conversation this was built in for the
actual test runs, or just re-run them yourself):
- All three sizing modes produce correct output for known inputs
- Distance-based SL/TP math is correct for both buy and sell directions,
  correctly preserves risk distance across different fill prices, and
  correctly produces no SL/TP when the master has none set
- `OrderPairStore` correctly handles out-of-order async fill confirmation
  and cleans up on close
- Full simulated lifecycle (fake master/follower agents standing in for real
  terminals): master opens -> follower filled at a slipped price gets
  correctly distance-adjusted SL/TP -> master tightens SL, propagated
  correctly against the follower's real entry -> master partially closes
  60%, follower's position reduced by the same 60% ratio against what was
  actually dispatched -> master fully closes, follower fully closes
- `TerminalAgent`/`FollowerAgent` construct correctly against the real,
  unmodified `dwx_client`, and both the event-driven thread and the new
  modification-polling thread start/stop cleanly

**Not yet verified - needs an actual running MT5 terminal + EA**, which
wasn't available in the environment this was built in:
- That DWX Connect's real file-based bridge behaves exactly as the source
  code implied (it should, per the confirmed `account_info`/order JSON
  shapes read directly from the EA source, but real I/O timing/race
  conditions can only be proven by actually running it)
- Real network/execution latency from master fill to follower fill, and
  from master modification to follower SL/TP update
- The 1-second modification-poll interval hasn't been tuned against a real
  terminal - may need adjusting once real timing is observable

## Known limitations, stated plainly rather than hidden

- **State is in-memory only.** A restart loses the order-pairing table.
  Rebuilding pairs from each terminal's live `comment` tags on startup is a
  reasonable follow-up, not built here.
- **Lot step is assumed at 0.01**, not fetched from the broker's actual
  symbol specs (DWX Connect doesn't expose a symbol-info lookup for this).
  Fine for most forex pairs, worth revisiting for instruments with
  different lot steps.
- If a follower manually closes a copied position themselves, this does
  NOT try to reopen it to match the master - it's logged and left alone.
  Worth deciding deliberately whether that's the behavior you want.
- **Multiple simultaneous partial closes on the same master ticket** are
  handled correctly in sequence (each ratio is computed against the
  previous remaining lots, not the original open lots), but this hasn't
  been tested with a rapid back-to-back sequence, only one at a time.

## Supabase integration point (not built - by design, per your instruction to test this in isolation first)

`ConfigStore.load_from_file()` is the seam. Replace it with a Supabase
Realtime subscription that calls `ConfigStore.set_config()` whenever a
follower's subscription/multiplier changes - nothing else in `FanoutCore`
needs to change for that swap, since it only ever reads from `ConfigStore`
in memory. Same idea for trade history: add an async, non-blocking write to
Supabase inside `FanoutCore._fan_out_open`/`_fan_out_close` after dispatch,
never before or blocking on it.
