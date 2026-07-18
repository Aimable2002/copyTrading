from __future__ import annotations

from typing import Literal

SizingMode = Literal["fixed_multiplier", "balance_proportional", "fixed_master_balance_percentage"]

# Sensible default - most brokers use 0.01 lot steps. Not fetched from the
# broker's actual SymbolParams (DWX Connect doesn't expose that lookup) -
# a known simplification, flagged here rather than silently assumed.
_DEFAULT_LOT_STEP = 0.01
_MIN_LOT = 0.01


def calculate_follower_volume(
    *,
    mode: SizingMode,
    master_lots: float,
    multiplier: float,
    master_balance: float | None = None,
    follower_balance: float | None = None,
    fixed_master_balance: float | None = None,
) -> float:
    """
    Ported from tetratensor's three sizing modes (trading/masters/metatrader5.master.trading.js),
    rewritten as multiplier-as-fraction (1.0 = same size, 0.5 = half) instead
    of the original's *100 integer-percentage convention.

    - fixed_multiplier: follower_lots = master_lots * multiplier
      Ignores account balances entirely - the simplest mode.

    - balance_proportional: follower_lots = (follower_balance / master_balance) * master_lots * multiplier
      Scales by the live balance ratio between accounts - a follower with
      half the master's balance gets half-sized trades automatically, even
      as both balances change over time.

    - fixed_master_balance_percentage: follower_lots = (follower_balance / fixed_master_balance) * master_lots * multiplier
      Same idea, but scaled against a FIXED master balance snapshot rather
      than the master's live balance - keeps follower sizing stable even if
      the master's balance swings, at the cost of drifting from the master's
      true current risk if their balance has moved a lot since the snapshot.
    """
    if mode == "fixed_multiplier":
        raw = master_lots * multiplier

    elif mode == "balance_proportional":
        if not master_balance or not follower_balance:
            raise ValueError("balance_proportional mode requires both master_balance and follower_balance")
        raw = (follower_balance / master_balance) * master_lots * multiplier

    elif mode == "fixed_master_balance_percentage":
        if not fixed_master_balance or not follower_balance:
            raise ValueError(
                "fixed_master_balance_percentage mode requires both fixed_master_balance and follower_balance"
            )
        raw = (follower_balance / fixed_master_balance) * master_lots * multiplier

    else:
        raise ValueError(f"Unknown sizing mode: {mode}")

    # Round to the lot step and enforce a sane minimum.
    stepped = round(raw / _DEFAULT_LOT_STEP) * _DEFAULT_LOT_STEP
    return max(round(stepped, 2), _MIN_LOT)
