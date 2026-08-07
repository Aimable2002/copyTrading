from __future__ import annotations

from typing import Literal

SizingMode = Literal["fixed_multiplier", "balance_proportional", "fixed_master_balance_percentage"]

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
    if mode == "fixed_multiplier":
        # raw = master_lots * multiplier
        raw = (follower_balance / master_balance) * master_lots
    elif mode == "balance_proportional":
        if not master_balance or not follower_balance:
            raise ValueError("balance_proportional mode requires both master_balance and follower_balance")
        raw = (follower_balance / master_balance) * master_lots
    elif mode == "fixed_master_balance_percentage":
        if not fixed_master_balance or not follower_balance:
            raise ValueError(
                "fixed_master_balance_percentage mode requires both fixed_master_balance and follower_balance"
            )
        raw = (follower_balance / fixed_master_balance) * master_lots
    else:
        assert_never(mode)

    stepped = round(raw / _DEFAULT_LOT_STEP) * _DEFAULT_LOT_STEP
    return max(round(stepped, 2), _MIN_LOT)
