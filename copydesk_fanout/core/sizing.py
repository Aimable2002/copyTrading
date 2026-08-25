from __future__ import annotations

from typing import Literal

# Real sizing methods, as defined by the frontend - these are the only four
# sizing modes the platform supports. The previous fixed_multiplier /
# balance_proportional / fixed_master_balance_percentage modes are retired
# (see config_store.FollowerSubscription for the commented-out legacy fields).
SizingMode = Literal["proportional", "fixed-lot", "micro-scale", "risk-percent"]

_DEFAULT_LOT_STEP = 0.01
_MIN_LOT = 0.01


def calculate_follower_volume(
    *,
    mode: SizingMode,
    master_lots: float,
    master_balance: float | None = None,
    follower_balance: float | None = None,
    follower_equity: float | None = None,
    sizing_value: float | None = None,
) -> float:
    """Compute the lot size a follower's copy of a master's fill should use.

    - proportional: standard balance-ratio scaling
      (follower_balance / master_balance) * master_lots. No sizing_value.
    - fixed-lot: follower always trades exactly `sizing_value` lots,
      ignoring the master's lot size and both balances entirely.
    - micro-scale: same proportional scaling as `proportional`, but the
      result is floored at `sizing_value` (must be >= 0.01) instead of the
      broker minimum, so small accounts never get skipped on a trade that
      would otherwise round down to nothing.
    - risk-percent: only `sizing_value` percent of the follower's own
      equity is exposed per trade, scaled the same way as `proportional`
      (no stop-loss/pip-value involved - this is a balance-at-risk cap,
      not a stop-distance risk calculation).
    """
    if mode == "fixed-lot":
        if not sizing_value:
            raise ValueError("fixed-lot mode requires sizing_value")
        raw = sizing_value

    elif mode == "proportional":
        if not master_balance or not follower_balance:
            raise ValueError("proportional mode requires both master_balance and follower_balance")
        raw = (follower_balance / master_balance) * master_lots

    elif mode == "micro-scale":
        if not master_balance or not follower_balance:
            raise ValueError("micro-scale mode requires both master_balance and follower_balance")
        if not sizing_value or sizing_value < _MIN_LOT:
            raise ValueError("micro-scale mode requires sizing_value >= 0.01")
        raw = (follower_balance / master_balance) * master_lots
        stepped = round(raw / _DEFAULT_LOT_STEP) * _DEFAULT_LOT_STEP
        return max(round(stepped, 2), round(sizing_value, 2))

    elif mode == "risk-percent":
        if not master_balance or not follower_equity:
            raise ValueError("risk-percent mode requires both master_balance and follower_equity")
        if not sizing_value:
            raise ValueError("risk-percent mode requires sizing_value")
        raw = ((follower_equity * sizing_value / 100) / master_balance) * master_lots

    else:
        raise ValueError(f"Unknown sizing mode: {mode}")

    stepped = round(raw / _DEFAULT_LOT_STEP) * _DEFAULT_LOT_STEP
    return max(round(stepped, 2), _MIN_LOT)