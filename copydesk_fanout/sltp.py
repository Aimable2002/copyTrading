from __future__ import annotations


def sl_tp_distance(*, order_type: str, entry_price: float, sl: float, tp: float) -> tuple[float, float]:
    """
    Distance (in price units, not pips) between entry and SL/TP. Direction-
    agnostic on purpose - a distance is always a positive number regardless
    of buy/sell, direction is reapplied in apply_sl_tp_distance below.
    Returns (sl_distance, tp_distance); either is 0 if that side isn't set.
    """
    sl_distance = abs(entry_price - sl) if sl else 0.0
    tp_distance = abs(entry_price - tp) if tp else 0.0
    return sl_distance, tp_distance


def apply_sl_tp_distance(
    *, order_type: str, entry_price: float, sl_distance: float, tp_distance: float
) -> tuple[float, float]:
    """
    Reapplies a distance to a (possibly different) entry price, in the
    correct direction for the order type. This is the actual "inherit the
    master's risk profile, not their absolute price" logic: a follower's
    fill price will rarely match the master's exactly (different broker,
    spread, slippage, timing), so copying absolute SL/TP prices verbatim
    can put a follower's stop on the wrong side of their own entry - copying
    the *distance* and reapplying it to their real entry avoids that.

    For a buy: SL sits below entry, TP sits above.
    For a sell: SL sits above entry, TP sits below.
    """
    is_buy = order_type.lower() == "buy"

    sl = 0.0
    if sl_distance:
        sl = entry_price - sl_distance if is_buy else entry_price + sl_distance

    tp = 0.0
    if tp_distance:
        tp = entry_price + tp_distance if is_buy else entry_price - tp_distance

    return sl, tp
