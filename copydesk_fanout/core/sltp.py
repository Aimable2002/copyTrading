from __future__ import annotations


def sl_tp_distance(*, order_type: str, entry_price: float, sl: float, tp: float) -> tuple[float, float]:
    sl_distance = abs(entry_price - sl) if sl else 0.0
    tp_distance = abs(entry_price - tp) if tp else 0.0
    return sl_distance, tp_distance


def apply_sl_tp_distance(
    *, order_type: str, entry_price: float, sl_distance: float, tp_distance: float
) -> tuple[float, float]:

    is_buy = order_type.lower() == "buy"

    sl = 0.0
    if sl_distance:
        sl = entry_price - sl_distance if is_buy else entry_price + sl_distance

    tp = 0.0
    if tp_distance:
        tp = entry_price + tp_distance if is_buy else entry_price - tp_distance

    return sl, tp


def pending_price_distance(*, reference_price: float, pending_price: float) -> float:
    return abs(pending_price - reference_price)


def apply_pending_price_distance(*, order_type: str, reference_price: float, distance: float) -> float:
    below = order_type.lower() in ("buy_limit", "sell_stop")
    return reference_price - distance if below else reference_price + distance