"""
smc_structure.py
------------------
Deterministic structure-detection engine for the BOS / IDM / order-block
entry methodology (Smart Money Concepts). Consumes OHLC bars, produces a
directional signal (BUY / SELL / None) with an entry zone, once per
completed sequence: significant swing -> sweep (IDM) -> reversal ->
BOS -> order block -> entry zone.

No indicators beyond ATR are used. Every threshold below is a named
constant so it can be tuned against real chart history without touching
the detection logic itself.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence


FRACTAL_LEGS = 2
ATR_PERIOD = 14
SIGNIFICANT_SWING_ATR_MULT = 1.5
REVERSAL_CONFIRM_CLOSES = 2
MAX_BARS_IDM_TO_BOS = 40


@dataclass
class Bar:
    time: int
    open: float
    high: float
    low: float
    close: float


@dataclass
class SwingPoint:
    index: int
    price: float
    kind: str  # "high" or "low"
    significant: bool


@dataclass
class StructureSignal:
    direction: str  # "BUY" or "SELL"
    bos_index: int
    bos_price: float
    idm_index: int
    idm_price: float
    order_block_index: int
    zone_high: float
    zone_low: float
    entry_price: float


def compute_atr(bars: Sequence[Bar], period: int = ATR_PERIOD) -> List[Optional[float]]:
    trs: List[float] = []
    for i, bar in enumerate(bars):
        if i == 0:
            trs.append(bar.high - bar.low)
            continue
        prev_close = bars[i - 1].close
        tr = max(
            bar.high - bar.low,
            abs(bar.high - prev_close),
            abs(bar.low - prev_close),
        )
        trs.append(tr)

    atr: List[Optional[float]] = [None] * len(bars)
    for i in range(len(bars)):
        if i < period:
            continue
        if atr[i - 1] is None:
            atr[i] = sum(trs[i - period + 1 : i + 1]) / period
        else:
            atr[i] = (atr[i - 1] * (period - 1) + trs[i]) / period
    return atr


def find_fractals(bars: Sequence[Bar], legs: int = FRACTAL_LEGS) -> List[SwingPoint]:
    swings: List[SwingPoint] = []
    n = len(bars)
    for i in range(legs, n - legs):
        window = bars[i - legs : i + legs + 1]
        center = bars[i]
        if all(center.high >= b.high for b in window) and center.high == max(b.high for b in window):
            swings.append(SwingPoint(index=i, price=center.high, kind="high", significant=False))
        if all(center.low <= b.low for b in window) and center.low == min(b.low for b in window):
            swings.append(SwingPoint(index=i, price=center.low, kind="low", significant=False))
    swings.sort(key=lambda s: s.index)
    return swings


def mark_significant_swings(
    swings: Sequence[SwingPoint],
    atr: Sequence[Optional[float]],
    atr_mult: float = SIGNIFICANT_SWING_ATR_MULT,
) -> List[SwingPoint]:
    result = list(swings)
    last_opposite: dict = {"high": None, "low": None}
    for s in result:
        opposite_kind = "low" if s.kind == "high" else "high"
        prior = last_opposite[opposite_kind]
        if prior is not None and atr[s.index] is not None:
            leg_size = abs(s.price - prior.price)
            if leg_size >= atr_mult * atr[s.index]:
                s.significant = True
        last_opposite[s.kind] = s
    return result


def _last_significant_before(swings: Sequence[SwingPoint], index: int, kind: str) -> Optional[SwingPoint]:
    candidate = None
    for s in swings:
        if s.index >= index:
            break
        if s.kind == kind and s.significant:
            candidate = s
    return candidate


def _minor_swings_between(
    swings: Sequence[SwingPoint], start_index: int, end_index: int, kind: str
) -> List[SwingPoint]:
    return [
        s
        for s in swings
        if start_index <= s.index < end_index and s.kind == kind and not s.significant
    ]


def _detect_sweep_and_reversal(
    bars: Sequence[Bar], minor_swing: SwingPoint, direction: str
) -> Optional[int]:
    n = len(bars)
    for i in range(minor_swing.index + 1, n):
        bar = bars[i]
        if direction == "bullish":
            swept = bar.low < minor_swing.price and bar.close > minor_swing.price
        else:
            swept = bar.high > minor_swing.price and bar.close < minor_swing.price
        if not swept:
            continue
        confirmed = 0
        for j in range(i, min(i + REVERSAL_CONFIRM_CLOSES + 2, n)):
            cbar = bars[j]
            if direction == "bullish" and cbar.close > minor_swing.price:
                confirmed += 1
            elif direction == "bearish" and cbar.close < minor_swing.price:
                confirmed += 1
            else:
                confirmed = 0
            if confirmed >= REVERSAL_CONFIRM_CLOSES:
                return i
        continue
    return None


def _find_order_block(bars: Sequence[Bar], bos_index: int, direction: str) -> Optional[int]:
    opposing_color = "bearish" if direction == "BUY" else "bullish"
    for i in range(bos_index, -1, -1):
        bar = bars[i]
        is_bearish = bar.close < bar.open
        is_bullish = bar.close > bar.open
        if opposing_color == "bearish" and is_bearish:
            return i
        if opposing_color == "bullish" and is_bullish:
            return i
    return None


def detect_signals(bars: Sequence[Bar]) -> List[StructureSignal]:
    atr = compute_atr(bars)
    swings = find_fractals(bars)
    swings = mark_significant_swings(swings, atr)

    signals: List[StructureSignal] = []
    n = len(bars)

    for s in swings:
        if not s.significant:
            continue

        if s.kind == "low":
            minors = _minor_swings_between(swings, max(0, s.index - MAX_BARS_IDM_TO_BOS), s.index, "low")
            direction = "bearish"
            bos_direction = "SELL"
        else:
            minors = _minor_swings_between(swings, max(0, s.index - MAX_BARS_IDM_TO_BOS), s.index, "high")
            direction = "bullish"
            bos_direction = "BUY"

        for minor in minors:
            sweep_confirm_index = _detect_sweep_and_reversal(
                bars, minor, "bullish" if bos_direction == "BUY" else "bearish"
            )
            if sweep_confirm_index is None:
                continue

            bos_index = None
            for j in range(sweep_confirm_index, min(sweep_confirm_index + MAX_BARS_IDM_TO_BOS, n)):
                bar = bars[j]
                if bos_direction == "BUY" and bar.close > s.price:
                    bos_index = j
                    break
                if bos_direction == "SELL" and bar.close < s.price:
                    bos_index = j
                    break
            if bos_index is None:
                continue

            ob_index = _find_order_block(bars, bos_index, bos_direction)
            if ob_index is None:
                continue

            ob_bar = bars[ob_index]
            zone_high = max(ob_bar.open, ob_bar.close, ob_bar.high)
            zone_low = min(ob_bar.open, ob_bar.close, ob_bar.low)
            entry_price = (zone_high + zone_low) / 2.0

            signals.append(
                StructureSignal(
                    direction=bos_direction,
                    bos_index=bos_index,
                    bos_price=bars[bos_index].close,
                    idm_index=minor.index,
                    idm_price=minor.price,
                    order_block_index=ob_index,
                    zone_high=zone_high,
                    zone_low=zone_low,
                    entry_price=entry_price,
                )
            )

    signals.sort(key=lambda sig: sig.bos_index)

    deduped: List[StructureSignal] = []
    seen_bos_indices = set()
    for sig in signals:
        if sig.bos_index in seen_bos_indices:
            continue
        seen_bos_indices.add(sig.bos_index)
        deduped.append(sig)
    return deduped


def latest_signal(bars: Sequence[Bar]) -> Optional[StructureSignal]:
    signals = detect_signals(bars)
    return signals[-1] if signals else None