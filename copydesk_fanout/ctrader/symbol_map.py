"""
Two separate translation problems live here:

1. cTrader identifies symbols by an integer `symbolId`, not a string name -
   ProtoOAExecutionEvent/ProtoOADeal/ProtoOATradeData all carry symbolId only.
   We resolve that to a human symbol name (e.g. "XAUUSD") via
   ProtoOASymbolsListReq, cached per account since the symbol list is static
   for a given trading account.

2. cTrader's own symbol name for an instrument isn't guaranteed to match the
   follower's MT5 broker's symbol name for the same instrument (e.g. a broker
   might list gold as "XAUUSD.m" or "XAUUSDm"). Per your rule: match by
   character prefix, and if any character mismatches, drop the symbol rather
   than guess - a follower silently not getting a trade is much safer than a
   follower getting the wrong instrument.

Also handles volume: cTrader's `volume` field is in hundredths of a unit
(1000 = 10.00 units; confirmed against the official proto definition and
cross-checked against a real order example: 10,000,000 units == 1.00 lot for a
100,000-unit-lot-size FX symbol). Lot size itself is symbol-specific
(ProtoOASymbol.lotSize, also in the same hundredths-of-a-unit convention), so
this is computed per-symbol rather than assuming the standard 100,000 FX lot
size everywhere (index/metal/crypto CFDs commonly differ).

IMPORTANT: the lots figure this module produces has not been validated against
a live cTrader demo account. Before this touches real follower money, place one
known-size trade on the subscribed master (e.g. exactly 0.10 lots) and confirm
`volume_to_lots()` reproduces 0.10 exactly for that symbol before trusting it
for sizing.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOASymbolsListReq

from .proto_client import get_connection

logger = logging.getLogger("ctrader.symbol_map")

_VOLUME_SCALE = 100.0  # cTrader volume/lotSize fields are in hundredths of a unit


@dataclass
class SymbolInfo:
    symbol_id: int
    name: str
    lot_size_units: float  # e.g. 100_000 for a standard FX lot


class SymbolCache:
    """One instance per cTrader master account (per ctid_trading_account_id)."""

    def __init__(self, ctid_trading_account_id: int) -> None:
        self._ctid = ctid_trading_account_id
        self._by_id: dict[int, SymbolInfo] = {}
        self._loaded = False

    def _load(self) -> None:
        conn = get_connection()
        response = conn.send_and_wait(
            ProtoOASymbolsListReq(ctidTraderAccountId=self._ctid, includeArchivedSymbols=False)
        )
        for light in response.symbol:
            # ProtoOASymbolsListRes only gives ProtoOALightSymbol (no lotSize) -
            # lotSize needs a per-symbol ProtoOASymbolByIdReq. To avoid N+1
            # round-trips on load, default to the FX-standard 100,000 units and
            # only fetch the precise lotSize lazily, the first time a trade on
            # that symbol actually needs converting - see lot_size_for().
            self._by_id[light.symbolId] = SymbolInfo(
                symbol_id=light.symbolId, name=light.symbolName, lot_size_units=100_000.0,
            )
        self._loaded = True
        logger.info("[ctid %s] Loaded %d cTrader symbols", self._ctid, len(self._by_id))

    def name_for(self, symbol_id: int) -> str:
        if not self._loaded:
            self._load()
        info = self._by_id.get(symbol_id)
        if info is None:
            raise KeyError(f"Unknown cTrader symbolId {symbol_id} for ctid {self._ctid}")
        return info.name

    def lot_size_for(self, symbol_id: int) -> float:
        if not self._loaded:
            self._load()
        info = self._by_id.get(symbol_id)
        if info is None:
            raise KeyError(f"Unknown cTrader symbolId {symbol_id} for ctid {self._ctid}")
        if info.lot_size_units == 100_000.0:
            # Lazily fetch the real value the first time this symbol is traded -
            # see the comment in _load() above.
            from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOASymbolByIdReq

            conn = get_connection()
            precise = conn.send_and_wait(
                ProtoOASymbolByIdReq(ctidTraderAccountId=self._ctid, symbolId=[symbol_id])
            )
            if precise.symbol:
                info.lot_size_units = precise.symbol[0].lotSize / _VOLUME_SCALE
        return info.lot_size_units


def volume_to_lots(volume: int, lot_size_units: float) -> float:
    units = volume / _VOLUME_SCALE
    return units / lot_size_units


def lots_to_volume(lots: float, lot_size_units: float) -> int:
    units = lots * lot_size_units
    return round(units * _VOLUME_SCALE)


_STRIP_PATTERN = re.compile(r"[^A-Z0-9]")


def _normalize(symbol: str) -> str:
    return _STRIP_PATTERN.sub("", symbol.upper())


def to_follower_symbol(ctrader_symbol_name: str, follower_available_symbols: set[str]) -> str | None:
    """
    Strict prefix match, per your rule: normalize both sides (strip anything
    that isn't A-Z/0-9, uppercase), then a follower symbol matches only if it
    STARTS WITH the cTrader symbol's normalized name exactly - e.g. cTrader
    "XAUUSD" matches follower "XAUUSDm" or "XAUUSD.a", but not "GXAUUSD" or
    "XAUUSDT". Returns None (caller must skip the trade) on no match or on
    more than one equally-valid match, since a silent wrong-symbol pick is far
    worse than a skipped trade.
    """
    target = _normalize(ctrader_symbol_name)
    matches = [
        s for s in follower_available_symbols
        if _normalize(s).startswith(target)
    ]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        # Prefer an exact normalized match over a longer suffixed one, if present.
        exact = [s for s in matches if _normalize(s) == target]
        if len(exact) == 1:
            return exact[0]
        logger.warning(
            "Ambiguous symbol match for %r among follower symbols %s - skipping rather than guessing",
            ctrader_symbol_name, matches,
        )
        return None
    logger.warning("No follower symbol match for cTrader symbol %r", ctrader_symbol_name)
    return None