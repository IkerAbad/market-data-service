"""Normalized market data records.

All timestamps are timezone-aware UTC. Prices and quantities are ``Decimal``
end to end: they are never converted to ``float``. The Parquet schema stores
them as ``decimal128(38, 12)``, which is exact for the eight fractional digits
Binance publishes and leaves room for derived quote values.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum


class Side(StrEnum):
    """Book side."""

    BID = "bid"
    ASK = "ask"


@dataclass(frozen=True, slots=True)
class Trade:
    """One aggregate trade from the Binance ``aggTrade`` stream."""

    symbol: str
    trade_id: int
    price: Decimal
    quantity: Decimal
    quote_quantity: Decimal
    event_time: datetime
    trade_time: datetime
    first_trade_id: int
    last_trade_id: int
    is_buyer_maker: bool
    ingested_at: datetime

    @property
    def side(self) -> str:
        """Aggressor side. A buyer-maker trade was sold into the bid."""
        return "sell" if self.is_buyer_maker else "buy"


@dataclass(frozen=True, slots=True)
class BookLevel:
    """One price level of the order book."""

    price: Decimal
    quantity: Decimal


@dataclass(frozen=True, slots=True)
class BookSnapshot:
    """Top-N order book captured at ``event_time``."""

    symbol: str
    last_update_id: int
    event_time: datetime
    ingested_at: datetime
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]


@dataclass(frozen=True, slots=True)
class DepthUpdate:
    """Diff depth event. Empty sides are valid."""

    symbol: str
    first_update_id: int
    last_update_id: int
    event_time: datetime
    ingested_at: datetime
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]


@dataclass(frozen=True, slots=True)
class Candle:
    """Aggregated OHLCV bucket.

    The bucket covers ``[open_time, open_time + interval_seconds)``.
    """

    symbol: str
    interval_seconds: int
    open_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Decimal
    trade_count: int
    closed: bool
