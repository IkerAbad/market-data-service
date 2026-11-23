"""Shared builders for deterministic market data fixtures."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

SYMBOL = "BTCUSDT"
T0 = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)
QUANTUM = Decimal("0.000000000001")


def ms(value: datetime) -> int:
    """Milliseconds since the Unix epoch."""
    return int(value.timestamp() * 1000)


def agg_trade_payload(
    trade_id: int,
    *,
    ts: datetime = T0,
    price: str = "61264.83",
    quantity: str = "0.001",
    symbol: str = SYMBOL,
    buyer_maker: bool = False,
) -> dict[str, Any]:
    """Payload of one ``aggTrade`` event, without the stream envelope."""
    return {
        "e": "aggTrade",
        "E": ms(ts),
        "s": symbol,
        "a": trade_id,
        "p": price,
        "q": quantity,
        "f": trade_id,
        "l": trade_id,
        "T": ms(ts),
        "m": buyer_maker,
        "M": True,
    }


def agg_trade_frame(
    trade_id: int,
    *,
    ts: datetime = T0,
    price: str = "61264.83",
    quantity: str = "0.001",
) -> str:
    """Combined-stream frame for one aggregate trade."""
    payload = agg_trade_payload(trade_id, ts=ts, price=price, quantity=quantity)
    return json.dumps({"stream": f"{SYMBOL.lower()}@aggTrade", "data": payload})


def depth_update_payload(
    first_update_id: int,
    last_update_id: int,
    *,
    ts: datetime = T0,
    bids: list[list[str]] | None = None,
    asks: list[list[str]] | None = None,
) -> dict[str, Any]:
    """Payload of one ``depthUpdate`` event."""
    return {
        "e": "depthUpdate",
        "E": ms(ts),
        "T": ms(ts),
        "s": SYMBOL,
        "U": first_update_id,
        "u": last_update_id,
        "b": bids if bids is not None else [["100.00", "1.0"]],
        "a": asks if asks is not None else [["100.50", "2.0"]],
    }


def make_trade(
    trade_id: int,
    *,
    price: str = "100.00",
    quantity: str = "1.0",
    ts: datetime = T0,
    symbol: str = SYMBOL,
    buyer_maker: bool = False,
    ingested_at: datetime | None = None,
) -> Any:
    """Build a normalized :class:`Trade`."""
    from market_data_service.models import Trade

    price_decimal = Decimal(price)
    quantity_decimal = Decimal(quantity)
    return Trade(
        symbol=symbol,
        trade_id=trade_id,
        price=price_decimal,
        quantity=quantity_decimal,
        quote_quantity=(price_decimal * quantity_decimal).quantize(QUANTUM),
        event_time=ts,
        trade_time=ts,
        first_trade_id=trade_id,
        last_trade_id=trade_id,
        is_buyer_maker=buyer_maker,
        ingested_at=ingested_at or ts + timedelta(milliseconds=5),
    )
