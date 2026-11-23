"""Parsing and validation of Binance payloads into normalized records.

The parser is deliberately strict. A malformed message raises ``ParseError``
instead of producing a half-valid record, so ingestion failures are visible in
logs and tests instead of silently corrupting the lake.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from market_data_service.models import BookLevel, BookSnapshot, DepthUpdate, Trade

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MAX_DECIMALS = Decimal("0.000000000001")


class ParseError(ValueError):
    """Raised when a payload does not match the expected Binance shape."""


def _as_mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ParseError(f"{field} must be an object")
    return value


def _as_list(value: object, field: str) -> list[object]:
    if not isinstance(value, list):
        raise ParseError(f"{field} must be a list")
    return value


def _as_str(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ParseError(f"{field} must be a non-empty string")
    return value


def _as_int(value: object, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ParseError(f"{field} must be an integer")
    if value < minimum:
        raise ParseError(f"{field} must be >= {minimum}")
    return value


def _as_bool(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise ParseError(f"{field} must be a boolean")
    return value


def _as_decimal(value: object, field: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, str):
        raise ParseError(f"{field} must be a decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ParseError(f"{field} is not a decimal: {value!r}") from exc
    if not parsed.is_finite():
        raise ParseError(f"{field} must be finite")
    if parsed < 0:
        raise ParseError(f"{field} must be non-negative")
    return parsed


def _as_ms(value: object, field: str) -> datetime:
    return _EPOCH + timedelta(milliseconds=_as_int(value, field))


def _as_levels(value: object, field: str) -> tuple[BookLevel, ...]:
    levels: list[BookLevel] = []
    for index, raw_level in enumerate(_as_list(value, field)):
        pair = _as_list(raw_level, f"{field}[{index}]")
        if len(pair) != 2:
            raise ParseError(f"{field}[{index}] must have two elements")
        levels.append(
            BookLevel(
                price=_as_decimal(pair[0], f"{field}[{index}].price"),
                quantity=_as_decimal(pair[1], f"{field}[{index}].quantity"),
            )
        )
    return tuple(levels)


def _quantize(value: Decimal) -> Decimal:
    """Round a derived value to the storage scale without binary floats."""
    return value.quantize(_MAX_DECIMALS)


def parse_agg_trade(data: Mapping[str, Any], *, ingested_at: datetime) -> Trade:
    """Parse the ``data`` object of an ``aggTrade`` stream event."""
    price = _as_decimal(data.get("p"), "p")
    quantity = _as_decimal(data.get("q"), "q")
    return Trade(
        symbol=_as_str(data.get("s"), "s").upper(),
        trade_id=_as_int(data.get("a"), "a", minimum=1),
        price=price,
        quantity=quantity,
        quote_quantity=_quantize(price * quantity),
        event_time=_as_ms(data.get("E"), "E"),
        trade_time=_as_ms(data.get("T"), "T"),
        first_trade_id=_as_int(data.get("f"), "f"),
        last_trade_id=_as_int(data.get("l"), "l"),
        is_buyer_maker=_as_bool(data.get("m"), "m"),
        ingested_at=ingested_at,
    )


def parse_depth_update(data: Mapping[str, Any], *, ingested_at: datetime) -> DepthUpdate:
    """Parse the ``data`` object of a ``depthUpdate`` stream event."""
    return DepthUpdate(
        symbol=_as_str(data.get("s"), "s").upper(),
        first_update_id=_as_int(data.get("U"), "U"),
        last_update_id=_as_int(data.get("u"), "u", minimum=1),
        event_time=_as_ms(data.get("E"), "E"),
        ingested_at=ingested_at,
        bids=_as_levels(data.get("b"), "b"),
        asks=_as_levels(data.get("a"), "a"),
    )


def parse_depth_snapshot(
    payload: Mapping[str, Any],
    *,
    symbol: str,
    ingested_at: datetime,
) -> BookSnapshot:
    """Parse ``GET /api/v3/depth`` into a snapshot."""
    raw_bids = _as_levels(payload.get("bids"), "bids")
    raw_asks = _as_levels(payload.get("asks"), "asks")
    bids = tuple(level for level in raw_bids if level.quantity > 0)
    asks = tuple(level for level in raw_asks if level.quantity > 0)
    return BookSnapshot(
        symbol=symbol.upper(),
        last_update_id=_as_int(payload.get("lastUpdateId"), "lastUpdateId"),
        event_time=ingested_at,
        ingested_at=ingested_at,
        bids=bids,
        asks=asks,
    )


def parse_rest_agg_trade(
    item: Mapping[str, Any],
    *,
    symbol: str,
    ingested_at: datetime,
) -> Trade:
    """Parse one element of ``GET /api/v3/aggTrades``."""
    price = _as_decimal(item.get("p"), "p")
    quantity = _as_decimal(item.get("q"), "q")
    return Trade(
        symbol=symbol.upper(),
        trade_id=_as_int(item.get("a"), "a", minimum=1),
        price=price,
        quantity=quantity,
        quote_quantity=_quantize(price * quantity),
        event_time=_as_ms(item.get("T"), "T"),
        trade_time=_as_ms(item.get("T"), "T"),
        first_trade_id=_as_int(item.get("f"), "f"),
        last_trade_id=_as_int(item.get("l"), "l"),
        is_buyer_maker=_as_bool(item.get("m"), "m"),
        ingested_at=ingested_at,
    )


def parse_stream_message(
    raw: str | bytes,
    *,
    ingested_at: datetime,
) -> Trade | DepthUpdate | None:
    """Parse a combined-stream frame.

    Unknown event types return ``None``: Binance sends subscription
    acknowledgements on the same socket and they are not market data.
    """
    try:
        payload: object = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ParseError("frame is not valid JSON") from exc
    envelope = _as_mapping(payload, "frame")
    data = envelope.get("data")
    if data is None:
        # Some deployments send bare payloads without the combined envelope.
        data = envelope
    event = _as_mapping(data, "data")
    event_type = event.get("e")
    if event_type == "aggTrade":
        return parse_agg_trade(event, ingested_at=ingested_at)
    if event_type == "depthUpdate":
        return parse_depth_update(event, ingested_at=ingested_at)
    return None


def parse_rest_agg_trades(
    payload: object,
    *,
    symbol: str,
    ingested_at: datetime,
) -> list[Trade]:
    """Parse a full ``GET /api/v3/aggTrades`` response."""
    if not isinstance(payload, Sequence) or isinstance(payload, (str, bytes)):
        raise ParseError("aggTrades response must be a list")
    return [
        parse_rest_agg_trade(
            _as_mapping(item, f"aggTrades[{index}]"), symbol=symbol, ingested_at=ingested_at
        )
        for index, item in enumerate(payload)
    ]
