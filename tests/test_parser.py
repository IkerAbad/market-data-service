"""Parser tests: normalization, decimals and validation."""

from __future__ import annotations

import json
from datetime import UTC, timedelta
from decimal import Decimal
from typing import Any

import pytest

from market_data_service.models import DepthUpdate, Trade
from market_data_service.parser import (
    ParseError,
    parse_depth_snapshot,
    parse_rest_agg_trades,
    parse_stream_message,
)
from tests.support import T0, agg_trade_payload, depth_update_payload

NOW = T0 + timedelta(milliseconds=10)


def _envelope(payload: dict[str, Any]) -> str:
    return json.dumps({"stream": "btcusdt@aggTrade", "data": payload})


def test_agg_trade_is_normalized_with_exact_decimals() -> None:
    event = parse_stream_message(_envelope(agg_trade_payload(42, ts=T0)), ingested_at=NOW)
    assert isinstance(event, Trade)
    assert event.symbol == "BTCUSDT"
    assert event.trade_id == 42
    assert event.price == Decimal("61264.83")
    assert event.quantity == Decimal("0.001")
    assert event.quote_quantity == Decimal("61.26483")
    assert event.event_time == T0
    assert event.event_time.tzinfo is UTC
    assert event.ingested_at == NOW


def test_agg_trade_carries_ids_and_side() -> None:
    payload = agg_trade_payload(8, ts=T0, buyer_maker=True)
    payload["f"] = 100
    payload["l"] = 104
    event = parse_stream_message(_envelope(payload), ingested_at=NOW)
    assert isinstance(event, Trade)
    assert event.first_trade_id == 100
    assert event.last_trade_id == 104
    assert event.side == "sell"


def test_bare_payload_without_envelope_is_accepted() -> None:
    frame = json.dumps(agg_trade_payload(3, ts=T0))
    event = parse_stream_message(frame, ingested_at=NOW)
    assert isinstance(event, Trade)
    assert event.trade_id == 3


def test_unknown_event_is_ignored() -> None:
    frame = json.dumps({"stream": "btcusdt@kline_1m", "data": {"e": "kline"}})
    assert parse_stream_message(frame, ingested_at=NOW) is None


def test_depth_update_allows_an_empty_side() -> None:
    payload = depth_update_payload(11, 14, bids=[], asks=[["100.5", "2"]])
    event = parse_stream_message(_envelope(payload), ingested_at=NOW)
    assert isinstance(event, DepthUpdate)
    assert event.first_update_id == 11
    assert event.last_update_id == 14
    assert event.bids == ()
    assert len(event.asks) == 1


def test_depth_snapshot_drops_zero_quantity_levels() -> None:
    snapshot = parse_depth_snapshot(
        {
            "lastUpdateId": 99,
            "bids": [["100.00", "0"], ["99.50", "3.5"]],
            "asks": [["101.00", "1.25"]],
        },
        symbol="btcusdt",
        ingested_at=NOW,
    )
    assert snapshot.symbol == "BTCUSDT"
    assert snapshot.last_update_id == 99
    assert [level.price for level in snapshot.bids] == [Decimal("99.50")]


def test_rest_agg_trades_are_parsed() -> None:
    payload = [agg_trade_payload(1), agg_trade_payload(2)]
    trades = parse_rest_agg_trades(payload, symbol="BTCUSDT", ingested_at=NOW)
    assert [trade.trade_id for trade in trades] == [1, 2]
    assert all(trade.symbol == "BTCUSDT" for trade in trades)


@pytest.mark.parametrize(
    "payload",
    [
        {"e": "aggTrade", "s": "BTCUSDT"},
        {**agg_trade_payload(1), "p": 1.0},
        {**agg_trade_payload(1), "q": "-1.0"},
        {**agg_trade_payload(1), "s": ""},
        {**agg_trade_payload(1), "a": 0},
    ],
)
def test_malformed_payloads_raise(payload: dict[str, Any]) -> None:
    with pytest.raises(ParseError):
        parse_stream_message(_envelope(payload), ingested_at=NOW)


def test_invalid_json_raises() -> None:
    with pytest.raises(ParseError):
        parse_stream_message("{not-json", ingested_at=NOW)
