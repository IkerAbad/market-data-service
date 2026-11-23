"""Aggregator tests: OHLC, volumes, bucket boundaries and late data."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from market_data_service.aggregator import CandleAggregator
from tests.support import T0, make_trade


def _at(seconds: float) -> datetime:
    return T0 + timedelta(seconds=seconds)


def test_candle_ohlcv_is_built_from_trades() -> None:
    aggregator = CandleAggregator("BTCUSDT", (1,))
    assert aggregator.on_trade(make_trade(1, ts=_at(0.1), price="100", quantity="1")) == []
    assert aggregator.on_trade(make_trade(2, ts=_at(0.5), price="99", quantity="2")) == []
    assert aggregator.on_trade(make_trade(3, ts=_at(0.9), price="102", quantity="0.5")) == []

    closed = aggregator.on_trade(make_trade(4, ts=_at(1.1), price="101", quantity="3"))
    assert len(closed) == 1
    candle = closed[0]
    assert candle.open_time == T0
    assert candle.open == Decimal("100")
    assert candle.high == Decimal("102")
    assert candle.low == Decimal("99")
    assert candle.close == Decimal("102")
    assert candle.volume == Decimal("3.5")
    assert candle.trade_count == 3
    assert candle.closed is True


def test_every_interval_rolls_over_independently() -> None:
    aggregator = CandleAggregator("BTCUSDT", (1, 60))
    aggregator.on_trade(make_trade(1, ts=_at(0.0), price="100"))
    aggregator.on_trade(make_trade(2, ts=_at(59.9), price="105"))
    closed = aggregator.on_trade(make_trade(3, ts=_at(60.0), price="110"))

    intervals = {candle.interval_seconds for candle in closed}
    assert intervals == {1, 60}
    minute = next(candle for candle in closed if candle.interval_seconds == 60)
    assert minute.open_time == T0
    assert minute.open == Decimal("100")
    assert minute.close == Decimal("105")
    assert minute.trade_count == 2


def test_late_trade_from_a_closed_bucket_is_ignored() -> None:
    aggregator = CandleAggregator("BTCUSDT", (1,))
    aggregator.on_trade(make_trade(1, ts=_at(1.2), price="100"))
    aggregator.on_trade(make_trade(2, ts=_at(1.4), price="101"))
    aggregator.on_trade(make_trade(3, ts=_at(0.5), price="1"))

    recent = aggregator.recent(1, 10)
    assert len(recent) == 1
    assert recent[0].open == Decimal("100")
    assert recent[0].close == Decimal("101")


def test_recent_includes_the_live_bucket() -> None:
    aggregator = CandleAggregator("BTCUSDT", (1,))
    aggregator.on_trade(make_trade(1, ts=_at(0.2), price="100"))
    aggregator.on_trade(make_trade(2, ts=_at(1.2), price="101"))
    aggregator.on_trade(make_trade(3, ts=_at(1.4), price="102"))

    recent = aggregator.recent(1, 10)
    assert [candle.open_time for candle in recent] == [T0, T0 + timedelta(seconds=1)]
    assert recent[-1].closed is False
    assert recent[-1].close == Decimal("102")


def test_seeded_history_is_kept_before_live_trades() -> None:
    aggregator = CandleAggregator("BTCUSDT", (1,))
    aggregator.on_trade(make_trade(1, ts=_at(0.2), price="100"))
    aggregator.on_trade(make_trade(2, ts=_at(1.2), price="101"))
    closed = aggregator.recent(1, 10)[0]
    aggregator.seed([closed])

    assert aggregator.recent(1, 10)[0].open_time == closed.open_time
    assert aggregator.recent(1, 10)[0].closed is True


def test_unsupported_interval_is_rejected() -> None:
    aggregator = CandleAggregator("BTCUSDT", (1,))
    with pytest.raises(ValueError, match="unsupported"):
        aggregator.recent(60, 10)
