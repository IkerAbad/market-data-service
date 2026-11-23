"""Live OHLCV aggregation.

Candles are derived from the trade stream and kept in memory for the live feed
and the API. The Parquet lake remains the source of truth: the API merges the
persisted candles with this cache, so the dashboard never shows a hole while a
buffer is waiting to be flushed.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from math import floor

from market_data_service.models import Candle, Trade

SUPPORTED_INTERVALS: tuple[int, ...] = (1, 5, 60)


@dataclass(slots=True)
class _Bucket:
    open_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Decimal
    trade_count: int


class CandleAggregator:
    """Fixed-interval OHLCV aggregation driven by aggregate trades.

    Trades are processed in ``trade_id`` order by the ingestor, so the first and
    last price seen in a bucket are its open and close. A late trade from an
    already closed bucket is ignored instead of rewriting history.
    """

    def __init__(
        self,
        symbol: str,
        intervals: tuple[int, ...] = SUPPORTED_INTERVALS,
        history: int = 2_000,
    ) -> None:
        if not intervals:
            raise ValueError("at least one interval is required")
        self._symbol = symbol
        self._intervals = tuple(sorted(set(intervals)))
        self._history = history
        self._current: dict[int, _Bucket] = {}
        self._closed: dict[int, deque[Candle]] = {
            interval: deque(maxlen=history) for interval in self._intervals
        }

    @property
    def intervals(self) -> tuple[int, ...]:
        """Intervals maintained by this aggregator."""
        return self._intervals

    def seed(self, candles: list[Candle]) -> None:
        """Load closed candles from storage into the live cache."""
        for candle in candles:
            if not candle.closed or candle.interval_seconds not in self._closed:
                continue
            bucket = self._closed[candle.interval_seconds]
            if bucket and candle.open_time <= bucket[-1].open_time:
                continue
            bucket.append(candle)
            self._current.pop(candle.interval_seconds, None)

    def on_trade(self, trade: Trade) -> list[Candle]:
        """Update every interval and return the candles that just closed."""
        closed: list[Candle] = []
        for interval in self._intervals:
            bucket_start = _bucket_start(trade.trade_time, interval)
            current = self._current.get(interval)
            if current is not None and bucket_start < current.open_time:
                continue
            if current is not None and bucket_start > current.open_time:
                closed.append(_finalize(self._symbol, interval, current))
                self._closed[interval].append(closed[-1])
            bucket = self._current.get(interval)
            if bucket is None or bucket.open_time != bucket_start:
                bucket = _Bucket(
                    open_time=bucket_start,
                    open=trade.price,
                    high=trade.price,
                    low=trade.price,
                    close=trade.price,
                    volume=Decimal(0),
                    quote_volume=Decimal(0),
                    trade_count=0,
                )
                self._current[interval] = bucket
            bucket.high = max(bucket.high, trade.price)
            bucket.low = min(bucket.low, trade.price)
            bucket.close = trade.price
            bucket.volume += trade.quantity
            bucket.quote_volume += trade.quote_quantity
            bucket.trade_count += 1
        return closed

    def recent(self, interval: int, limit: int) -> list[Candle]:
        """Return the most recent closed candles plus the live bucket."""
        if interval not in self._closed:
            raise ValueError(f"unsupported interval {interval}")
        candles = list(self._closed[interval])[-limit:]
        current = self._current.get(interval)
        if current is not None:
            candles.append(_finalize(self._symbol, interval, current, closed=False))
        return candles


def _bucket_start(ts: datetime, interval: int) -> datetime:
    epoch_seconds = int(ts.astimezone(UTC).timestamp())
    start = floor(epoch_seconds / interval) * interval
    return datetime.fromtimestamp(start, tz=UTC)


def _finalize(symbol: str, interval: int, bucket: _Bucket, *, closed: bool = True) -> Candle:
    return Candle(
        symbol=symbol,
        interval_seconds=interval,
        open_time=bucket.open_time,
        open=bucket.open,
        high=bucket.high,
        low=bucket.low,
        close=bucket.close,
        volume=bucket.volume,
        quote_volume=bucket.quote_volume,
        trade_count=bucket.trade_count,
        closed=closed,
    )


def bucket_end(open_time: datetime, interval: int) -> datetime:
    """Exclusive end of a candle bucket."""
    return open_time + timedelta(seconds=interval)


__all__ = ["SUPPORTED_INTERVALS", "CandleAggregator", "bucket_end"]
