"""Composition root for the ingestion pipeline.

The pipeline is the single place where an accepted trade becomes: a buffered
Parquet row, an update to the live candles, a WebSocket frame for dashboards and
a Prometheus counter increment.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import UTC, datetime
from typing import Any

from market_data_service import metrics
from market_data_service.aggregator import CandleAggregator
from market_data_service.config import Settings
from market_data_service.depth import DepthBook
from market_data_service.hub import EventHub
from market_data_service.models import BookSnapshot, Candle, Trade
from market_data_service.storage import ParquetStore

log = logging.getLogger(__name__)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def trade_message(trade: Trade) -> dict[str, Any]:
    """JSON-safe trade frame."""
    return {
        "type": "trade",
        "symbol": trade.symbol,
        "trade_id": trade.trade_id,
        "price": str(trade.price),
        "quantity": str(trade.quantity),
        "side": trade.side,
        "time": _iso(trade.trade_time),
    }


def candle_message(candle: Candle) -> dict[str, Any]:
    """JSON-safe candle frame."""
    return {
        "type": "candle",
        "symbol": candle.symbol,
        "interval": candle.interval_seconds,
        "open_time": _iso(candle.open_time),
        "open": str(candle.open),
        "high": str(candle.high),
        "low": str(candle.low),
        "close": str(candle.close),
        "volume": str(candle.volume),
        "trade_count": candle.trade_count,
        "closed": candle.closed,
    }


def book_message(snapshot: BookSnapshot) -> dict[str, Any]:
    """JSON-safe top-of-book frame."""
    return {
        "type": "book",
        "symbol": snapshot.symbol,
        "update_id": snapshot.last_update_id,
        "time": _iso(snapshot.event_time),
        "bids": [[str(level.price), str(level.quantity)] for level in snapshot.bids],
        "asks": [[str(level.price), str(level.quantity)] for level in snapshot.asks],
    }


class Pipeline:
    """Fan-out sink used by the ingestor."""

    def __init__(
        self,
        settings: Settings,
        *,
        store: ParquetStore,
        aggregator: CandleAggregator,
        depth: DepthBook,
        hub: EventHub,
    ) -> None:
        self._settings = settings
        self._store = store
        self._aggregator = aggregator
        self._depth = depth
        self._hub = hub
        self._tasks: list[asyncio.Task[None]] = []
        self._last_trade: Trade | None = None

    @property
    def last_trade(self) -> Trade | None:
        """Most recent accepted trade, including rows still buffered."""
        return self._last_trade

    async def on_trade(self, trade: Trade) -> None:
        """Persist, aggregate and broadcast one trade."""
        self._last_trade = trade
        self._store.append_trade(trade)
        for closed in self._aggregator.on_trade(trade):
            self._hub.publish(candle_message(closed))
        self._hub.publish(trade_message(trade))

    async def start(self) -> None:
        """Start the flush, book sampling and candle ticker tasks."""
        self._tasks = [
            asyncio.create_task(self._flush_loop(), name="flush"),
            asyncio.create_task(self._book_loop(), name="book-sampler"),
            asyncio.create_task(self._candle_loop(), name="candle-ticker"),
        ]

    async def stop(self) -> None:
        """Stop background tasks and flush everything to disk."""
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._tasks.clear()
        metrics.refresh_writer_gauges(self._store.buffered_rows)
        if self._store.buffered_rows:
            await asyncio.to_thread(self._store.flush_all)

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(0.5)
            if self._store.buffered_rows == 0:
                continue
            await asyncio.to_thread(self._store.flush_stale)
            if self._store.buffered_rows >= self._settings.flush_max_rows:
                await asyncio.to_thread(self._store.flush_all)
            metrics.refresh_writer_gauges(self._store.buffered_rows)
            metrics.PARQUET_FILES.set(self._store.files_written)
            metrics.PARQUET_ROWS.set(self._store.rows_written)

    async def _book_loop(self) -> None:
        while True:
            await asyncio.sleep(self._settings.depth_snapshot_interval_s)
            snapshot = self._depth.snapshot(self._settings.depth_levels)
            if snapshot is None:
                continue
            self._store.append_book(snapshot)
            metrics.BOOK_SNAPSHOTS.inc()
            self._hub.publish(book_message(snapshot))

    async def _candle_loop(self) -> None:
        while True:
            await asyncio.sleep(0.5)
            for interval in self._aggregator.intervals:
                recent = self._aggregator.recent(interval, 1)
                if recent:
                    self._hub.publish(candle_message(recent[-1]))
