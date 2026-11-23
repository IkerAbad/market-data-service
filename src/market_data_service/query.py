"""DuckDB analytics over the Parquet lake.

Candles are computed from stored trades with ordered aggregates, so the lake is
the only source of truth. The API merges these rows with the live in-memory
aggregator to cover the few seconds a buffer may still be unsaved.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, TypeVar

import duckdb

from market_data_service.models import Candle

T = TypeVar("T")


class Analytics:
    """Single-threaded DuckDB access, serialized through an executor."""

    def __init__(self, data_dir: Path) -> None:
        self._data_dir = data_dir
        self._connection = duckdb.connect(":memory:")
        self._connection.execute("SET threads TO 2")
        self._connection.execute("SET TimeZone = 'UTC'")
        self._lock = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="duckdb")
        self._closed = False

    def close(self) -> None:
        """Shut down the executor and the connection."""
        if self._closed:
            return
        self._closed = True
        self._executor.shutdown(wait=True)
        self._connection.close()

    async def _run(self, operation: Callable[[], T]) -> T:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, operation)

    # ---- synchronous core, used by startup and tests -----------------------

    def _trades_glob(self, symbol: str) -> str:
        path = self._data_dir / "trades" / f"symbol={symbol}" / "date=*" / "*.parquet"
        if not any((self._data_dir / "trades" / f"symbol={symbol}").glob("date=*/*.parquet")):
            raise LookupError(f"no trades stored for {symbol}")
        return path.as_posix().replace("'", "''")

    def latest_trade(self, symbol: str) -> dict[str, Any] | None:
        """Most recent trade, or ``None`` when the lake is empty."""
        try:
            glob = self._trades_glob(symbol)
        except LookupError:
            return None
        with self._lock:
            row = self._connection.execute(
                f"""
                SELECT symbol, trade_id, price, quantity, quote_quantity,
                       event_time, trade_time, is_buyer_maker, ingested_at
                FROM read_parquet('{glob}', hive_partitioning = false)
                WHERE symbol = ?
                ORDER BY trade_id DESC
                LIMIT 1
                """,
                [symbol],
            ).fetchone()
        if row is None:
            return None
        keys = (
            "symbol",
            "trade_id",
            "price",
            "quantity",
            "quote_quantity",
            "event_time",
            "trade_time",
            "is_buyer_maker",
            "ingested_at",
        )
        return dict(zip(keys, row, strict=True))

    def max_trade_id(self, symbol: str) -> int | None:
        """Highest stored trade id, used to resume the gap sequence."""
        try:
            glob = self._trades_glob(symbol)
        except LookupError:
            return None
        with self._lock:
            row = self._connection.execute(
                f"SELECT max(trade_id) FROM read_parquet('{glob}', hive_partitioning = false) "
                "WHERE symbol = ?",
                [symbol],
            ).fetchone()
        if row is None or row[0] is None:
            return None
        return int(row[0])

    def trade_count(self, symbol: str) -> int:
        """Number of stored trades."""
        try:
            glob = self._trades_glob(symbol)
        except LookupError:
            return 0
        with self._lock:
            row = self._connection.execute(
                f"SELECT count(*) FROM read_parquet('{glob}', hive_partitioning = false) "
                "WHERE symbol = ?",
                [symbol],
            ).fetchone()
        return int(row[0]) if row is not None else 0

    def candles(self, symbol: str, interval_seconds: int, limit: int) -> list[Candle]:
        """Aggregate stored trades into fixed-width OHLCV buckets."""
        if interval_seconds <= 0:
            raise ValueError("interval must be positive")
        try:
            glob = self._trades_glob(symbol)
        except LookupError:
            return []
        with self._lock:
            rows = self._connection.execute(
                f"""
                SELECT CAST(floor(epoch(trade_time) / ?) * ? AS BIGINT) AS bucket,
                       first(price ORDER BY trade_time, trade_id) AS open,
                       max(price) AS high,
                       min(price) AS low,
                       last(price ORDER BY trade_time, trade_id) AS close,
                       sum(quantity) AS volume,
                       sum(quote_quantity) AS quote_volume,
                       count(*) AS trade_count
                FROM read_parquet('{glob}', hive_partitioning = false)
                WHERE symbol = ?
                GROUP BY bucket
                ORDER BY bucket DESC
                LIMIT ?
                """,
                [interval_seconds, interval_seconds, symbol, limit],
            ).fetchall()
        candles: list[Candle] = []
        for row in reversed(rows):
            candles.append(
                Candle(
                    symbol=symbol,
                    interval_seconds=interval_seconds,
                    open_time=datetime.fromtimestamp(int(row[0]), tz=UTC),
                    open=_as_decimal(row[1]),
                    high=_as_decimal(row[2]),
                    low=_as_decimal(row[3]),
                    close=_as_decimal(row[4]),
                    volume=_as_decimal(row[5]),
                    quote_volume=_as_decimal(row[6]),
                    trade_count=int(row[7]),
                    closed=True,
                )
            )
        return candles

    def latest_book(self, symbol: str, levels: int = 5) -> dict[str, list[dict[str, str]]] | None:
        """Latest persisted book snapshot, useful after a restart."""
        path = self._data_dir / "book" / f"symbol={symbol}"
        if not any(path.glob("date=*/*.parquet")):
            return None
        glob = (path / "date=*" / "*.parquet").as_posix().replace("'", "''")
        with self._lock:
            rows = self._connection.execute(
                f"""
                WITH latest AS (
                    SELECT max(last_update_id) AS id
                    FROM read_parquet('{glob}', hive_partitioning = false)
                    WHERE symbol = ?
                )
                SELECT side, level, price, quantity
                FROM read_parquet('{glob}', hive_partitioning = false)
                WHERE symbol = ? AND last_update_id = (SELECT id FROM latest) AND level < ?
                ORDER BY side DESC, level ASC
                """,
                [symbol, symbol, levels],
            ).fetchall()
        if not rows:
            return None
        book: dict[str, list[dict[str, str]]] = {"bids": [], "asks": []}
        for side, _level, price, quantity in rows:
            key = "bids" if side == "bid" else "asks"
            book[key].append({"price": str(price), "quantity": str(quantity)})
        return book

    def storage_summary(self, symbol: str) -> dict[str, int]:
        """File and byte counts for both datasets."""
        summary: dict[str, int] = {}
        for dataset in ("trades", "book"):
            files = list((self._data_dir / dataset / f"symbol={symbol}").glob("date=*/*.parquet"))
            summary[f"{dataset}_files"] = len(files)
            summary[f"{dataset}_bytes"] = sum(path.stat().st_size for path in files)
        return summary

    # ---- async wrappers for request handlers ------------------------------

    async def latest_trade_async(self, symbol: str) -> dict[str, Any] | None:
        """Async wrapper for :meth:`latest_trade`."""
        return await self._run(lambda: self.latest_trade(symbol))

    async def candles_async(self, symbol: str, interval_seconds: int, limit: int) -> list[Candle]:
        """Async wrapper for :meth:`candles`."""
        return await self._run(lambda: self.candles(symbol, interval_seconds, limit))

    async def trade_count_async(self, symbol: str) -> int:
        """Async wrapper for :meth:`trade_count`."""
        return await self._run(lambda: self.trade_count(symbol))

    async def latest_book_async(
        self, symbol: str, levels: int = 5
    ) -> dict[str, list[dict[str, str]]] | None:
        """Async wrapper for :meth:`latest_book`."""
        return await self._run(lambda: self.latest_book(symbol, levels))

    async def storage_summary_async(self, symbol: str) -> dict[str, int]:
        """Async wrapper for :meth:`storage_summary`."""
        return await self._run(lambda: self.storage_summary(symbol))


def _as_decimal(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, str)):
        return Decimal(value)
    raise TypeError(f"unexpected numeric type: {type(value)!r}")
