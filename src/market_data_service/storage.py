"""Partitioned Parquet storage with size- and time-based rotation.

Layout:

    {data_dir}/trades/symbol={SYMBOL}/date={YYYY-MM-DD}/part-*.parquet
    {data_dir}/book/symbol={SYMBOL}/date={YYYY-MM-DD}/part-*.parquet

A file is written when the in-memory buffer reaches ``max_rows`` or
``max_bytes``, or when the oldest buffered record is ``max_age_s`` old. Writes
go to a temporary file in the destination directory and are moved into place
with ``os.replace``, so readers never see a partial Parquet file.
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import pyarrow.parquet as pq

from market_data_service.models import BookSnapshot, Side, Trade

log = logging.getLogger(__name__)

DECIMAL_TYPE: Final = pa.decimal128(38, 12)
TS_TYPE: Final = pa.timestamp("us", tz="UTC")

TRADE_SCHEMA: Final = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("trade_id", pa.uint64(), nullable=False),
        pa.field("price", DECIMAL_TYPE, nullable=False),
        pa.field("quantity", DECIMAL_TYPE, nullable=False),
        pa.field("quote_quantity", DECIMAL_TYPE, nullable=False),
        pa.field("event_time", TS_TYPE, nullable=False),
        pa.field("trade_time", TS_TYPE, nullable=False),
        pa.field("first_trade_id", pa.uint64(), nullable=False),
        pa.field("last_trade_id", pa.uint64(), nullable=False),
        pa.field("is_buyer_maker", pa.bool_(), nullable=False),
        pa.field("ingested_at", TS_TYPE, nullable=False),
    ],
    metadata={"schema": "market-data-service/trades/v1", "price_scale": "12", "timezone": "UTC"},
)

BOOK_SCHEMA: Final = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("last_update_id", pa.uint64(), nullable=False),
        pa.field("event_time", TS_TYPE, nullable=False),
        pa.field("ingested_at", TS_TYPE, nullable=False),
        pa.field("side", pa.string(), nullable=False),
        pa.field("level", pa.uint16(), nullable=False),
        pa.field("price", DECIMAL_TYPE, nullable=False),
        pa.field("quantity", DECIMAL_TYPE, nullable=False),
    ],
    metadata={"schema": "market-data-service/book/v1", "price_scale": "12", "timezone": "UTC"},
)


@dataclass(frozen=True, slots=True)
class RotationPolicy:
    """Flush triggers, checked on append and on the periodic flush tick."""

    max_rows: int = 50_000
    max_bytes: int = 64 * 1024 * 1024
    max_age_s: float = 5.0

    def __post_init__(self) -> None:
        if self.max_rows < 1 or self.max_bytes < 1 or self.max_age_s <= 0:
            raise ValueError("rotation thresholds must be positive")


@dataclass(slots=True)
class _Buffer:
    rows: list[dict[str, Any]] = field(default_factory=list)
    opened_at: float = field(default_factory=time.monotonic)
    nbytes: int = 0


class ParquetStore:
    """Synchronous partitioned writer. Call ``flush`` from a worker thread."""

    def __init__(self, data_dir: Path, policy: RotationPolicy | None = None) -> None:
        self._data_dir = data_dir
        self._policy = policy or RotationPolicy()
        self._buffers: dict[tuple[str, str, str], _Buffer] = {}
        self._files_written = 0
        self._rows_written = 0

    @property
    def files_written(self) -> int:
        """Number of Parquet files written since process start."""
        return self._files_written

    @property
    def rows_written(self) -> int:
        """Number of rows written since process start."""
        return self._rows_written

    @property
    def buffered_rows(self) -> int:
        """Rows waiting in memory."""
        return sum(len(buffer.rows) for buffer in self._buffers.values())

    def append_trade(self, trade: Trade) -> bool:
        """Buffer a trade. Returns ``True`` when a size threshold is crossed."""
        row: dict[str, Any] = {
            "symbol": trade.symbol,
            "trade_id": trade.trade_id,
            "price": trade.price,
            "quantity": trade.quantity,
            "quote_quantity": trade.quote_quantity,
            "event_time": trade.event_time,
            "trade_time": trade.trade_time,
            "first_trade_id": trade.first_trade_id,
            "last_trade_id": trade.last_trade_id,
            "is_buyer_maker": trade.is_buyer_maker,
            "ingested_at": trade.ingested_at,
        }
        return self._append("trades", trade.symbol, trade.event_time, row)

    def append_book(self, snapshot: BookSnapshot) -> bool:
        """Buffer a top-N snapshot in long form. Returns ``True`` on rotation."""
        urgent = False
        for side, levels in ((Side.BID, snapshot.bids), (Side.ASK, snapshot.asks)):
            for level, entry in enumerate(levels):
                row: dict[str, Any] = {
                    "symbol": snapshot.symbol,
                    "last_update_id": snapshot.last_update_id,
                    "event_time": snapshot.event_time,
                    "ingested_at": snapshot.ingested_at,
                    "side": str(side),
                    "level": level,
                    "price": entry.price,
                    "quantity": entry.quantity,
                }
                urgent = self._append("book", snapshot.symbol, snapshot.event_time, row) or urgent
        return urgent

    def _append(self, dataset: str, symbol: str, ts: datetime, row: dict[str, Any]) -> bool:
        key = (dataset, symbol, ts.astimezone(UTC).strftime("%Y-%m-%d"))
        buffer = self._buffers.get(key)
        if buffer is None:
            buffer = _Buffer()
            self._buffers[key] = buffer
        buffer.rows.append(row)
        buffer.nbytes += _estimate_row_bytes(row)
        return len(buffer.rows) >= self._policy.max_rows or buffer.nbytes >= self._policy.max_bytes

    def flush_stale(self, *, now: float | None = None) -> list[Path]:
        """Flush every buffer older than the age threshold."""
        current = time.monotonic() if now is None else now
        return self._flush(
            [
                key
                for key, buffer in self._buffers.items()
                if current - buffer.opened_at >= self._policy.max_age_s
            ]
        )

    def flush_all(self) -> list[Path]:
        """Flush every buffer, used on shutdown and in tests."""
        return self._flush(list(self._buffers))

    def _flush(self, keys: list[tuple[str, str, str]]) -> list[Path]:
        written: list[Path] = []
        for key in keys:
            buffer = self._buffers.pop(key, None)
            if buffer is None or not buffer.rows:
                continue
            dataset, symbol, date = key
            written.extend(self._write(dataset, symbol, date, buffer.rows))
        return written

    def _write(
        self, dataset: str, symbol: str, date: str, rows: list[dict[str, Any]]
    ) -> list[Path]:
        schema = TRADE_SCHEMA if dataset == "trades" else BOOK_SCHEMA
        directory = self._data_dir / dataset / f"symbol={symbol}" / f"date={date}"
        directory.mkdir(parents=True, exist_ok=True)
        paths: list[Path] = []
        for start in range(0, len(rows), self._policy.max_rows):
            chunk = rows[start : start + self._policy.max_rows]
            table = pa.Table.from_pylist(chunk, schema=schema)
            stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
            name = f"part-{stamp}-{uuid.uuid4().hex[:8]}.parquet"
            destination = directory / name
            temporary = directory / f".{name}.tmp"
            pq.write_table(table, temporary, compression="zstd", use_dictionary=True)
            os.replace(temporary, destination)
            self._files_written += 1
            self._rows_written += len(chunk)
            paths.append(destination)
            log.debug("wrote %d rows to %s", len(chunk), destination)
        return paths


def _estimate_row_bytes(row: dict[str, Any]) -> int:
    total = 48  # Arrow struct overhead approximation per row.
    for value in row.values():
        if isinstance(value, str):
            total += len(value)
        elif isinstance(value, Decimal):
            total += 32
        else:
            total += 8
    return total


def parquet_schemas() -> dict[str, Any]:
    """Expose the schemas for documentation tests."""
    return {"trades": TRADE_SCHEMA, "book": BOOK_SCHEMA}


__all__ = ["BOOK_SCHEMA", "TRADE_SCHEMA", "ParquetStore", "RotationPolicy", "parquet_schemas"]
