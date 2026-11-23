"""Storage tests: partitioning, schema, rotation and atomic files."""

from __future__ import annotations

import time
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from market_data_service.models import BookLevel, BookSnapshot
from market_data_service.storage import ParquetStore, RotationPolicy
from tests.support import T0, make_trade


def _store(tmp_path: Path, *, max_rows: int = 100, max_age_s: float = 3600.0) -> ParquetStore:
    return ParquetStore(
        tmp_path,
        RotationPolicy(max_rows=max_rows, max_bytes=10 * 1024 * 1024, max_age_s=max_age_s),
    )


def test_partition_layout_and_schema(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.append_trade(make_trade(1, price="61264.83", quantity="0.001"))
    paths = store.flush_all()
    assert len(paths) == 1

    path = paths[0]
    assert path.parts[-4] == "trades"
    assert path.parts[-3] == "symbol=BTCUSDT"
    assert path.parts[-2] == "date=2026-11-23"
    assert path.name.startswith("part-")
    assert path.suffix == ".parquet"

    table = pq.ParquetFile(path).read()
    assert table.schema.field("price").type == pa.decimal128(38, 12)
    assert table.schema.field("trade_time").type == pa.timestamp("us", tz="UTC")
    assert table.schema.metadata is not None
    assert table.schema.metadata[b"schema"] == b"market-data-service/trades/v1"
    assert table.column("price")[0].as_py() == Decimal("61264.83")


def test_rotation_by_row_count_creates_multiple_files(tmp_path: Path) -> None:
    store = _store(tmp_path, max_rows=2)
    for trade_id in range(1, 6):
        store.append_trade(make_trade(trade_id, ts=T0))
    paths = store.flush_all()
    assert len(paths) == 3

    directory = tmp_path / "trades" / "symbol=BTCUSDT" / "date=2026-11-23"
    files = sorted(directory.glob("part-*.parquet"))
    assert len(files) == 3
    rows = sum(pq.ParquetFile(path).read().num_rows for path in files)
    assert rows == 5
    assert not list(directory.glob("*.tmp"))
    assert store.buffered_rows == 0


def test_rotation_by_age(tmp_path: Path) -> None:
    store = _store(tmp_path, max_age_s=0.01)
    store.append_trade(make_trade(1))
    assert store.flush_stale() == []
    time.sleep(0.02)
    paths = store.flush_stale()
    assert len(paths) == 1
    assert store.rows_written == 1


def test_rotation_by_estimated_bytes(tmp_path: Path) -> None:
    store = ParquetStore(
        tmp_path,
        RotationPolicy(max_rows=100_000, max_bytes=1, max_age_s=3_600),
    )
    store.append_trade(make_trade(1))
    paths = store.flush_stale()
    assert len(paths) == 1
    assert store.rows_written == 1


def test_book_is_stored_in_long_form(tmp_path: Path) -> None:
    store = _store(tmp_path)
    snapshot = BookSnapshot(
        symbol="BTCUSDT",
        last_update_id=10,
        event_time=T0,
        ingested_at=T0,
        bids=(BookLevel(price=Decimal("100"), quantity=Decimal("1")),),
        asks=(
            BookLevel(price=Decimal("101"), quantity=Decimal("2")),
            BookLevel(price=Decimal("102"), quantity=Decimal("3")),
        ),
    )
    store.append_book(snapshot)
    paths = store.flush_all()
    assert len(paths) == 1

    table = pq.ParquetFile(paths[0]).read()
    assert table.num_rows == 3
    assert set(table.column("side").to_pylist()) == {"bid", "ask"}
    assert table.schema.field("price").type == pa.decimal128(38, 12)


def test_append_reports_size_threshold(tmp_path: Path) -> None:
    store = _store(tmp_path, max_rows=1)
    assert store.append_trade(make_trade(1)) is True
    assert store.buffered_rows == 1


def test_rotation_policy_validates_thresholds() -> None:
    with pytest.raises(ValueError):
        RotationPolicy(max_rows=0)
