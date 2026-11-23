"""End-to-end recovery test.

A fake exchange replays 45 aggregate trades, drops the WebSocket for exactly five
seconds and resumes at trade 26. Trades 21-25 are only available through the REST
endpoint, so the test proves that a five-second network cut leaves no gap in the
Parquet lake.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
from collections.abc import Callable
from pathlib import Path
from typing import Any

import duckdb
import pytest

from market_data_service.aggregator import CandleAggregator
from market_data_service.binance import BinanceClient
from market_data_service.config import Settings
from market_data_service.depth import DepthBook
from market_data_service.hub import EventHub
from market_data_service.ingestion import Ingestor
from market_data_service.pipeline import Pipeline
from market_data_service.storage import ParquetStore, RotationPolicy
from tests.fake_exchange import FakeExchange

TOTAL_TRADES = 45
DOWNTIME_S = 5.0


async def _wait_for(
    ingestor: Ingestor, predicate: Callable[[], bool], timeout: float = 30.0
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError(f"timed out with last trade id {ingestor.last_trade_id}")
        await asyncio.sleep(0.05)


def _stored_ids(tmp_path: Path) -> list[int]:
    glob = (tmp_path / "trades" / "symbol=BTCUSDT" / "date=*" / "*.parquet").as_posix()
    rows = (
        duckdb.connect()
        .execute(
            "SELECT trade_id FROM read_parquet(?) ORDER BY trade_id",
            [glob],
        )
        .fetchall()
    )
    return [int(row[0]) for row in rows]


@pytest.mark.integration
async def test_five_second_cut_is_backfilled_without_gaps(tmp_path: Path) -> None:
    exchange = FakeExchange(
        first_batch=20,
        resume_at=26,
        downtime=DOWNTIME_S,
    )
    exchange.start()
    assert exchange.total == TOTAL_TRADES
    settings = Settings(
        symbol="BTCUSDT",
        data_dir=tmp_path,
        binance_ws_base=exchange.ws_base,
        binance_rest_base=exchange.http_base,
        enable_ingestion=False,
        reconnect_base_s=0.05,
        reconnect_max_s=0.3,
        reconnect_stable_after_s=1.0,
        flush_interval_s=0.2,
        flush_max_rows=10_000,
        depth_snapshot_interval_s=5.0,
    )
    store = ParquetStore(
        tmp_path,
        RotationPolicy(max_rows=10_000, max_bytes=10 * 1024 * 1024, max_age_s=0.2),
    )
    client = BinanceClient(settings.binance_rest_base)
    hub = EventHub()
    depth = DepthBook(settings.symbol, settings.depth_levels, client)
    aggregator = CandleAggregator(settings.symbol)
    pipeline = Pipeline(settings, store=store, aggregator=aggregator, depth=depth, hub=hub)
    ingestor = Ingestor(
        settings,
        source=client,
        sink=pipeline,
        depth=depth,
        rng=random.Random(7),
    )
    await pipeline.start()
    task = asyncio.create_task(ingestor.run())
    status: dict[str, Any] = {}
    try:
        await _wait_for(ingestor, lambda: ingestor.last_trade_id == TOTAL_TRADES)
        status = ingestor.status()
    finally:
        await ingestor.stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await pipeline.stop()
        await client.close()
        exchange.stop()

    # The outage really lasted five seconds.
    assert exchange.cut_at is not None
    assert exchange.resume_time is not None
    measured = exchange.resume_time - exchange.cut_at
    assert measured >= DOWNTIME_S - 0.05, f"outage was only {measured:.2f}s"

    # Gap accounting is explicit.
    assert status["gaps_detected"] >= 1
    assert status["trades_backfilled"] == 5
    assert status["trades_missing"] == 0
    assert status["last_trade_id"] == TOTAL_TRADES

    # The lake is contiguous: 1..45, no holes and no duplicates.
    ids = _stored_ids(tmp_path)
    assert ids == list(range(1, TOTAL_TRADES + 1))
    assert len(ids) == len(set(ids))

    # At least one Parquet file exists in the symbol/date layout.
    files = list((tmp_path / "trades" / "symbol=BTCUSDT").glob("date=*/*.parquet"))
    assert files
    assert store.rows_written == TOTAL_TRADES
