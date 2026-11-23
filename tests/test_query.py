"""DuckDB analytics tests against a small partitioned lake."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from market_data_service.models import BookLevel, BookSnapshot
from market_data_service.query import Analytics
from market_data_service.storage import ParquetStore, RotationPolicy
from tests.support import T0, make_trade


def _seed(tmp_path: Path) -> None:
    store = ParquetStore(
        tmp_path, RotationPolicy(max_rows=10, max_bytes=10 * 1024 * 1024, max_age_s=3600)
    )
    for index, seconds in enumerate((0, 10, 20, 70), start=1):
        store.append_trade(
            make_trade(index, ts=T0 + timedelta(seconds=seconds), price=str(100 + index))
        )
    store.append_trade(make_trade(99, symbol="ETHUSDT", ts=T0, price="3000"))
    snapshot = BookSnapshot(
        symbol="BTCUSDT",
        last_update_id=7,
        event_time=T0,
        ingested_at=T0,
        bids=(BookLevel(price=Decimal("99"), quantity=Decimal("1")),),
        asks=(BookLevel(price=Decimal("101"), quantity=Decimal("2")),),
    )
    store.append_book(snapshot)
    store.flush_all()


def test_latest_trade_and_ids(tmp_path: Path) -> None:
    _seed(tmp_path)
    analytics = Analytics(tmp_path)
    try:
        latest = analytics.latest_trade("BTCUSDT")
        assert latest is not None
        assert latest["trade_id"] == 4
        assert latest["price"] == Decimal("104")
        assert analytics.max_trade_id("BTCUSDT") == 4
        assert analytics.trade_count("BTCUSDT") == 4
        assert analytics.max_trade_id("NOPEUSDT") is None
        assert analytics.trade_count("NOPEUSDT") == 0
    finally:
        analytics.close()


def test_candles_are_aggregated_from_parquet(tmp_path: Path) -> None:
    _seed(tmp_path)
    analytics = Analytics(tmp_path)
    try:
        hourly = analytics.candles("BTCUSDT", 60, 10)
        assert len(hourly) == 2
        assert hourly[0].open == Decimal("101")
        assert hourly[0].close == Decimal("103")
        assert hourly[0].trade_count == 3
        assert hourly[1].open_time == T0 + timedelta(minutes=1)

        ten_second = analytics.candles("BTCUSDT", 10, 10)
        assert len(ten_second) == 4
        assert ten_second[-1].open_time == T0 + timedelta(seconds=70)
        assert ten_second[-1].close == Decimal("104")
    finally:
        analytics.close()


def test_latest_book_and_storage_summary(tmp_path: Path) -> None:
    _seed(tmp_path)
    analytics = Analytics(tmp_path)
    try:
        book = analytics.latest_book("BTCUSDT")
        assert book is not None
        assert book["bids"] == [{"price": "99.000000000000", "quantity": "1.000000000000"}]
        assert book["asks"][0]["price"].startswith("101")
        summary = analytics.storage_summary("BTCUSDT")
        assert summary["trades_files"] == 1
        assert summary["book_files"] == 1
        assert summary["trades_bytes"] > 0
    finally:
        analytics.close()
