"""HTTP API tests over a seeded lake, without network access."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from fastapi.testclient import TestClient

from market_data_service.api import create_app
from market_data_service.config import Settings
from market_data_service.models import BookLevel, BookSnapshot
from market_data_service.storage import ParquetStore, RotationPolicy
from tests.support import T0, make_trade


def _seed(tmp_path: Path) -> None:
    store = ParquetStore(
        tmp_path, RotationPolicy(max_rows=100, max_bytes=10 * 1024 * 1024, max_age_s=3600)
    )
    store.append_trade(make_trade(1, ts=T0, price="100.00"))
    store.append_trade(make_trade(2, ts=T0.replace(second=10), price="99.50"))
    store.append_trade(make_trade(3, ts=T0.replace(second=20), price="101.00", buyer_maker=True))
    store.append_trade(make_trade(99, symbol="ETHUSDT", ts=T0, price="3000.00"))
    for update_id in (5, 6):
        store.append_book(
            BookSnapshot(
                symbol="BTCUSDT",
                last_update_id=update_id,
                event_time=T0,
                ingested_at=T0,
                bids=(BookLevel(price=Decimal("100"), quantity=Decimal("1")),),
                asks=(BookLevel(price=Decimal("101"), quantity=Decimal("2")),),
            )
        )
    store.flush_all()


def _client(tmp_path: Path, symbol: str = "BTCUSDT") -> TestClient:
    settings = Settings(
        symbol=symbol,
        data_dir=tmp_path,
        enable_ingestion=False,
        flush_interval_s=0.2,
        depth_snapshot_interval_s=5.0,
    )
    return TestClient(create_app(settings))


def test_price_endpoint_returns_latest_trade(tmp_path: Path) -> None:
    _seed(tmp_path)
    with _client(tmp_path) as client:
        response = client.get("/api/v1/price")
        assert response.status_code == 200
        body = response.json()
        assert body["trade_id"] == 3
        assert body["price"] == "101"
        assert body["side"] == "sell"
        assert body["trade_time"].endswith("Z")


def test_candles_endpoint_merges_intervals(tmp_path: Path) -> None:
    _seed(tmp_path)
    with _client(tmp_path) as client:
        minute = client.get("/api/v1/candles", params={"interval": "1m"}).json()
        assert len(minute) == 1
        assert minute[0]["open"] == "100"
        assert minute[0]["high"] == "101"
        assert minute[0]["low"] == "99.5"
        assert minute[0]["close"] == "101"
        assert minute[0]["trade_count"] == 3

        seconds = client.get("/api/v1/candles", params={"interval": "1s", "limit": 10}).json()
        assert len(seconds) == 3

        eth = client.get(
            "/api/v1/candles",
            params={"interval": "1m", "symbol": "ETHUSDT"},
        ).json()
        assert len(eth) == 1
        assert eth[0]["open"] == "3000"


def test_book_falls_back_to_parquet(tmp_path: Path) -> None:
    _seed(tmp_path)
    with _client(tmp_path) as client:
        response = client.get("/api/v1/book")
        assert response.status_code == 200
        body = response.json()
        assert body["source"] == "parquet"
        assert body["bids"][0]["price"] == "100"
        assert body["asks"][0]["quantity"] == "2"


def test_status_and_metrics(tmp_path: Path) -> None:
    _seed(tmp_path)
    with _client(tmp_path) as client:
        status = client.get("/api/v1/status").json()
        assert status["ingest"]["symbol"] == "BTCUSDT"
        assert status["storage"]["trades_stored"] == 3
        assert status["storage"]["trades_files"] == 1
        assert status["ingest"]["trades_backfilled"] == 0

        metrics = client.get("/metrics")
        assert metrics.status_code == 200
        assert "mds_ws_connected" in metrics.text
        assert "mds_ingest_lag_seconds" in metrics.text
        assert "mds_write_lag_seconds" in metrics.text


def test_health_and_dashboard(tmp_path: Path) -> None:
    _seed(tmp_path)
    with _client(tmp_path) as client:
        health = client.get("/api/v1/health").json()
        assert health["status"] == "ok"
        assert health["connected"] is False
        page = client.get("/")
        assert page.status_code == 200
        assert "Market Data Service" in page.text
        assert client.get("/static/app.js").status_code == 200


def test_websocket_handshake(tmp_path: Path) -> None:
    _seed(tmp_path)
    with _client(tmp_path) as client, client.websocket_connect("/ws") as websocket:
        hello = websocket.receive_json()
        assert hello["type"] == "hello"
        assert hello["symbol"] == "BTCUSDT"
        assert hello["intervals"] == [1, 5, 60]


def test_unknown_symbol_and_invalid_interval(tmp_path: Path) -> None:
    _seed(tmp_path)
    with _client(tmp_path) as client:
        assert client.get("/api/v1/price", params={"symbol": "SOLUSDT"}).status_code == 404
        assert client.get("/api/v1/candles", params={"interval": "7s"}).status_code == 422
