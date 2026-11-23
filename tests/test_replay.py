"""Offline replay tests. No Binance connection is used."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, cast

from fastapi.testclient import TestClient

from market_data_service.api import create_app
from market_data_service.config import Settings
from market_data_service.replay import load_replay_dataset
from market_data_service.service import Service

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "btcusdt-5m.jsonl.gz"


def test_example_recording_covers_five_minutes() -> None:
    assert EXAMPLE.is_file(), f"missing replay recording: {EXAMPLE}"
    assert EXAMPLE.stat().st_size < 5 * 1024 * 1024
    dataset = load_replay_dataset(EXAMPLE)
    assert dataset.meta.symbol == "BTCUSDT"
    assert dataset.duration_seconds >= 300
    assert len(dataset.frames) > 1_000


def test_replay_dashboard_and_api_without_network(tmp_path: Path) -> None:
    settings = Settings(
        symbol="BTCUSDT",
        data_dir=tmp_path,
        replay_file=EXAMPLE,
        replay_speed=500.0,
        enable_ingestion=False,
        flush_interval_s=0.2,
        depth_snapshot_interval_s=5.0,
    )
    app = create_app(settings)
    service = cast(Service, app.state.service)
    assert service.client is None

    with TestClient(app) as client:
        deadline = time.monotonic() + 20
        status: dict[str, Any] = {}
        while time.monotonic() < deadline:
            status = client.get("/api/v1/status").json()
            if status["replay"]["finished"] and status["ingest"]["book_ready"]:
                break
            time.sleep(0.1)

        assert status["mode"] == "replay"
        assert status["replay"]["finished"] is True
        assert status["replay"]["position"] == status["replay"]["frames"]
        assert status["replay"]["error"] is None
        assert status["ingest"]["last_trade_id"] is not None
        assert status["ingest"]["book_ready"] is True

        price = client.get("/api/v1/price")
        assert price.status_code == 200
        assert float(price.json()["price"]) > 0

        candles = client.get("/api/v1/candles", params={"interval": "1m", "limit": 50})
        assert candles.status_code == 200
        assert len(candles.json()) >= 2

        book = client.get("/api/v1/book")
        assert book.status_code == 200
        assert book.json()["bids"]
        assert book.json()["asks"]

        dashboard = client.get("/")
        assert dashboard.status_code == 200
        assert "Market Data Service" in dashboard.text
        script = client.get("/static/app.js")
        assert script.status_code == 200
        assert 'message.mode === "replay"' in script.text

        with client.websocket_connect("/ws") as websocket:
            hello = websocket.receive_json()
            assert hello["type"] == "hello"
            assert hello["mode"] == "replay"
