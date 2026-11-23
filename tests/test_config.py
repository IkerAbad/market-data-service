"""Configuration tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from market_data_service.config import Settings


def test_defaults_are_runnable() -> None:
    settings = Settings()
    assert settings.symbol == "BTCUSDT"
    assert settings.stream_names == "btcusdt@aggTrade/btcusdt@depth@100ms"
    assert settings.websocket_url.endswith("/stream?streams=btcusdt@aggTrade/btcusdt@depth@100ms")


def test_environment_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MDS_SYMBOL", "ethusdt")
    monkeypatch.setenv("MDS_DATA_DIR", str(Path("lake")))
    monkeypatch.setenv("MDS_PORT", "9000")
    monkeypatch.setenv("MDS_ENABLE_INGESTION", "false")
    monkeypatch.setenv("MDS_CLOCK_OFFSET_S", "1.25")
    settings = Settings.from_env()
    assert settings.symbol == "ETHUSDT"
    assert settings.data_dir == Path("lake")
    assert settings.port == 9000
    assert settings.enable_ingestion is False
    assert settings.clock_offset_s == 1.25


def test_invalid_symbol_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MDS_SYMBOL", "not a symbol")
    with pytest.raises(ValueError, match="invalid symbol"):
        Settings.from_env()


def test_invalid_boolean_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MDS_ENABLE_INGESTION", "maybe")
    with pytest.raises(ValueError, match="boolean"):
        Settings.from_env()
