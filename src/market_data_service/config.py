"""Runtime configuration.

Every value has a default so the service starts with no arguments. Environment
variables use the ``MDS_`` prefix, for example ``MDS_SYMBOL=ETHUSDT``.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

_SYMBOL_RE = re.compile(r"^[A-Z0-9]{5,20}$")
_ENV_PREFIX = "MDS_"


def _raw(name: str) -> str | None:
    value = os.environ.get(_ENV_PREFIX + name)
    return value if value else None


def _env_str(name: str, default: str) -> str:
    value = _raw(name)
    return default if value is None else value


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    value = _raw(name)
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{_ENV_PREFIX}{name} must be an integer, got {value!r}") from exc
    if parsed < minimum:
        raise ValueError(f"{_ENV_PREFIX}{name} must be >= {minimum}, got {parsed}")
    return parsed


def _env_float(name: str, default: float, *, minimum: float = 0.0) -> float:
    value = _raw(name)
    if value is None:
        return default
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(f"{_ENV_PREFIX}{name} must be a number, got {value!r}") from exc
    if parsed < minimum:
        raise ValueError(f"{_ENV_PREFIX}{name} must be >= {minimum}, got {parsed}")
    return parsed


def _env_bool(name: str, default: bool) -> bool:
    value = _raw(name)
    if value is None:
        return default
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "on"}:
        return True
    if lowered in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{_ENV_PREFIX}{name} must be a boolean, got {value!r}")


@dataclass(frozen=True, slots=True)
class Settings:
    """Frozen runtime configuration."""

    symbol: str = "BTCUSDT"
    data_dir: Path = Path("data")

    binance_ws_base: str = "wss://stream.binance.com:9443"
    binance_rest_base: str = "https://api.binance.com"

    # Ingestion and recovery.
    depth_levels: int = 20
    depth_snapshot_interval_s: float = 1.0
    reconnect_base_s: float = 0.5
    reconnect_max_s: float = 30.0
    reconnect_stable_after_s: float = 30.0
    max_backfill_trades: int = 50_000
    backfill_page_size: int = 1_000
    pending_buffer_size: int = 100_000

    # Parquet writer.
    flush_interval_s: float = 5.0
    flush_max_rows: int = 50_000
    flush_max_bytes: int = 64 * 1024 * 1024
    queue_max_records: int = 200_000

    # HTTP server.
    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "INFO"
    enable_ingestion: bool = True

    @property
    def stream_names(self) -> str:
        """Combined-stream names for the configured symbol."""
        symbol = self.symbol.lower()
        return f"{symbol}@aggTrade/{symbol}@depth@100ms"

    @property
    def websocket_url(self) -> str:
        """Full combined-stream URL."""
        return f"{self.binance_ws_base}/stream?streams={self.stream_names}"

    @classmethod
    def from_env(cls) -> Settings:
        """Build settings from ``MDS_*`` environment variables."""
        symbol = _env_str("SYMBOL", "BTCUSDT").strip().upper()
        if not _SYMBOL_RE.match(symbol):
            raise ValueError(f"invalid symbol {symbol!r}; expected e.g. BTCUSDT")
        return cls(
            symbol=symbol,
            data_dir=Path(_env_str("DATA_DIR", "data")),
            binance_ws_base=_env_str("BINANCE_WS_BASE", "wss://stream.binance.com:9443"),
            binance_rest_base=_env_str("BINANCE_REST_BASE", "https://api.binance.com"),
            depth_levels=_env_int("DEPTH_LEVELS", 20),
            depth_snapshot_interval_s=_env_float("DEPTH_SNAPSHOT_INTERVAL_S", 1.0, minimum=0.1),
            reconnect_base_s=_env_float("RECONNECT_BASE_S", 0.5, minimum=0.01),
            reconnect_max_s=_env_float("RECONNECT_MAX_S", 30.0, minimum=0.1),
            reconnect_stable_after_s=_env_float("RECONNECT_STABLE_AFTER_S", 30.0),
            max_backfill_trades=_env_int("MAX_BACKFILL_TRADES", 50_000),
            backfill_page_size=_env_int("BACKFILL_PAGE_SIZE", 1_000),
            pending_buffer_size=_env_int("PENDING_BUFFER_SIZE", 100_000),
            flush_interval_s=_env_float("FLUSH_INTERVAL_S", 5.0, minimum=0.1),
            flush_max_rows=_env_int("FLUSH_MAX_ROWS", 50_000),
            flush_max_bytes=_env_int("FLUSH_MAX_BYTES", 64 * 1024 * 1024),
            queue_max_records=_env_int("QUEUE_MAX_RECORDS", 200_000),
            host=_env_str("HOST", "0.0.0.0"),
            port=_env_int("PORT", 8000),
            log_level=_env_str("LOG_LEVEL", "INFO").upper(),
            enable_ingestion=_env_bool("ENABLE_INGESTION", True),
        )
