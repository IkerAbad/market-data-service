"""Prometheus metrics.

Every metric uses the ``mds_`` prefix. Counters are cumulative for the process
lifetime, which is what Prometheus expects for ``rate()`` queries.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

WS_MESSAGES = Counter(
    "mds_ws_messages_total",
    "WebSocket frames accepted, by stream.",
    ["stream"],
)
WS_RECONNECTS = Counter(
    "mds_ws_reconnects_total",
    "WebSocket connection attempts after a failure.",
)
WS_CONNECTED = Gauge(
    "mds_ws_connected",
    "1 when the combined stream is connected.",
)
INGEST_LAG = Gauge(
    "mds_ingest_lag_seconds",
    "Wall clock minus event time of the last trade.",
)
TRADES_INGESTED = Counter(
    "mds_trades_ingested_total",
    "Aggregate trades accepted into the pipeline.",
)
TRADES_BACKFILLED = Counter(
    "mds_trades_backfilled_total",
    "Trades recovered through REST backfill.",
)
TRADES_MISSING = Counter(
    "mds_trades_missing_total",
    "Trade ids that could not be recovered.",
)
GAPS_DETECTED = Counter(
    "mds_gaps_detected_total",
    "Discontinuities found in the trade id sequence.",
)
DEPTH_RESYNCS = Counter(
    "mds_depth_resyncs_total",
    "Order book rebuilds triggered by a snapshot or update id mismatch.",
)
DEPTH_READY = Gauge(
    "mds_depth_ready",
    "1 when the local order book is synchronized.",
)
DEPTH_BUFFERED = Gauge(
    "mds_depth_buffered_updates",
    "Depth updates waiting for the current snapshot.",
)
BOOK_SNAPSHOTS = Counter(
    "mds_book_snapshots_total",
    "Order book snapshots persisted.",
)
PARQUET_FILES = Gauge(
    "mds_parquet_files_written",
    "Parquet files written since process start.",
)
PARQUET_ROWS = Gauge(
    "mds_parquet_rows_written",
    "Rows written to Parquet since process start.",
)
WRITER_BUFFERED_ROWS = Gauge(
    "mds_writer_buffered_rows",
    "Rows waiting in the writer buffer.",
)
MESSAGE_LATENCY = Histogram(
    "mds_message_latency_seconds",
    "Time between the exchange timestamp and local ingestion.",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60),
)


def refresh_writer_gauges(buffered_rows: int) -> None:
    """Update writer gauges from the store state."""
    WRITER_BUFFERED_ROWS.set(buffered_rows)
