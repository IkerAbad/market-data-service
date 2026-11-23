"""Service container and lifecycle."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from market_data_service.aggregator import CandleAggregator
from market_data_service.binance import BinanceClient
from market_data_service.config import Settings
from market_data_service.depth import DepthBook
from market_data_service.hub import EventHub
from market_data_service.ingestion import Ingestor
from market_data_service.pipeline import Pipeline
from market_data_service.query import Analytics
from market_data_service.replay import (
    ReplayClock,
    ReplayDataset,
    ReplayRunner,
    ReplaySnapshotSource,
    ReplayTradeSource,
    load_replay_dataset,
)
from market_data_service.storage import ParquetStore, RotationPolicy

log = logging.getLogger(__name__)


class Service:
    """Holds every stateful component for one process."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.data_dir = Path(settings.data_dir)
        self._clock_offset = timedelta(seconds=settings.clock_offset_s)
        self.replay_dataset: ReplayDataset | None = None
        if settings.replay_file is not None:
            self.replay_dataset = load_replay_dataset(settings.replay_file)
            if self.replay_dataset.meta.symbol != settings.symbol:
                raise ValueError(
                    f"replay is for {self.replay_dataset.meta.symbol}, "
                    f"but MDS_SYMBOL is {settings.symbol}"
                )
        self._replay_clock = (
            ReplayClock(self.replay_dataset.meta.recorded_at)
            if self.replay_dataset is not None
            else None
        )
        self.mode = "replay" if self.replay_dataset is not None else "live"

        self.analytics = Analytics(self.data_dir)
        self.store = ParquetStore(
            self.data_dir,
            RotationPolicy(
                max_rows=settings.flush_max_rows,
                max_bytes=settings.flush_max_bytes,
                max_age_s=settings.flush_interval_s,
            ),
            clock=self.now,
        )
        self.hub = EventHub()

        self.client: BinanceClient | None = None
        if self.replay_dataset is not None:
            self.depth = DepthBook(
                settings.symbol,
                settings.depth_levels,
                ReplaySnapshotSource(self.replay_dataset.meta.depth_snapshot),
            )
        else:
            client = BinanceClient(settings.binance_rest_base)
            self.client = client
            self.depth = DepthBook(settings.symbol, settings.depth_levels, client)

        self.aggregator = CandleAggregator(settings.symbol)
        self.pipeline = Pipeline(
            settings,
            store=self.store,
            aggregator=self.aggregator,
            depth=self.depth,
            hub=self.hub,
        )

        if self.replay_dataset is not None:
            replay_clock = self._replay_clock
            assert replay_clock is not None
            self.ingestor = Ingestor(
                settings,
                source=ReplayTradeSource(),
                sink=self.pipeline,
                depth=self.depth,
                last_trade_id=None,
                clock=replay_clock,
            )
            self.replay_runner: ReplayRunner | None = ReplayRunner(
                self.ingestor,
                self.replay_dataset,
                replay_clock,
                settings.replay_speed,
            )
        else:
            assert self.client is not None
            self.ingestor = Ingestor(
                settings,
                source=self.client,
                sink=self.pipeline,
                depth=self.depth,
                last_trade_id=self.analytics.max_trade_id(settings.symbol),
                clock=self.now,
            )
            self.replay_runner = None

        self._ingest_task: asyncio.Task[None] | None = None
        self._replay_task: asyncio.Task[None] | None = None
        self._started_at: float | None = None

    def now(self) -> datetime:
        """Replay time, or host time plus the configured measurement offset."""
        if self._replay_clock is not None:
            return self._replay_clock()
        return datetime.now(UTC) + self._clock_offset

    @property
    def uptime_seconds(self) -> float:
        """Seconds since :meth:`start`."""
        return 0.0 if self._started_at is None else time.monotonic() - self._started_at

    async def start(self) -> None:
        """Seed the live cache, start background tasks and ingestion."""
        self._started_at = time.monotonic()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        for interval in self.aggregator.intervals:
            try:
                candles = self.analytics.candles(self.settings.symbol, interval, 500)
            except Exception:
                log.exception("could not seed candles for interval %ss", interval)
                continue
            self.aggregator.seed(candles)
        await self.pipeline.start()
        if self.replay_runner is not None:
            self._replay_task = asyncio.create_task(self.replay_runner.run(), name="replay")
        elif self.settings.enable_ingestion:
            self._ingest_task = asyncio.create_task(self.ingestor.run(), name="ingestor")

    async def stop(self) -> None:
        """Stop ingestion first, then flush and close resources."""
        if self._replay_task is not None:
            self._replay_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._replay_task
            self._replay_task = None
        await self.ingestor.stop()
        if self._ingest_task is not None:
            self._ingest_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._ingest_task
            self._ingest_task = None
        await self.pipeline.stop()
        if self.client is not None:
            await self.client.close()
        self.analytics.close()

    async def status(self) -> dict[str, object]:
        """Live process and storage status."""
        ingest = self.ingestor.status()
        storage = await self.analytics.storage_summary_async(self.settings.symbol)
        trades_stored = await self.analytics.trade_count_async(self.settings.symbol)
        return {
            "mode": self.mode,
            "replay": self.replay_runner.status() if self.replay_runner is not None else None,
            "ingest": ingest,
            "storage": {
                **storage,
                "buffered_rows": self.store.buffered_rows,
                "files_written": self.store.files_written,
                "rows_written": self.store.rows_written,
                "trades_stored": trades_stored,
            },
            "websocket_clients": self.hub.subscribers,
            "uptime_seconds": round(self.uptime_seconds, 3),
            "server_time": self.now(),
        }
