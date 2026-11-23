"""Binance WebSocket ingestion with gap detection and REST backfill.

The trade stream is ordered by aggregate trade id. A disconnect is not the only
source of gaps: frames can be dropped while the socket stays open. Both cases
are handled the same way.

1. Track the last accepted trade id.
2. When an id arrives that is not ``last + 1``, report a gap, buffer live
   trades and fetch ``[last + 1, first_buffered)`` from ``GET /api/v3/aggTrades``.
3. Emit recovered trades in id order, then drain the live buffer.
4. If REST cannot fill an id, count it as missing and continue: the service
   reports the hole instead of hiding it.

The WebSocket loop reconnects with exponential backoff and equal jitter.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import time
from collections import deque
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Protocol

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from market_data_service import metrics
from market_data_service.binance import BinanceError
from market_data_service.config import Settings
from market_data_service.depth import DepthBook
from market_data_service.models import Trade
from market_data_service.parser import ParseError, parse_stream_message

log = logging.getLogger(__name__)

MAX_BACKOFF_ATTEMPTS = 20


class TradeSource(Protocol):
    """REST operations needed by the ingestor."""

    async def agg_trades(self, symbol: str, from_id: int, limit: int) -> list[Trade]:
        """Return aggregate trades starting at ``from_id``."""
        ...


class TradeSink(Protocol):
    """Downstream consumer of accepted trades."""

    async def on_trade(self, trade: Trade) -> None:
        """Handle one trade, in ascending ``trade_id`` order."""
        ...


def backoff_delay(
    attempt: int,
    *,
    base: float,
    cap: float,
    rng: random.Random | None = None,
) -> float:
    """Exponential backoff with equal jitter.

    ``delay = min(cap, base * 2**attempt) / 2 + U(0, delay / 2)``.
    """
    generator = rng or random.Random()
    window: float = min(cap, base * float(2 ** min(attempt, MAX_BACKOFF_ATTEMPTS)))
    return window / 2 + float(generator.uniform(0, window / 2))


class Ingestor:
    """Owns the WebSocket connection and the trade id sequence."""

    def __init__(
        self,
        settings: Settings,
        *,
        source: TradeSource,
        sink: TradeSink,
        depth: DepthBook,
        last_trade_id: int | None = None,
        clock: Callable[[], datetime] | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self._settings = settings
        self._source = source
        self._sink = sink
        self._depth = depth
        self._last_trade_id = last_trade_id
        self._clock = clock or (lambda: datetime.now(UTC))
        self._rng = rng or random.Random()

        self._connected = False
        self._reconnects = 0
        self._backfilling = False
        self._backfill_task: asyncio.Task[None] | None = None
        self._pending: deque[Trade] = deque()
        self._last_event_time: datetime | None = None
        self._gaps_detected = 0
        self._missing_trades = 0
        self._trades_backfilled = 0
        self._stop = asyncio.Event()

    @property
    def connected(self) -> bool:
        """Whether the combined stream is open."""
        return self._connected

    @property
    def last_trade_id(self) -> int | None:
        """Last accepted aggregate trade id."""
        return self._last_trade_id

    async def stop(self) -> None:
        """Signal the loop to stop and wait for the backfill task."""
        self._stop.set()
        if self._backfill_task is not None and not self._backfill_task.done():
            self._backfill_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._backfill_task

    async def run(self) -> None:
        """Connect, ingest and reconnect until :meth:`stop` is called."""
        attempt = 0
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                await self._consume()
            except asyncio.CancelledError:
                raise
            except (WebSocketException, OSError) as exc:
                log.warning("websocket closed: %s", exc)
            except Exception:
                log.exception("unexpected ingestion error")
            finally:
                self._connected = False
                metrics.WS_CONNECTED.set(0)
                self._depth.reset()
            if self._stop.is_set():
                break
            if time.monotonic() - started >= self._settings.reconnect_stable_after_s:
                attempt = 0
            delay = backoff_delay(
                attempt,
                base=self._settings.reconnect_base_s,
                cap=self._settings.reconnect_max_s,
                rng=self._rng,
            )
            attempt += 1
            self._reconnects += 1
            metrics.WS_RECONNECTS.inc()
            log.info("reconnecting in %.2f s (attempt %d)", delay, attempt)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            except TimeoutError:
                continue

    async def _consume(self) -> None:
        url = self._settings.websocket_url
        log.info("connecting to %s", url)
        async with connect(
            url,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_queue=1_024,
        ) as socket:
            self._connected = True
            metrics.WS_CONNECTED.set(1)
            self._depth.reset()
            log.info("websocket connected")
            async for raw in socket:
                if self._stop.is_set():
                    return
                await self.handle_frame(raw)

    async def handle_frame(self, raw: str | bytes) -> None:
        """Parse and dispatch one raw combined-stream frame.

        Public because the offline replay runner feeds the same path.
        """
        try:
            event = parse_stream_message(raw, ingested_at=self._clock())
        except ParseError as exc:
            log.warning("discarding malformed frame: %s", exc)
            return
        if event is None:
            return
        if isinstance(event, Trade):
            metrics.WS_MESSAGES.labels("trade").inc()
            await self._on_trade(event)
        else:
            metrics.WS_MESSAGES.labels("depth").inc()
            self._depth.on_update(event)

    async def _on_trade(self, trade: Trade) -> None:
        lag = (trade.ingested_at - trade.event_time).total_seconds()
        metrics.MESSAGE_LATENCY.observe(max(0.0, lag))
        metrics.INGEST_LAG.set(lag)
        if self._last_event_time is None or trade.event_time > self._last_event_time:
            self._last_event_time = trade.event_time

        if self._backfilling:
            self._buffer_live(trade)
            return
        if self._last_trade_id is None:
            self._last_trade_id = trade.trade_id - 1
        if trade.trade_id <= self._last_trade_id:
            return
        if trade.trade_id == self._last_trade_id + 1:
            await self._accept(trade)
            return

        self._gaps_detected += 1
        metrics.GAPS_DETECTED.inc()
        log.info(
            "trade gap: expected %d, got %d",
            self._last_trade_id + 1,
            trade.trade_id,
        )
        self._buffer_live(trade)
        self._backfilling = True
        self._backfill_task = asyncio.create_task(self._run_backfill())

    def _buffer_live(self, trade: Trade) -> None:
        self._pending.append(trade)
        if len(self._pending) > self._settings.pending_buffer_size:
            dropped = self._pending.popleft()
            log.warning("pending buffer full; dropping trade %d", dropped.trade_id)

    async def _run_backfill(self) -> None:
        pages = 0
        max_pages = max(1, self._settings.max_backfill_trades // self._settings.backfill_page_size)
        try:
            while self._pending:
                target = self._pending[0].trade_id
                last = self._last_trade_id
                if last is None or last + 1 >= target:
                    break
                if pages >= max_pages:
                    log.warning("backfill budget exhausted at trade %d", last)
                    break
                batch = await self._source.agg_trades(
                    self._settings.symbol,
                    last + 1,
                    self._settings.backfill_page_size,
                )
                pages += 1
                progressed = False
                for trade in batch:
                    if self._last_trade_id is None or trade.trade_id <= self._last_trade_id:
                        continue
                    if trade.trade_id != self._last_trade_id + 1:
                        break
                    await self._accept(trade, backfilled=True)
                    progressed = True
                    if self._last_trade_id + 1 >= target:
                        break
                if not progressed:
                    break
            await self._drain_pending()
        except asyncio.CancelledError:
            raise
        except (BinanceError, OSError) as exc:
            log.warning("backfill failed: %s", exc)
            await self._drain_pending()
        except Exception:
            log.exception("unexpected backfill failure")
            await self._drain_pending()
        finally:
            self._backfilling = False

    async def _drain_pending(self) -> None:
        while self._pending:
            batch = sorted(self._pending, key=lambda trade: trade.trade_id)
            self._pending.clear()
            for trade in batch:
                if self._last_trade_id is None:
                    self._last_trade_id = trade.trade_id - 1
                if trade.trade_id <= self._last_trade_id:
                    continue
                if trade.trade_id != self._last_trade_id + 1:
                    missing = trade.trade_id - self._last_trade_id - 1
                    self._missing_trades += missing
                    metrics.TRADES_MISSING.inc(missing)
                    log.warning(
                        "unrecovered trade ids %d..%d", self._last_trade_id + 1, trade.trade_id - 1
                    )
                await self._accept(trade)

    async def _accept(self, trade: Trade, *, backfilled: bool = False) -> None:
        self._last_trade_id = trade.trade_id
        metrics.TRADES_INGESTED.inc()
        if backfilled:
            self._trades_backfilled += 1
            metrics.TRADES_BACKFILLED.inc()
        await self._sink.on_trade(trade)

    def status(self) -> dict[str, Any]:
        """Ingestion state for the status endpoint."""
        now = self._clock()
        lag: float | None = None
        if self._last_event_time is not None:
            lag = (now - self._last_event_time).total_seconds()
        return {
            "connected": self._connected,
            "symbol": self._settings.symbol,
            "last_trade_id": self._last_trade_id,
            "last_trade_time": self._last_event_time,
            "lag_seconds": lag,
            "reconnects": self._reconnects,
            "gaps_detected": self._gaps_detected,
            "trades_backfilled": self._trades_backfilled,
            "trades_missing": self._missing_trades,
            "backfilling": self._backfilling,
            "pending_trades": len(self._pending),
            "book_ready": self._depth.ready,
            "book_update_id": self._depth.last_update_id,
        }
