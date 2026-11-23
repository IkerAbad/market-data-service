"""Local order book maintained from diff depth updates plus REST snapshots.

Binance publishes order book deltas, not full books, on the ``depth`` stream.
The documented recovery procedure implemented here is:

1. Buffer updates while a REST snapshot is requested.
2. Drop updates older than the snapshot.
3. Apply the first update that satisfies ``U <= lastUpdateId + 1 <= u``.
4. Require every following update to satisfy ``U == previous_u + 1``.
5. On any discontinuity, discard the book and request a fresh snapshot.

Every rebuild is counted in ``mds_depth_resyncs_total``.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal, Protocol

from market_data_service import metrics
from market_data_service.models import BookLevel, BookSnapshot, DepthUpdate, Side

log = logging.getLogger(__name__)

MAX_BUFFERED_UPDATES = 10_000
MAX_SYNC_ATTEMPTS = 4
MAX_BOOK_LEVELS = 5_000

State = Literal["idle", "syncing", "ready"]


class DepthSnapshotSource(Protocol):
    """Minimal REST interface required by :class:`DepthBook`."""

    async def depth_snapshot(self, symbol: str, limit: int) -> BookSnapshot:
        """Return a top-N snapshot anchored at ``lastUpdateId``."""
        ...


class DepthBook:
    """Maintains the local book and exposes sampled snapshots."""

    def __init__(
        self,
        symbol: str,
        levels: int,
        source: DepthSnapshotSource,
    ) -> None:
        self._symbol = symbol
        self._levels = levels
        self._source = source
        self._state: State = "idle"
        self._bids: dict[Decimal, Decimal] = {}
        self._asks: dict[Decimal, Decimal] = {}
        self._last_update_id: int | None = None
        self._last_event_time: datetime | None = None
        self._buffer: list[DepthUpdate] = []
        self._sync_task: asyncio.Task[None] | None = None

    @property
    def symbol(self) -> str:
        """Configured symbol."""
        return self._symbol

    @property
    def ready(self) -> bool:
        """Whether the book is synchronized with the exchange."""
        return self._state == "ready"

    @property
    def last_update_id(self) -> int | None:
        """Last applied update id."""
        return self._last_update_id

    def reset(self) -> None:
        """Discard state, for example after a WebSocket disconnect."""
        if self._sync_task is not None and not self._sync_task.done():
            self._sync_task.cancel()
        self._sync_task = None
        self._state = "idle"
        self._bids.clear()
        self._asks.clear()
        self._last_update_id = None
        self._last_event_time = None
        self._buffer.clear()
        self._publish_state()

    def on_update(self, update: DepthUpdate) -> None:
        """Feed one diff update. Never blocks."""
        if self._state == "idle":
            self._buffer.append(update)
            self._start_sync()
            return
        if self._state == "syncing":
            self._buffer.append(update)
            if len(self._buffer) > MAX_BUFFERED_UPDATES:
                del self._buffer[: len(self._buffer) - MAX_BUFFERED_UPDATES]
            metrics.DEPTH_BUFFERED.set(len(self._buffer))
            return
        if self._last_update_id is None:
            self._start_sync()
            return
        if update.last_update_id <= self._last_update_id:
            return
        if update.first_update_id != self._last_update_id + 1:
            metrics.DEPTH_RESYNCS.inc()
            log.info(
                "depth gap: expected U=%d, got U=%d; resyncing",
                self._last_update_id + 1,
                update.first_update_id,
            )
            self._start_sync()
            self._buffer.append(update)
            return
        self._apply(update)

    def snapshot(self, levels: int | None = None) -> BookSnapshot | None:
        """Sample the top of the book, or ``None`` when not synchronized."""
        if self._state != "ready" or self._last_update_id is None:
            return None
        count = self._levels if levels is None else max(1, levels)
        now = datetime.now(UTC)
        return BookSnapshot(
            symbol=self._symbol,
            last_update_id=self._last_update_id,
            event_time=self._last_event_time or now,
            ingested_at=now,
            bids=tuple(
                BookLevel(price=price, quantity=self._bids[price])
                for price in sorted(self._bids, reverse=True)[:count]
            ),
            asks=tuple(
                BookLevel(price=price, quantity=self._asks[price])
                for price in sorted(self._asks)[:count]
            ),
        )

    def _publish_state(self) -> None:
        metrics.DEPTH_READY.set(1 if self._state == "ready" else 0)
        metrics.DEPTH_BUFFERED.set(len(self._buffer))

    def _start_sync(self) -> None:
        if self._sync_task is not None and not self._sync_task.done():
            return
        self._state = "syncing"
        self._bids.clear()
        self._asks.clear()
        self._last_update_id = None
        metrics.DEPTH_RESYNCS.inc()
        self._publish_state()
        self._sync_task = asyncio.create_task(self._sync())

    async def _sync(self) -> None:
        try:
            for _attempt in range(MAX_SYNC_ATTEMPTS):
                snapshot = await self._source.depth_snapshot(self._symbol, MAX_BOOK_LEVELS)
                bids = {level.price: level.quantity for level in snapshot.bids}
                asks = {level.price: level.quantity for level in snapshot.asks}
                last = snapshot.last_update_id
                buffered = list(self._buffer)
                start = self._find_start(buffered, last)
                if start is None:
                    if self._buffer_is_stale(buffered, last):
                        self._commit(bids, asks, last, snapshot.event_time)
                        return
                    continue
                applied_time = snapshot.event_time
                restart = False
                for update in buffered[start:]:
                    if update.last_update_id <= last:
                        continue
                    if update.first_update_id != last + 1:
                        restart = True
                        break
                    _apply_levels(bids, update.bids)
                    _apply_levels(asks, update.asks)
                    last = update.last_update_id
                    applied_time = update.event_time
                if not restart:
                    self._commit(bids, asks, last, applied_time)
                    return
            log.warning("depth sync failed after %d attempts", MAX_SYNC_ATTEMPTS)
            self._state = "idle"
            self._publish_state()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("depth snapshot request failed")
            self._state = "idle"
            self._publish_state()
        finally:
            if self._state != "ready":
                self._sync_task = None
                if self._buffer:
                    metrics.DEPTH_BUFFERED.set(len(self._buffer))

    def _commit(
        self,
        bids: dict[Decimal, Decimal],
        asks: dict[Decimal, Decimal],
        last_update_id: int,
        event_time: datetime,
    ) -> None:
        self._bids = bids
        self._asks = asks
        self._last_update_id = last_update_id
        self._last_event_time = event_time
        self._buffer.clear()
        self._state = "ready"
        self._publish_state()
        log.info("order book synchronized at update %d", last_update_id)

    @staticmethod
    def _find_start(updates: list[DepthUpdate], last: int) -> int | None:
        for index, update in enumerate(updates):
            if update.last_update_id <= last:
                continue
            if update.first_update_id <= last + 1:
                return index
            return None
        return None

    @staticmethod
    def _buffer_is_stale(updates: list[DepthUpdate], last: int) -> bool:
        return all(update.last_update_id <= last for update in updates)

    def _apply(self, update: DepthUpdate) -> None:
        _apply_levels(self._bids, update.bids)
        _apply_levels(self._asks, update.asks)
        self._last_update_id = update.last_update_id
        self._last_event_time = update.event_time


def _apply_levels(book: dict[Decimal, Decimal], levels: tuple[BookLevel, ...]) -> None:
    for level in levels:
        if level.quantity == 0:
            book.pop(level.price, None)
        else:
            book[level.price] = level.quantity
    if len(book) > MAX_BOOK_LEVELS:
        # The snapshot window is 5000 levels. Updates can only add levels close
        # to the touch, so this branch is defensive: drop the oldest insertions
        # instead of letting a pathological stream grow without bound.
        while len(book) > MAX_BOOK_LEVELS:
            book.pop(next(iter(book)))


__all__ = ["DepthBook", "DepthSnapshotSource", "Side"]
