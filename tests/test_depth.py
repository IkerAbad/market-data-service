"""Depth book tests: snapshot anchoring, updates and resynchronization."""

from __future__ import annotations

import asyncio
from collections import deque
from datetime import timedelta
from decimal import Decimal

from market_data_service.depth import DepthBook
from market_data_service.models import BookLevel, BookSnapshot, DepthUpdate
from tests.support import T0


def _snapshot(update_id: int, *, bid: str = "100", ask: str = "101") -> BookSnapshot:
    return BookSnapshot(
        symbol="BTCUSDT",
        last_update_id=update_id,
        event_time=T0,
        ingested_at=T0,
        bids=(BookLevel(price=Decimal(bid), quantity=Decimal("1")),),
        asks=(BookLevel(price=Decimal(ask), quantity=Decimal("2")),),
    )


def _update(
    first: int, last: int, *, bid: str | None = None, ask: str | None = None
) -> DepthUpdate:
    bids: tuple[BookLevel, ...] = ()
    asks: tuple[BookLevel, ...] = ()
    if bid is not None:
        bids = (BookLevel(price=Decimal(bid), quantity=Decimal("3")),)
    if ask is not None:
        asks = (BookLevel(price=Decimal(ask), quantity=Decimal("4")),)
    return DepthUpdate(
        symbol="BTCUSDT",
        first_update_id=first,
        last_update_id=last,
        event_time=T0 + timedelta(milliseconds=last),
        ingested_at=T0,
        bids=bids,
        asks=asks,
    )


class StubSource:
    def __init__(self, snapshots: list[BookSnapshot]) -> None:
        self._snapshots = deque(snapshots)
        self.calls = 0

    async def depth_snapshot(self, symbol: str, limit: int) -> BookSnapshot:
        del symbol, limit
        self.calls += 1
        if not self._snapshots:
            raise AssertionError("no snapshot left")
        return self._snapshots.popleft()


async def _wait_ready(book: DepthBook, timeout: float = 2.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not book.ready:
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("book did not synchronize")
        await asyncio.sleep(0.01)


async def test_snapshot_anchors_the_book_and_updates_apply() -> None:
    source = StubSource([_snapshot(10)])
    book = DepthBook("BTCUSDT", 5, source)
    book.on_update(_update(10, 10, bid="100.5"))
    await _wait_ready(book)

    snapshot = book.snapshot(5)
    assert snapshot is not None
    assert snapshot.last_update_id == 10
    assert [level.price for level in snapshot.bids] == [Decimal("100")]
    assert [level.price for level in snapshot.asks] == [Decimal("101")]

    book.on_update(_update(11, 11, bid="100.5", ask="101.5"))
    assert book.last_update_id == 11
    updated = book.snapshot(5)
    assert updated is not None
    assert updated.bids[0].price == Decimal("100.5")
    assert updated.asks[0].price == Decimal("101")

    book.on_update(_update(12, 12, bid="99.5"))
    final = book.snapshot(5)
    assert final is not None
    assert [level.price for level in final.bids] == [
        Decimal("100.5"),
        Decimal("100"),
        Decimal("99.5"),
    ]


async def test_gap_triggers_resync_from_a_fresh_snapshot() -> None:
    source = StubSource([_snapshot(10), _snapshot(20, bid="95", ask="105")])
    book = DepthBook("BTCUSDT", 5, source)
    book.on_update(_update(10, 10))
    await _wait_ready(book)
    assert source.calls == 1

    book.on_update(_update(15, 15, bid="1"))
    await _wait_ready(book)
    assert source.calls == 2
    assert book.last_update_id == 20
    snapshot = book.snapshot(5)
    assert snapshot is not None
    assert snapshot.bids[0].price == Decimal("95")


async def test_reset_discards_state() -> None:
    source = StubSource([_snapshot(10)])
    book = DepthBook("BTCUSDT", 5, source)
    book.on_update(_update(10, 10))
    await _wait_ready(book)
    assert book.snapshot(5) is not None

    book.reset()
    assert book.ready is False
    assert book.snapshot(5) is None
