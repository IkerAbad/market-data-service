"""Offline replay of recorded Binance frames.

The replay file is JSONL: a metadata line with a REST depth snapshot followed by
the raw combined-stream frames. Replay feeds the same pipeline as the live
ingestor, but the clock and the pacing come from the recorded exchange
timestamps. No network client is created in this mode.
"""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from market_data_service.ingestion import Ingestor
from market_data_service.models import BookSnapshot, Trade
from market_data_service.parser import parse_depth_snapshot

log = logging.getLogger(__name__)

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class ReplayError(ValueError):
    """Raised when a recording is missing or malformed."""


@dataclass(frozen=True, slots=True)
class ReplayMeta:
    """Metadata written at the start of a recording."""

    symbol: str
    recorded_at: datetime
    depth_snapshot: BookSnapshot


@dataclass(frozen=True, slots=True)
class ReplayFrame:
    """One raw combined-stream frame and its exchange event time."""

    raw: str
    event_time: datetime


@dataclass(frozen=True, slots=True)
class ReplayDataset:
    """A loaded recording."""

    path: Path
    meta: ReplayMeta
    frames: tuple[ReplayFrame, ...]

    @property
    def duration_seconds(self) -> float:
        """Event-time span of the recording."""
        if not self.frames:
            return 0.0
        return (self.frames[-1].event_time - self.frames[0].event_time).total_seconds()


class ReplayClock:
    """Mutable clock advanced by the replay runner."""

    def __init__(self, initial: datetime) -> None:
        self._now = initial

    def __call__(self) -> datetime:
        return self._now

    def set(self, value: datetime) -> None:
        """Move the clock to the next recorded event time."""
        self._now = value


class ReplayTradeSource:
    """Trade source with no network access.

    A gap in a recording cannot be repaired offline. Returning an empty page
    makes the ingestor report the missing ids instead of contacting Binance.
    """

    async def agg_trades(self, symbol: str, from_id: int, limit: int) -> list[Trade]:
        del symbol, from_id, limit
        return []


class ReplaySnapshotSource:
    """Serves the depth snapshot embedded in the recording."""

    def __init__(self, snapshot: BookSnapshot) -> None:
        self._snapshot = snapshot

    async def depth_snapshot(self, symbol: str, limit: int) -> BookSnapshot:
        del limit
        if symbol.upper() != self._snapshot.symbol:
            raise ReplayError(f"snapshot is for {self._snapshot.symbol}, not {symbol}")
        return self._snapshot


class ReplayRunner:
    """Feeds recorded frames to the ingestor at their original pace."""

    def __init__(
        self,
        ingestor: Ingestor,
        dataset: ReplayDataset,
        clock: ReplayClock,
        speed: float,
    ) -> None:
        if speed <= 0:
            raise ReplayError("replay speed must be positive")
        self._ingestor = ingestor
        self._dataset = dataset
        self._clock = clock
        self._speed = speed
        self._position = 0
        self._finished = False
        self._error: str | None = None

    async def run(self) -> None:
        """Replay every frame. The task ends when the file is exhausted."""
        if not self._dataset.frames:
            self._finished = True
            return
        start = time.monotonic()
        first_event = self._dataset.frames[0].event_time
        try:
            for index, frame in enumerate(self._dataset.frames):
                target = (frame.event_time - first_event).total_seconds() / self._speed
                delay = target - (time.monotonic() - start)
                if delay > 0:
                    await asyncio.sleep(delay)
                self._clock.set(frame.event_time)
                await self._ingestor.handle_frame(frame.raw)
                self._position = index + 1
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception("replay failed")
            self._error = str(exc)
        finally:
            self._finished = True

    def status(self) -> dict[str, object]:
        """Progress for the status endpoint."""
        return {
            "file": str(self._dataset.path),
            "speed": self._speed,
            "frames": len(self._dataset.frames),
            "position": self._position,
            "finished": self._finished,
            "error": self._error,
        }


def load_replay_dataset(path: Path) -> ReplayDataset:
    """Load and validate a plain or gzipped JSONL recording."""
    if not path.is_file():
        raise FileNotFoundError(f"replay file not found: {path}")
    lines = _read_lines(path)
    if not lines:
        raise ReplayError(f"empty replay file: {path}")
    meta_payload = _as_mapping(json.loads(lines[0]), "metadata")
    if meta_payload.get("type") != "meta":
        raise ReplayError("the first line must be a metadata record")
    symbol_raw = meta_payload.get("symbol")
    if not isinstance(symbol_raw, str) or not symbol_raw:
        raise ReplayError("metadata has no symbol")
    symbol = symbol_raw.upper()
    recorded_raw = meta_payload.get("recorded_at")
    if not isinstance(recorded_raw, str):
        raise ReplayError("metadata has no recorded_at")
    try:
        recorded_at = datetime.fromisoformat(recorded_raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ReplayError(f"invalid recorded_at: {recorded_raw}") from exc
    if recorded_at.tzinfo is None:
        recorded_at = recorded_at.replace(tzinfo=UTC)
    snapshot_raw = meta_payload.get("depth_snapshot")
    if not isinstance(snapshot_raw, dict):
        raise ReplayError("metadata has no depth_snapshot")
    snapshot = parse_depth_snapshot(
        _as_mapping(snapshot_raw, "depth_snapshot"),
        symbol=symbol,
        ingested_at=recorded_at,
    )

    frames: list[ReplayFrame] = []
    for line in lines[1:]:
        payload = _as_mapping(json.loads(line), "frame")
        data_raw = payload.get("data", payload)
        data = _as_mapping(data_raw, "data")
        frame_symbol = data.get("s")
        if isinstance(frame_symbol, str) and frame_symbol.upper() != symbol:
            raise ReplayError(f"frame is for {frame_symbol}, not {symbol}")
        event_ms = data.get("E", data.get("T"))
        if not isinstance(event_ms, int) or isinstance(event_ms, bool):
            continue
        frames.append(
            ReplayFrame(
                raw=line,
                event_time=_EPOCH + timedelta(milliseconds=event_ms),
            )
        )
    if not frames:
        raise ReplayError("the recording contains no market-data frames")
    return ReplayDataset(
        path=path, meta=ReplayMeta(symbol, recorded_at, snapshot), frames=tuple(frames)
    )


def _read_lines(path: Path) -> list[str]:
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            return [line.strip() for line in handle if line.strip()]
    with path.open("r", encoding="utf-8") as handle:
        return [line.strip() for line in handle if line.strip()]


def _as_mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReplayError(f"{field} must be an object")
    return value


__all__ = [
    "ReplayClock",
    "ReplayDataset",
    "ReplayError",
    "ReplayFrame",
    "ReplayMeta",
    "ReplayRunner",
    "ReplaySnapshotSource",
    "ReplayTradeSource",
    "load_replay_dataset",
]
