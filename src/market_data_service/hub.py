"""In-process fan-out for WebSocket clients."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from typing import Any

log = logging.getLogger(__name__)

MAX_QUEUE = 1_000


class EventHub:
    """Broadcast JSON messages to every connected dashboard."""

    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue[str]] = set()

    @property
    def subscribers(self) -> int:
        """Number of active WebSocket clients."""
        return len(self._subscribers)

    def subscribe(self) -> asyncio.Queue[str]:
        """Register a queue and return it."""
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=MAX_QUEUE)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[str]) -> None:
        """Remove a queue. Safe to call twice."""
        self._subscribers.discard(queue)

    def publish(self, message: dict[str, Any]) -> None:
        """Enqueue a message, dropping the oldest frame if a client lags."""
        payload = json.dumps(message, default=str, separators=(",", ":"))
        for queue in self._subscribers:
            if queue.full():
                with contextlib.suppress(asyncio.QueueEmpty):
                    queue.get_nowait()
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:  # pragma: no cover - defensive
                log.warning("dropping frame for a slow subscriber")
