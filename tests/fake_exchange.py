"""Fixture for a fake Binance exchange used by the integration test."""

from __future__ import annotations

import asyncio
import json
import socket
import threading
import time
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, WebSocket

FIXTURE = Path(__file__).parent / "fixtures" / "agg_trades.jsonl"


class FakeExchange:
    """Replays the aggregate trades stored in ``FIXTURE``.

    The first WebSocket connection receives ``first_batch`` trades and is then
    closed. The server rejects connections for exactly ``downtime`` seconds.
    After the outage it resumes at ``resume_at``, leaving a deliberate hole in
    the stream that only the REST endpoint can fill.
    """

    def __init__(
        self,
        *,
        first_batch: int = 20,
        resume_at: int = 26,
        downtime: float = 5.0,
    ) -> None:
        self._frames = self._load_frames()
        self._trades = [frame["data"] for frame in self._frames]
        self.total = len(self._frames)
        self.resume_at = resume_at
        self.downtime = downtime
        self._first_batch = first_batch
        self._first_done = False
        self._served_resume = False
        self._resumed = threading.Event()
        self._finished = threading.Event()
        self._lock = threading.Lock()
        self._cut_at: float | None = None
        self._resume_at: float | None = None
        self._server: uvicorn.Server | None = None
        self._thread: threading.Thread | None = None
        self.port = 0
        self.app = self._build_app()

    @staticmethod
    def _load_frames() -> list[dict[str, Any]]:
        text = FIXTURE.read_text(encoding="utf-8")
        frames = [json.loads(line) for line in text.splitlines() if line.strip()]
        if not frames:
            raise ValueError(f"empty fixture: {FIXTURE}")
        return frames

    def _build_app(self) -> FastAPI:
        app = FastAPI()

        @app.websocket("/stream")
        async def stream(websocket: WebSocket) -> None:
            await websocket.accept()
            with self._lock:
                first = not self._first_done
                if first:
                    self._first_done = True
                    self._cut_at = time.monotonic()
            if first:
                for frame in self._frames[: self._first_batch]:
                    await websocket.send_text(json.dumps(frame))
                    await asyncio.sleep(0.004)
                threading.Timer(self.downtime, self._resume).start()
                await websocket.close()
                return
            if not self._resumed.is_set():
                await websocket.close()
                return
            with self._lock:
                if self._served_resume:
                    await websocket.close()
                    return
                self._served_resume = True
            for frame in self._frames[self.resume_at - 1 :]:
                await websocket.send_text(json.dumps(frame))
                await asyncio.sleep(0.004)
            self._finished.set()
            await asyncio.sleep(0.2)
            await websocket.close()

        @app.get("/api/v3/aggTrades")
        def agg_trades(symbol: str, fromId: int, limit: int = 1000) -> list[dict[str, Any]]:  # noqa: N803
            del symbol
            return [payload for payload in self._trades if payload["a"] >= fromId][:limit]

        @app.get("/api/v3/depth")
        def depth(symbol: str, limit: int = 100) -> dict[str, Any]:
            del symbol, limit
            return {
                "lastUpdateId": self.total,
                "bids": [["61000.00", "1.0"]],
                "asks": [["62000.00", "1.0"]],
            }

        return app

    def _resume(self) -> None:
        self._resume_at = time.monotonic()
        self._resumed.set()

    def start(self) -> None:
        """Start uvicorn in a daemon thread and wait for the port."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind(("127.0.0.1", 0))
            self.port = int(probe.getsockname()[1])
        config = uvicorn.Config(
            self.app,
            host="127.0.0.1",
            port=self.port,
            log_level="warning",
            access_log=False,
            lifespan="off",
        )
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, name="fake-exchange", daemon=True)
        self._thread.start()
        deadline = time.monotonic() + 10
        while not self._server.started:
            if time.monotonic() > deadline:
                raise RuntimeError("fake exchange did not start")
            time.sleep(0.01)

    def stop(self) -> None:
        """Stop uvicorn and join the thread."""
        if self._server is not None:
            self._server.should_exit = True
        if self._thread is not None:
            self._thread.join(timeout=10)

    @property
    def http_base(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def ws_base(self) -> str:
        return f"ws://127.0.0.1:{self.port}"

    @property
    def cut_at(self) -> float | None:
        return self._cut_at

    @property
    def resume_time(self) -> float | None:
        return self._resume_at

    @property
    def finished(self) -> bool:
        return self._finished.is_set()


__all__ = ["FakeExchange"]
