"""FastAPI application: REST analytics, live WebSocket and the dashboard."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any, cast

from fastapi import Depends, FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, PlainSerializer

from market_data_service.aggregator import SUPPORTED_INTERVALS, bucket_end
from market_data_service.config import Settings
from market_data_service.models import Candle, Trade
from market_data_service.service import Service

STATIC_DIR = Path(__file__).parent / "static"
SYMBOL_PATTERN = r"^[A-Z0-9]{5,20}$"


def decimal_str(value: Decimal) -> str:
    """Render a Decimal without exponent or storage padding."""
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


PlainDecimal = Annotated[Decimal, PlainSerializer(decimal_str, return_type=str, when_used="json")]


class Interval(StrEnum):
    """Intervals exposed by the API."""

    ONE_SECOND = "1s"
    FIVE_SECONDS = "5s"
    ONE_MINUTE = "1m"


INTERVAL_SECONDS: dict[Interval, int] = {
    Interval.ONE_SECOND: 1,
    Interval.FIVE_SECONDS: 5,
    Interval.ONE_MINUTE: 60,
}
assert set(INTERVAL_SECONDS.values()) == set(SUPPORTED_INTERVALS)


class LevelOut(BaseModel):
    """One order book level."""

    price: PlainDecimal
    quantity: PlainDecimal


class TradeOut(BaseModel):
    """Normalized trade as exposed over HTTP."""

    symbol: str
    trade_id: int
    price: PlainDecimal
    quantity: PlainDecimal
    quote_quantity: PlainDecimal
    trade_time: datetime
    ingested_at: datetime
    side: str

    @classmethod
    def from_trade(cls, trade: Trade) -> TradeOut:
        """Build the response model from a normalized trade."""
        return cls(
            symbol=trade.symbol,
            trade_id=trade.trade_id,
            price=trade.price,
            quantity=trade.quantity,
            quote_quantity=trade.quote_quantity,
            trade_time=trade.trade_time,
            ingested_at=trade.ingested_at,
            side=trade.side,
        )


class CandleOut(BaseModel):
    """OHLCV bucket. Times are UTC, decimals are strings in JSON."""

    open_time: datetime
    close_time: datetime
    interval: int
    open: PlainDecimal
    high: PlainDecimal
    low: PlainDecimal
    close: PlainDecimal
    volume: PlainDecimal
    quote_volume: PlainDecimal
    trade_count: int
    closed: bool

    @classmethod
    def from_candle(cls, candle: Candle) -> CandleOut:
        """Build the response model from an aggregated candle."""
        return cls(
            open_time=candle.open_time,
            close_time=bucket_end(candle.open_time, candle.interval_seconds),
            interval=candle.interval_seconds,
            open=candle.open,
            high=candle.high,
            low=candle.low,
            close=candle.close,
            volume=candle.volume,
            quote_volume=candle.quote_volume,
            trade_count=candle.trade_count,
            closed=candle.closed,
        )


class BookOut(BaseModel):
    """Order book view."""

    symbol: str
    update_id: int
    time: datetime
    bids: list[LevelOut]
    asks: list[LevelOut]
    source: str


class IngestStatus(BaseModel):
    """Ingestion state."""

    connected: bool
    symbol: str
    last_trade_id: int | None
    last_trade_time: datetime | None
    lag_seconds: float | None
    reconnects: int
    gaps_detected: int
    trades_backfilled: int
    trades_missing: int
    backfilling: bool
    pending_trades: int
    book_ready: bool
    book_update_id: int | None


class StorageStatus(BaseModel):
    """Lake and writer state."""

    trades_files: int
    trades_bytes: int
    book_files: int
    book_bytes: int
    buffered_rows: int
    files_written: int
    rows_written: int
    trades_stored: int


class ReplayStatus(BaseModel):
    """Offline replay progress."""

    file: str
    speed: float
    frames: int
    position: int
    finished: bool
    error: str | None


class StatusResponse(BaseModel):
    """Aggregated status payload."""

    mode: str
    replay: ReplayStatus | None
    ingest: IngestStatus
    storage: StorageStatus
    websocket_clients: int
    uptime_seconds: float
    server_time: datetime


class HealthResponse(BaseModel):
    """Liveness payload used by Docker and Prometheus."""

    status: str
    mode: str
    symbol: str
    connected: bool
    book_ready: bool


def get_service(request: Request) -> Service:
    """Resolve the process-wide service from application state."""
    return cast(Service, request.app.state.service)


ServiceDep = Annotated[Service, Depends(get_service)]
SymbolQuery = Annotated[str, Query(pattern=SYMBOL_PATTERN, description="Binance spot symbol")]


def create_app(settings: Settings | None = None) -> FastAPI:
    """Application factory. Tests call it with explicit settings."""
    resolved = settings or Settings.from_env()
    service = Service(resolved)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        await service.start()
        try:
            yield
        finally:
            await service.stop()

    app = FastAPI(
        title="Market Data Service",
        version="0.1.0",
        description="Read-only Binance market data ingestion, Parquet lake and DuckDB analytics.",
        lifespan=lifespan,
    )
    app.state.service = service

    @app.get("/api/v1/health", response_model=HealthResponse, tags=["system"])
    async def health(dep: ServiceDep) -> HealthResponse:
        status = dep.ingestor.status()
        return HealthResponse(
            status="ok",
            mode=dep.mode,
            symbol=resolved.symbol,
            connected=bool(status["connected"]),
            book_ready=bool(status["book_ready"]),
        )

    @app.get("/api/v1/price", response_model=TradeOut, tags=["market data"])
    async def latest_price(
        dep: ServiceDep,
        symbol: SymbolQuery = resolved.symbol,
    ) -> TradeOut:
        live = dep.pipeline.last_trade
        if live is not None and live.symbol == symbol:
            return TradeOut.from_trade(live)
        row = await dep.analytics.latest_trade_async(symbol)
        if row is None:
            raise HTTPException(status_code=404, detail=f"no trades stored for {symbol}")
        return TradeOut(
            symbol=str(row["symbol"]),
            trade_id=int(cast(int, row["trade_id"])),
            price=Decimal(str(row["price"])),
            quantity=Decimal(str(row["quantity"])),
            quote_quantity=Decimal(str(row["quote_quantity"])),
            trade_time=cast(datetime, row["trade_time"]),
            ingested_at=cast(datetime, row["ingested_at"]),
            side="sell" if bool(row["is_buyer_maker"]) else "buy",
        )

    @app.get("/api/v1/candles", response_model=list[CandleOut], tags=["market data"])
    async def candles(
        dep: ServiceDep,
        interval: Interval = Interval.ONE_MINUTE,
        limit: Annotated[int, Query(ge=1, le=1_000)] = 200,
        symbol: SymbolQuery = resolved.symbol,
    ) -> list[CandleOut]:
        seconds = INTERVAL_SECONDS[interval]
        persisted = await dep.analytics.candles_async(symbol, seconds, limit)
        merged = {candle.open_time: candle for candle in persisted}
        if symbol == resolved.symbol:
            for candle in dep.aggregator.recent(seconds, limit):
                merged[candle.open_time] = candle
        ordered = sorted(merged.values(), key=lambda candle: candle.open_time)[-limit:]
        return [CandleOut.from_candle(candle) for candle in ordered]

    @app.get("/api/v1/book", response_model=BookOut, tags=["market data"])
    async def book(
        dep: ServiceDep,
        limit: Annotated[int, Query(ge=1, le=100)] = 20,
    ) -> BookOut:
        live = dep.depth.snapshot(limit)
        if live is not None:
            return BookOut(
                symbol=live.symbol,
                update_id=live.last_update_id,
                time=live.event_time,
                bids=[LevelOut(price=level.price, quantity=level.quantity) for level in live.bids],
                asks=[LevelOut(price=level.price, quantity=level.quantity) for level in live.asks],
                source="memory",
            )
        persisted = await dep.analytics.latest_book_async(resolved.symbol, limit)
        if persisted is None:
            raise HTTPException(status_code=404, detail="order book is not synchronized yet")
        return BookOut(
            symbol=resolved.symbol,
            update_id=0,
            time=dep.now(),
            bids=[
                LevelOut(price=Decimal(level["price"]), quantity=Decimal(level["quantity"]))
                for level in persisted["bids"]
            ],
            asks=[
                LevelOut(price=Decimal(level["price"]), quantity=Decimal(level["quantity"]))
                for level in persisted["asks"]
            ],
            source="parquet",
        )

    @app.get("/api/v1/status", response_model=StatusResponse, tags=["system"])
    async def status(dep: ServiceDep) -> StatusResponse:
        payload: dict[str, Any] = await dep.status()
        return StatusResponse.model_validate(payload)

    @app.get("/metrics", include_in_schema=False)
    async def metrics_endpoint() -> Response:
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.websocket("/ws")
    async def live(websocket: WebSocket) -> None:
        await websocket.accept()
        queue = service.hub.subscribe()
        await websocket.send_json(
            {
                "type": "hello",
                "mode": service.mode,
                "symbol": resolved.symbol,
                "intervals": list(SUPPORTED_INTERVALS),
                "depths": resolved.depth_levels,
            }
        )

        async def pump() -> None:
            while True:
                await websocket.send_text(await queue.get())

        sender = asyncio.create_task(pump())
        try:
            while True:
                message = await websocket.receive()
                if message["type"] == "websocket.disconnect":
                    break
        except WebSocketDisconnect:
            pass
        finally:
            sender.cancel()
            service.hub.unsubscribe(queue)
            with contextlib.suppress(asyncio.CancelledError, WebSocketDisconnect):
                await sender

    @app.get("/", include_in_schema=False)
    async def dashboard() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html", headers={"Cache-Control": "no-store"})

    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
    return app
