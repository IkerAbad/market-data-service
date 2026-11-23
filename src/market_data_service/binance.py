"""Minimal Binance REST client used for recovery, never for trading.

Only public endpoints are used:

* ``GET /api/v3/aggTrades`` to backfill trade ids lost during a disconnect.
* ``GET /api/v3/depth`` to anchor the local order book.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any, Final

import httpx

from market_data_service.models import BookSnapshot, Trade
from market_data_service.parser import ParseError, parse_depth_snapshot, parse_rest_agg_trades

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT_S: Final = 10.0


class BinanceError(RuntimeError):
    """Raised when the public REST API cannot serve a request."""


class BinanceClient:
    """Async wrapper around the two public endpoints the service needs."""

    def __init__(self, base_url: str, *, timeout_s: float = DEFAULT_TIMEOUT_S) -> None:
        self._base_url = base_url.rstrip("/")
        self._http = httpx.AsyncClient(
            timeout=timeout_s, headers={"User-Agent": "market-data-service/0.1"}
        )

    async def close(self) -> None:
        """Close the underlying connection pool."""
        await self._http.aclose()

    async def agg_trades(self, symbol: str, from_id: int, limit: int) -> list[Trade]:
        """Fetch aggregate trades starting at ``from_id`` (inclusive)."""
        payload = await self._get(
            "/api/v3/aggTrades",
            {"symbol": symbol, "fromId": str(from_id), "limit": str(limit)},
        )
        try:
            return parse_rest_agg_trades(payload, symbol=symbol, ingested_at=datetime.now(UTC))
        except ParseError as exc:
            raise BinanceError(f"invalid aggTrades response: {exc}") from exc

    async def depth_snapshot(self, symbol: str, limit: int) -> BookSnapshot:
        """Fetch a top-N order book snapshot."""
        payload = await self._get("/api/v3/depth", {"symbol": symbol, "limit": str(limit)})
        if not isinstance(payload, dict):
            raise BinanceError("depth response must be an object")
        try:
            return parse_depth_snapshot(payload, symbol=symbol, ingested_at=datetime.now(UTC))
        except ParseError as exc:
            raise BinanceError(f"invalid depth response: {exc}") from exc

    async def _get(self, path: str, params: dict[str, str]) -> Any:
        try:
            response = await self._http.get(f"{self._base_url}{path}", params=params)
        except httpx.HTTPError as exc:
            raise BinanceError(f"GET {path} failed: {exc}") from exc
        if response.status_code != 200:
            raise BinanceError(f"GET {path} returned {response.status_code}: {response.text[:200]}")
        try:
            return response.json()
        except ValueError as exc:
            raise BinanceError(f"GET {path} returned invalid JSON") from exc
