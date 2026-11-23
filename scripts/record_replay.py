"""Record Binance WebSocket frames for the offline replay mode.

The output is JSONL. The first line is a metadata record with the REST depth
snapshot, so the replay can rebuild the book without any network access. Every
following line is the raw combined-stream frame exactly as Binance sent it.

    python scripts/record_replay.py --duration 330 --output examples/btcusdt-5m.jsonl.gz

The default duration is 330 seconds, slightly more than the five minutes used by
the shipped example. The file is gzipped when the output ends in ``.gz``.
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TextIO

import httpx
from websockets.asyncio.client import connect


def fetch_snapshot_args(symbol: str) -> dict[str, str]:
    """Parameters for the public depth endpoint."""
    return {"symbol": symbol, "limit": "1000"}


async def fetch_depth_snapshot(base_url: str, symbol: str) -> dict[str, Any]:
    """Fetch a raw REST order book snapshot."""
    async with httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=15.0) as client:
        response = await client.get("/api/v3/depth", params=fetch_snapshot_args(symbol))
        response.raise_for_status()
        payload = response.json()
    if not isinstance(payload, dict) or "lastUpdateId" not in payload:
        raise RuntimeError("depth endpoint returned an unexpected payload")
    return payload


def open_output(path: Path) -> TextIO:
    """Open a plain or gzipped JSONL output for writing."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == ".gz":
        return gzip.open(path, "wt", encoding="utf-8")
    return path.open("w", encoding="utf-8", newline="\n")


def combined_stream_url(base_url: str, symbol: str) -> str:
    """Combined-stream URL for one symbol."""
    streams = f"{symbol.lower()}@aggTrade/{symbol.lower()}@depth@100ms"
    return f"{base_url.rstrip('/')}/stream?streams={streams}"


async def record(
    *,
    symbol: str,
    duration: int,
    output: Path,
    ws_base: str,
    rest_base: str,
) -> int:
    """Record for ``duration`` seconds and return the number of frames."""
    url = combined_stream_url(ws_base, symbol)
    print(f"connecting to {url}")
    async with connect(url, max_queue=4_096, ping_interval=20, ping_timeout=20) as socket:
        print("fetching depth snapshot")
        snapshot = await fetch_depth_snapshot(rest_base, symbol)
        meta = {
            "type": "meta",
            "symbol": symbol.upper(),
            "recorded_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "depth_snapshot": snapshot,
        }
        frames = 0
        with open_output(output) as handle:
            handle.write(json.dumps(meta, separators=(",", ":")) + "\n")
            started = time.monotonic()
            async for raw in socket:
                text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
                handle.write(text + "\n")
                frames += 1
                if time.monotonic() - started >= duration:
                    break
    size = output.stat().st_size
    print(f"recorded {frames} frames in {duration}s to {output} ({size / 1024:.1f} KiB)")
    if size >= 5 * 1024 * 1024:
        print("warning: the recording is 5 MiB or larger; gzip it or reduce the duration")
    return frames


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=int, default=330, help="recording length in seconds")
    parser.add_argument("--symbol", default="BTCUSDT", help="Binance spot symbol")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("examples/btcusdt-5m.jsonl.gz"),
        help="JSONL output; .gz enables gzip",
    )
    parser.add_argument("--ws-base", default="wss://stream.binance.com:9443")
    parser.add_argument("--rest-base", default="https://api.binance.com")
    args = parser.parse_args()
    if args.duration < 1:
        parser.error("--duration must be positive")
    asyncio.run(
        record(
            symbol=args.symbol.upper(),
            duration=args.duration,
            output=args.output,
            ws_base=args.ws_base,
            rest_base=args.rest_base,
        )
    )


if __name__ == "__main__":
    main()
