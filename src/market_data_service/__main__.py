"""Process entry point."""

from __future__ import annotations

import argparse
import logging
from dataclasses import replace
from pathlib import Path

import uvicorn

from market_data_service.api import create_app
from market_data_service.config import Settings


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="market_data_service",
        description="Read-only Binance market data service with optional offline replay.",
    )
    parser.add_argument(
        "--replay",
        type=Path,
        metavar="FILE",
        help="replay a recorded JSONL(.gz) file instead of connecting to Binance",
    )
    parser.add_argument(
        "--replay-speed",
        type=float,
        default=None,
        metavar="FACTOR",
        help="replay speed multiplier (default 1.0, original event timing)",
    )
    return parser.parse_args()


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )


def main() -> None:
    """Run the HTTP server with settings from the environment."""
    args = _parse_args()
    settings = Settings.from_env()
    if args.replay is not None:
        if not args.replay.is_file():
            raise SystemExit(f"replay file not found: {args.replay}")
        settings = replace(settings, replay_file=args.replay, enable_ingestion=False)
    if args.replay_speed is not None:
        if settings.replay_file is None:
            raise SystemExit("--replay-speed requires --replay or MDS_REPLAY_FILE")
        if args.replay_speed <= 0:
            raise SystemExit("--replay-speed must be positive")
        settings = replace(settings, replay_speed=args.replay_speed)
    _configure_logging(settings.log_level)
    application = create_app(settings)
    uvicorn.run(
        application,
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
        access_log=False,
    )


if __name__ == "__main__":
    main()
