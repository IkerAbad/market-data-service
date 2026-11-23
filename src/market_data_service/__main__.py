"""Process entry point."""

from __future__ import annotations

import logging

import uvicorn

from market_data_service.api import create_app
from market_data_service.config import Settings


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )


def main() -> None:
    """Run the HTTP server with settings from the environment."""
    settings = Settings.from_env()
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
