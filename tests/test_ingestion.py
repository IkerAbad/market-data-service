"""Reconnection backoff properties."""

from __future__ import annotations

import random

from market_data_service.ingestion import backoff_delay


def test_first_attempt_stays_inside_the_jitter_window() -> None:
    rng = random.Random(7)
    for _ in range(100):
        delay = backoff_delay(0, base=0.5, cap=30.0, rng=rng)
        assert 0.25 <= delay <= 0.5


def test_backoff_grows_then_hits_the_cap() -> None:
    rng = random.Random(11)
    early = backoff_delay(1, base=0.5, cap=30.0, rng=rng)
    assert 0.5 <= early <= 1.0

    for _ in range(100):
        delay = backoff_delay(20, base=0.5, cap=2.0, rng=rng)
        assert 1.0 <= delay <= 2.0


def test_jitter_is_not_constant() -> None:
    rng = random.Random(3)
    values = {backoff_delay(5, base=1.0, cap=30.0, rng=rng) for _ in range(20)}
    assert len(values) > 1
