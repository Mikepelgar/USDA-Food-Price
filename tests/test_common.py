"""Tests for the shared ingestion helpers (no real network)."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
import requests

from usda_food_price_pipeline.ingestion import common


# ---- small helpers -----------------------------------------------------------

class FakeResponse:
    def __init__(self, status_code, headers=None):
        self.status_code = status_code
        self.headers = headers or {}


def caller(items):
    """Return a no-arg callable yielding items in order; Exceptions are raised."""
    it = iter(items)

    def call():
        value = next(it)
        if isinstance(value, Exception):
            raise value
        return value

    return call


class FakeClock:
    def __init__(self, start=0.0):
        self.t = start
        self.sleeps = []

    def monotonic(self):
        return self.t

    def sleep(self, secs):
        self.sleeps.append(secs)
        self.t += secs


# ---- backoff_delay -----------------------------------------------------------

def test_backoff_delay_is_exponential():
    assert common.backoff_delay(0, 1.0) == 1.0
    assert common.backoff_delay(1, 1.0) == 2.0
    assert common.backoff_delay(2, 2.0) == 8.0


def test_backoff_delay_honors_integer_retry_after():
    resp = FakeResponse(429, {"Retry-After": "7"})
    assert common.backoff_delay(3, 1.0, resp) == 7.0


def test_backoff_delay_ignores_non_integer_retry_after():
    resp = FakeResponse(429, {"Retry-After": "soon"})
    assert common.backoff_delay(0, 1.0, resp) == 1.0


# ---- retry_request -----------------------------------------------------------

def test_retry_request_returns_immediately_on_success():
    sleeps = []
    resp = common.retry_request(
        caller([FakeResponse(200)]), sleep=sleeps.append
    )
    assert resp.status_code == 200
    assert sleeps == []


def test_retry_request_retries_then_succeeds():
    sleeps = []
    resp = common.retry_request(
        caller([FakeResponse(429), FakeResponse(200)]),
        backoff_factor=1.0,
        sleep=sleeps.append,
    )
    assert resp.status_code == 200
    assert sleeps == [1.0]  # one backoff between the two attempts


def test_retry_request_exhausts_and_returns_last_response():
    sleeps = []
    resp = common.retry_request(
        caller([FakeResponse(500), FakeResponse(500), FakeResponse(500)]),
        max_retries=2,
        backoff_factor=1.0,
        sleep=sleeps.append,
    )
    assert resp.status_code == 500
    assert sleeps == [1.0, 2.0]  # max_retries backoffs


def test_retry_request_retries_on_connection_error():
    sleeps = []
    resp = common.retry_request(
        caller([requests.ConnectionError("boom"), FakeResponse(200)]),
        sleep=sleeps.append,
    )
    assert resp.status_code == 200
    assert sleeps == [1.0]


def test_retry_request_reraises_when_all_attempts_error():
    with pytest.raises(requests.Timeout):
        common.retry_request(
            caller([requests.Timeout("t1"), requests.Timeout("t2")]),
            max_retries=1,
            sleep=lambda _s: None,
        )


# ---- RateLimiter -------------------------------------------------------------

def test_rate_limiter_allows_up_to_limit_without_sleeping():
    clock = FakeClock()
    limiter = common.RateLimiter(2, period=10, sleep=clock.sleep, monotonic=clock.monotonic)
    limiter.acquire()
    limiter.acquire()
    assert clock.sleeps == []


def test_rate_limiter_sleeps_when_limit_exceeded():
    clock = FakeClock()
    limiter = common.RateLimiter(2, period=10, sleep=clock.sleep, monotonic=clock.monotonic)
    limiter.acquire()
    limiter.acquire()
    limiter.acquire()  # third call must wait out the window
    assert clock.sleeps == [10]


# ---- small pure helpers ------------------------------------------------------

def test_slugify():
    assert common.slugify("Cheddar Cheese!") == "cheddar_cheese"
    assert common.slugify("  ") == "item"


def test_utc_timestamp_format():
    ts = common.utc_timestamp(datetime(2026, 6, 20, 18, 45, 0, tzinfo=timezone.utc))
    assert ts == "20260620T184500Z"


def test_save_json_roundtrip(tmp_path):
    path = common.save_json({"a": 1, "ünïcode": "✓"}, tmp_path / "sub" / "out.json")
    assert path.exists()
    assert json.loads(path.read_text(encoding="utf-8")) == {"a": 1, "ünïcode": "✓"}
