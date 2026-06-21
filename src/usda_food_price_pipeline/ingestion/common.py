"""Shared helpers for the Phase 1 ingestion scripts.

These utilities are deliberately small and dependency-light (only ``requests``
and ``python-dotenv``). The network-free pieces — backoff math, the rate
limiter, the slug/timestamp/path builders — are unit-tested in ``tests/``.

Phase 1 rule: ingestion writes RAW responses to local files only. Nothing here
touches a cloud warehouse.
"""

from __future__ import annotations

import json
import re
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable

import requests
from dotenv import load_dotenv

# api.data.gov keys (FDC) are capped at 1,000 requests/hour. Keep a little
# headroom so concurrent/manual calls don't push us over the HTTP-429 line.
USDA_RATE_LIMIT_PER_HOUR = 1000

# Repo root is three levels up from this file:
#   src/usda_food_price_pipeline/ingestion/common.py -> repo root
_REPO_ROOT = Path(__file__).resolve().parents[3]


def project_root() -> Path:
    """Absolute path to the repository root."""
    return _REPO_ROOT


def raw_dir(*parts: str) -> Path:
    """Return (and create) a directory under ``data/raw/``.

    Example: ``raw_dir("nutrition")`` -> ``<repo>/data/raw/nutrition``.
    """
    path = _REPO_ROOT / "data" / "raw" / Path(*parts)
    path.mkdir(parents=True, exist_ok=True)
    return path


def utc_timestamp(now: datetime | None = None) -> str:
    """Filesystem-safe UTC timestamp, e.g. ``20260620T184500Z``.

    ``now`` is injectable so filename builders are deterministic in tests.
    """
    now = now or datetime.now(timezone.utc)
    return now.strftime("%Y%m%dT%H%M%SZ")


def slugify(text: str) -> str:
    """Lowercase a string and collapse non-alphanumerics into single ``_``."""
    slug = re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")
    return slug or "item"


def make_session(user_agent: str = "usda-food-price-pipeline/0.1 (ingestion)") -> requests.Session:
    """A ``requests.Session`` with a descriptive User-Agent."""
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})
    return session


def backoff_delay(
    attempt: int,
    backoff_factor: float = 1.0,
    response: requests.Response | None = None,
) -> float:
    """Seconds to wait before retry ``attempt`` (0-indexed).

    Exponential: ``backoff_factor * 2**attempt``. If the server sent an integer
    ``Retry-After`` header (common on HTTP 429), that value wins.
    """
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after and retry_after.strip().isdigit():
            return float(retry_after.strip())
    return backoff_factor * (2 ** attempt)


# Transient HTTP statuses worth retrying: rate-limit + common 5xx.
RETRYABLE_STATUSES = (429, 500, 502, 503, 504)


def retry_request(
    do_request: Callable[[], requests.Response],
    *,
    max_retries: int = 4,
    backoff_factor: float = 1.0,
    retryable_statuses: Iterable[int] = RETRYABLE_STATUSES,
    sleep: Callable[[float], None] = time.sleep,
) -> requests.Response:
    """Call ``do_request()`` with retries on transient failures.

    Retries on connection errors / timeouts and on ``retryable_statuses``,
    sleeping ``backoff_delay`` seconds between attempts. ``sleep`` is injectable
    so tests run instantly. After exhausting retries it returns the last
    response (so the caller can ``raise_for_status``) or re-raises the last
    network exception if no response was ever received.
    """
    retryable = set(retryable_statuses)
    last_response: requests.Response | None = None
    last_exc: Exception | None = None

    for attempt in range(max_retries + 1):
        try:
            response = do_request()
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exc = exc
            last_response = None
        else:
            if response.status_code not in retryable:
                return response
            last_response = response
            last_exc = None

        if attempt < max_retries:
            sleep(backoff_delay(attempt, backoff_factor, last_response))

    if last_response is not None:
        return last_response
    assert last_exc is not None  # loop ran at least once
    raise last_exc


class RateLimiter:
    """Sliding-window limiter: at most ``max_calls`` per ``period`` seconds.

    ``sleep`` and ``monotonic`` are injectable for deterministic tests.
    """

    def __init__(
        self,
        max_calls: int,
        period: float,
        *,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.max_calls = max_calls
        self.period = period
        self._sleep = sleep
        self._monotonic = monotonic
        self._calls: deque[float] = deque()

    def acquire(self) -> None:
        """Block until another call is allowed, then record it."""
        while True:
            now = self._monotonic()
            while self._calls and now - self._calls[0] >= self.period:
                self._calls.popleft()
            if len(self._calls) < self.max_calls:
                self._calls.append(now)
                return
            self._sleep(self.period - (now - self._calls[0]))


def save_json(data: object, path: Path) -> Path:
    """Write ``data`` as UTF-8 JSON, creating parent dirs. Returns ``path``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
    return path


def load_environment() -> None:
    """Load variables from the repo-root ``.env`` (no-op if absent)."""
    load_dotenv(_REPO_ROOT / ".env")
