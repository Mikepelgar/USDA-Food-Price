"""Phase 1 ingestion: BLS Average Price Data (retail food prices).

This is the live, forecastable monthly price feed (BLS "APU" Average Price
series, U.S. city average). NOTE: BLS is not USDA. It is a real JSON API.

Saves the raw API response as timestamped JSON under ``data/raw/prices/bls/``.

Run:
    python -m usda_food_price_pipeline.ingestion.prices_bls

``BLS_API_KEY`` in ``.env`` is OPTIONAL: with a key we use the v2 endpoint
(higher daily limit, up to 50 series / 20 years); without one we fall back to
the unauthenticated v1 endpoint (lower limits).
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone

from . import common

BLS_API_V1 = "https://api.bls.gov/publicAPI/v1/timeseries/data/"
BLS_API_V2 = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

# Curated APU series, U.S. city average (area code 0000). Titles match the
# official BLS catalog (verified 2026-06-20). Series ID format is
# APU + area(4) + item code; browse/expand via the BLS series finder:
# https://data.bls.gov/cgi-bin/surveymost?ap
DEFAULT_SERIES = {
    "APU0000708111": "Eggs, grade A, large, per doz.",
    "APU0000709112": "Milk, fresh, whole, fortified, per gal.",
    "APU0000702111": "Bread, white, pan, per lb.",
    "APU0000701111": "Flour, white, all purpose, per lb.",
    "APU0000703112": "Ground beef, 100% beef, per lb.",
    "APU0000FF1101": "Chicken breast, boneless, per lb.",
    "APU0000711211": "Bananas, per lb.",
    "APU0000712112": "Potatoes, white, per lb.",
}


def bls_endpoint(api_key: str | None) -> str:
    """v2 endpoint when a registration key is present, else v1."""
    return BLS_API_V2 if api_key else BLS_API_V1


def build_bls_payload(
    series_ids,
    start_year: int,
    end_year: int,
    api_key: str | None = None,
) -> dict:
    """Request body for the BLS timeseries POST. Key included only if set."""
    payload = {
        "seriesid": list(series_ids),
        "startyear": str(start_year),
        "endyear": str(end_year),
    }
    if api_key:
        payload["registrationkey"] = api_key
    return payload


def default_year_range(now: datetime | None = None) -> tuple[int, int]:
    """Recent window: (current_year - 3, current_year)."""
    year = (now or datetime.now(timezone.utc)).year
    return year - 3, year


def fetch_series(
    session,
    series_ids,
    start_year: int,
    end_year: int,
    api_key: str | None,
) -> dict:
    """POST to the BLS API (with retries) and return parsed JSON."""
    url = bls_endpoint(api_key)
    payload = build_bls_payload(series_ids, start_year, end_year, api_key)
    response = common.retry_request(
        lambda: session.post(url, json=payload, timeout=60)
    )
    response.raise_for_status()
    return response.json()


def ingest(
    series=DEFAULT_SERIES,
    start_year: int | None = None,
    end_year: int | None = None,
    api_key: str | None = None,
):
    """Pull BLS price series and save the raw response. Returns the saved path."""
    if start_year is None or end_year is None:
        default_start, default_end = default_year_range()
        start_year = start_year or default_start
        end_year = end_year or default_end

    out_dir = common.raw_dir("prices", "bls")
    session = common.make_session()
    series_ids = list(series)

    data = fetch_series(session, series_ids, start_year, end_year, api_key)
    path = out_dir / f"bls_ap_{common.utc_timestamp()}.json"
    common.save_json(data, path)

    status = data.get("status", "?")
    n_series = len(data.get("Results", {}).get("series", []))
    print(f"  saved {path.name}  (status={status}, {n_series} series, {start_year}-{end_year})")
    return path


def main(argv: list[str] | None = None) -> int:
    default_start, default_end = default_year_range()
    parser = argparse.ArgumentParser(description="Ingest BLS Average Price (food) data (raw, local).")
    parser.add_argument("--series", nargs="+", default=list(DEFAULT_SERIES), help="BLS APU series IDs.")
    parser.add_argument("--start-year", type=int, default=default_start)
    parser.add_argument("--end-year", type=int, default=default_end)
    args = parser.parse_args(argv)

    common.load_environment()
    api_key = os.environ.get("BLS_API_KEY")  # optional
    tier = "v2 (keyed)" if api_key else "v1 (no key)"
    print(f"Ingesting BLS prices [{tier}] for {len(args.series)} series -> {common.raw_dir('prices', 'bls')}")
    ingest(args.series, args.start_year, args.end_year, api_key)
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
