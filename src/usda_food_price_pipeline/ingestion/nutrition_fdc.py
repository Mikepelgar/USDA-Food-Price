"""Phase 1 ingestion: USDA FoodData Central (nutrition).

Pulls food records from the FoodData Central ``/foods/search`` endpoint for a
small sample of common-food queries and saves each raw page response as
timestamped JSON under ``data/raw/nutrition/``. The search payload already
includes each food's ``description``, ``foodCategory`` and ``foodNutrients``
(protein, fiber, micronutrients, ...), so no per-food follow-up calls are
needed for the sample.

Run:
    python -m usda_food_price_pipeline.ingestion.nutrition_fdc

Needs ``FDC_API_KEY`` in ``.env``. Free api.data.gov key, capped at 1,000
requests/hour (HTTP 429 when exceeded) — this script rate-limits and retries.
"""

from __future__ import annotations

import argparse
import os
import sys

from . import common

FDC_BASE_URL = "https://api.nal.usda.gov/fdc/v1"

# A small, representative sample across the food groups — NOT the whole DB.
DEFAULT_QUERIES = [
    "cheddar cheese",
    "whole milk",
    "white bread",
    "eggs",
    "chicken breast",
    "ground beef",
    "white rice",
    "black beans",
    "apple",
    "banana",
    "broccoli",
    "spinach",
    "orange juice",
    "peanut butter",
    "salmon",
]

DEFAULT_PAGE_SIZE = 50
DEFAULT_MAX_PAGES = 2  # per query; keeps the sample modest


def build_search_params(
    query: str,
    api_key: str,
    page_number: int,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> dict:
    """Query-string params for a ``/foods/search`` request (1-indexed pages)."""
    return {
        "query": query,
        "pageSize": page_size,
        "pageNumber": page_number,
        "api_key": api_key,
    }


def search_filename(query: str, page_number: int, timestamp: str) -> str:
    """Raw-output filename for one search page."""
    return f"fdc_search_{common.slugify(query)}_p{page_number:02d}_{timestamp}.json"


def fetch_page(
    session,
    query: str,
    api_key: str,
    page_number: int,
    page_size: int,
) -> dict:
    """Fetch a single search page (with retries) and return parsed JSON."""
    params = build_search_params(query, api_key, page_number, page_size)
    response = common.retry_request(
        lambda: session.get(f"{FDC_BASE_URL}/foods/search", params=params, timeout=30)
    )
    response.raise_for_status()
    return response.json()


def ingest(
    api_key: str,
    queries=DEFAULT_QUERIES,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_pages: int = DEFAULT_MAX_PAGES,
) -> list:
    """Pull sample nutrition data and save each raw page. Returns saved paths."""
    out_dir = common.raw_dir("nutrition")
    session = common.make_session()
    limiter = common.RateLimiter(common.USDA_RATE_LIMIT_PER_HOUR, period=3600)
    saved = []

    for query in queries:
        for page_number in range(1, max_pages + 1):
            limiter.acquire()
            page = fetch_page(session, query, api_key, page_number, page_size)
            timestamp = common.utc_timestamp()
            path = out_dir / search_filename(query, page_number, timestamp)
            common.save_json(page, path)
            saved.append(path)
            print(f"  saved {path.name}  ({len(page.get('foods', []))} foods)")

            # Stop early once we've passed the last page of results.
            total_pages = page.get("totalPages")
            if isinstance(total_pages, int) and page_number >= total_pages:
                break

    return saved


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest USDA FoodData Central nutrition data (raw, local).")
    parser.add_argument("--queries", nargs="+", default=DEFAULT_QUERIES, help="Food search terms.")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE, help="Results per page.")
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES, help="Max pages per query.")
    args = parser.parse_args(argv)

    common.load_environment()
    api_key = os.environ.get("FDC_API_KEY")
    if not api_key:
        print("ERROR: FDC_API_KEY is not set (.env). See .env.example.", file=sys.stderr)
        return 1

    print(f"Ingesting nutrition for {len(args.queries)} queries -> {common.raw_dir('nutrition')}")
    saved = ingest(api_key, args.queries, args.page_size, args.max_pages)
    print(f"Done. Wrote {len(saved)} raw page file(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
