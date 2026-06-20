# CLAUDE.md

Guidance for Claude Code (and humans) working in this repository.

## Project goal

An automated data pipeline that pulls US food **price** (USDA ERS via api.data.gov) and
**nutrition** (USDA FoodData Central) data from public USDA APIs and loads it into a cloud
data warehouse. The warehouse data is transformed into analytics-ready tables that power a
**dashboard** and a **food-price forecast**.

## Planned architecture & tech stack

```
USDA APIs            Ingestion          Warehouse         Transform         Serve
─────────            ─────────          ─────────         ─────────         ─────
FoodData Central ─┐
(nutrition)       ├─► Python scripts ─► raw tables ────► cleaned/ ───────► Dashboard
ERS via           │   (requests)        (BigQuery)       modeled tables    + Price forecast
api.data.gov ─────┘                                      (SQL)
```

- **Language:** Python 3.11 (venv at `.venv/`).
- **Ingestion:** `requests` for HTTP; `python-dotenv` for config; `pytest` for tests.
  Only these three dependencies exist today — add more per phase, not preemptively.
- **Warehouse:** Google Cloud **BigQuery** (permanent free tier; authenticated via the
  service-account JSON referenced by `GOOGLE_APPLICATION_CREDENTIALS`).
- **Transformation:** **dbt** on BigQuery.
- **Orchestration:** **Apache Airflow**, run locally via **Docker** (Docker Compose).
- **CI:** **GitHub Actions**.
- **Serving:** **Streamlit** dashboard + a price forecast using **scikit-learn** (or a
  simple statistical time-series model).

## Phase plan

0. **Phase 0 — Scaffold (DONE):** repo structure, venv, env templates, git + GitHub.
1. **Phase 1 — Ingestion → LOCAL FILES ONLY (DONE):** Python scripts pull nutrition
   (FoodData Central API) and prices (ERS F-MAP file download + BLS API) and save raw output
   to `data/raw/nutrition/`, `data/raw/prices/fmap/`, and `data/raw/prices/bls/`. `data/` is
   gitignored. **No cloud, warehouse, orchestration, or dashboard code in this phase.**
2. **Phase 2 — Load to BigQuery:** a loader reads the raw JSON files and loads them as-is
   into raw/staging tables in BigQuery (idempotent re-runs). No transformation yet.
3. **Phase 3 — Transformation (dbt on BigQuery):** staging + analytics models, including the
   nutrition-per-dollar join; dbt tests and docs.
4. **Phase 4 — Orchestration (Airflow + Docker):** one DAG runs ingest → load → `dbt build`
   → `dbt test` on a daily schedule, with retries; containerized via Docker Compose.
5. **Phase 5 — Dashboard + forecast (Streamlit + scikit-learn):** dashboard over the
   analytics tables; next-month price forecast written back to BigQuery.
6. **Phase 6 — Polish, CI, portfolio:** GitHub Actions CI, portfolio-quality README, repo
   cleanup.

Build phases one at a time. Do not write code for a later phase until that phase is
explicitly started — keep each session focused on its single phase.

## Conventions

- **Secrets:**
  - Real secrets live in `.env` (gitignored). `.env.example` is the committed template.
  - The Google Cloud service-account JSON lives in `secrets/` (gitignored except
    `.gitkeep`). Never commit credentials.
  - Env vars in `.env` (all set as of 2026-06-20): `FDC_API_KEY` (nutrition API),
    `BLS_API_KEY` (BLS price API; optional but set — raises the rate limit),
    `GOOGLE_APPLICATION_CREDENTIALS` (BigQuery, used Phase 2+), and `ERS_API_KEY`
    (validated but unused so far — kept for possible future ERS API use).
  - `.env.example` documents `FDC_API_KEY`, `ERS_API_KEY`, `BLS_API_KEY` (added Phase 1),
    and `GOOGLE_APPLICATION_CREDENTIALS`.
- **Project layout (src layout):**
  - `src/usda_food_price_pipeline/` — the importable Python package.
  - `src/usda_food_price_pipeline/ingestion/` — ingestion scripts.
  - `config/` — non-secret configuration. `tests/` — pytest tests. `docs/` — documentation.
- **Naming:** package/module/function names are `snake_case`; directories `snake_case`.
  Tests live in `tests/` as `test_*.py`. The distribution/repo name uses hyphens
  (`usda-food-price-pipeline`); the importable package uses underscores
  (`usda_food_price_pipeline`).
- **Dependencies:** keep `requirements.txt` minimal and phase-scoped; document why each is
  added.

## API reference (endpoints verified working in Phase 0)

These exact calls returned HTTP 200 with the project's real keys on 2026-06-20.

- **USDA FoodData Central (nutrition)** — base `https://api.nal.usda.gov/fdc/v1`
  - Verified: `GET /foods/search?query=<term>&pageSize=<n>&api_key=$FDC_API_KEY`
  - Other documented endpoints: `/food/{fdcId}`, `/foods`, `/foods/list`.
  - Docs: https://fdc.nal.usda.gov/api-guide.html
Price data comes from TWO sources (decided 2026-06-20 — the ERS F-MAP dataset has no API):

- **USDA ERS Food-at-Home Monthly Area Prices (F-MAP)** — **file download, NOT an API.**
  Covers 2012–2018; 90 food categories × 15 geographic areas, monthly. In Phase 1, download
  the raw data file(s) as-is and save to `data/raw/prices/fmap/`; do NOT parse them yet.
  No API key needed.
  - Page: https://www.ers.usda.gov/data-products/food-at-home-monthly-area-prices
- **BLS average retail food prices** — **real JSON API** (current/ongoing monthly "APU"
  Average Price Data series). This is the live, forecastable price feed. Optional free key
  `BLS_API_KEY` raises the daily limit; add it to `.env`/`.env.example` in Phase 1. Save raw
  responses to `data/raw/prices/bls/`. NOTE: BLS is not USDA.
  - API docs: https://www.bls.gov/developers/ · register: https://data.bls.gov/registrationEngine/
- **USDA ERS ARMS API** (validated, not currently a project source): base
  `https://api.ers.usda.gov/data/arms` (`GET /year` → 200 in Phase 0). `ERS_API_KEY` is an
  api.data.gov key kept for possible future ERS API use; F-MAP does not need it.
- **BigQuery REST** — `https://bigquery.googleapis.com/bigquery/v2/projects/usda-food-prices/...`
  - Auth: mint an OAuth token from `secrets/gcp-service-account.json` (scope
    `https://www.googleapis.com/auth/bigquery`). A dry-run query job returned 200.
- Both USDA keys are api.data.gov keys: rate-limited to 1,000 requests/hour (HTTP 429).

## What exists

**Phase 0 (scaffold) — COMPLETE (2026-06-20):** repo structure, venv, ingestion
`requirements.txt`, env templates, `.gitignore`, README, this file. Pushed to GitHub:
https://github.com/Mikepelgar/USDA-Food-Price (branch `main`; repo-local commit email
`Mikepelgar@users.noreply.github.com`). All credentials verified to authenticate
(2026-06-20): `FDC_API_KEY` (FDC 200 OK), `ERS_API_KEY` (ERS ARMS 200 OK), GCP
service-account JSON at `secrets/gcp-service-account.json` (BigQuery token + dry-run 200 OK;
project `usda-food-prices`).

**Phase 1 (ingestion → local raw files) — COMPLETE.** All ingestion writes RAW responses to
`data/raw/` only (gitignored); no cloud/warehouse code. Run scripts as modules with
`PYTHONPATH=src` (e.g. `python -m usda_food_price_pipeline.ingestion.nutrition_fdc`).

- **`src/usda_food_price_pipeline/ingestion/common.py`** — shared helpers (network-free
  pieces are unit-tested): `raw_dir(*parts)` (creates/returns `data/raw/...`),
  `utc_timestamp()`, `slugify()`, `make_session()`, `backoff_delay()`,
  `retry_request(do_request, ...)` (retries 429/5xx + connection errors/timeouts, injectable
  `sleep`), `RateLimiter` (sliding window; `USDA_RATE_LIMIT_PER_HOUR = 1000`), `save_json()`,
  `load_environment()` (loads repo-root `.env`).
- **`ingestion/nutrition_fdc.py`** — FoodData Central. Reads `FDC_API_KEY`. Paginates
  `GET /foods/search` (base `https://api.nal.usda.gov/fdc/v1`) over `DEFAULT_QUERIES` (15
  common foods), `DEFAULT_PAGE_SIZE=50`, `DEFAULT_MAX_PAGES=2` per query; rate-limited +
  retried. The search payload already carries `description`, `foodCategory`, `foodNutrients`.
  Saves **one raw page per file** to `data/raw/nutrition/` as
  `fdc_search_<query-slug>_p<NN>_<timestamp>.json`. CLI: `--queries --page-size --max-pages`.
- **`ingestion/prices_fmap.py`** — ERS F-MAP **file download (no API/key); not parsed.**
  Default downloads the verified 2012–2018 XLSX + supplemental-indexes XLSX (URLs in
  `FMAP_DOWNLOAD_URLS`, HEAD-checked 200, main file = 12,532,112 bytes = the manual
  `~/Downloads/FMAP.xlsx`). Saves raw to `data/raw/prices/fmap/` as
  `<timestamp>_<source-basename>.xlsx`. Offline fallback: `--from-file <path>` copies a local
  file instead. CLI: `--from-file --url`.
- **`ingestion/prices_bls.py`** — BLS Average Price API (NOT USDA). `BLS_API_KEY` optional:
  present → v2 endpoint (`.../publicAPI/v2/timeseries/data/`), absent → v1. POSTs
  `DEFAULT_SERIES` (8 curated `APU0000…` U.S. city-average food series — eggs, milk, bread,
  flour, ground beef, chicken breast, bananas, white potatoes — titles verified against the
  BLS catalog 2026-06-20) for the last 4 years; retried. Saves the raw response to
  `data/raw/prices/bls/` as `bls_ap_<timestamp>.json`. CLI: `--series --start-year --end-year`.
- **Tests:** `tests/test_common.py`, `test_nutrition_fdc.py`, `test_prices_fmap.py`,
  `test_prices_bls.py` — 27 tests, all HTTP mocked (`unittest.mock`), no network/keys.
  `pyproject.toml` sets `[tool.pytest.ini_options] pythonpath=["src"]`, so `python -m pytest`
  works without installing the package. **No deps added** beyond `requests`/`python-dotenv`/
  `pytest`. `.env.example` now includes `BLS_API_KEY`; `data/` is gitignored.

## Current state

**Phase 1 is done; the user runs the scripts to confirm real data lands in
`data/raw/{nutrition,prices/fmap,prices/bls}/`.** No transformation, warehouse, orchestration,
or dashboard code exists yet.

**Venv note:** `google-auth` was installed into `.venv` ad hoc for the Phase-0 credential
check but is intentionally NOT in `requirements.txt`. In Phase 2, add the real warehouse
client (likely `google-cloud-bigquery`) to `requirements.txt` properly.

**Billing note:** the user wants to stay on the **free tier only**. Confirm BigQuery
billing posture before heavy use (Sandbox = no billing account = cannot be charged;
otherwise rely on free tier + budget alerts). USDA APIs are free, capped at 1,000
requests/hour per key (HTTP 429 when exceeded).

**Next: Phase 2 — Load to BigQuery.** A loader reads the raw JSON/XLSX files from `data/raw/`
and loads them as-is into raw/staging tables in BigQuery (project `usda-food-prices`,
authenticated via `GOOGLE_APPLICATION_CREDENTIALS`), with idempotent re-runs. No
transformation yet (that's Phase 3 / dbt). Add `google-cloud-bigquery` to `requirements.txt`.
Stay within the free tier.
