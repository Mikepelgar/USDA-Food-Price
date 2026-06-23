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
- **End-of-session update (REQUIRED):** at the end of **every** session, before wrapping up,
  update this `CLAUDE.md` so it reflects what changed — keep "What exists" and especially
  "Current state" / "Next" accurate (convert relative dates to absolute) — then **commit and
  push it to `main`** so the copy on GitHub matches the local file. A docs-only update to
  `CLAUDE.md` may be committed directly to `main` (no branch/PR needed); reserve the
  branch + PR workflow for phase code. End the commit message with the
  `Co-Authored-By: Claude …` trailer per the repo's git convention.

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

**Phase 2 (load raw files → BigQuery) — COMPLETE and run (rows landed 2026-06-23).**
Reads the Phase-1 raw files and **batch-loads them essentially as-is** (no cleaning, casting,
or joining — that's Phase 3/dbt). Run as a module with `PYTHONPATH=src`. Added deps:
`google-cloud-bigquery` + `openpyxl` (F-MAP is `.xlsx`; BigQuery can't load `.xlsx` directly).

- **`src/usda_food_price_pipeline/load/bigquery_loader.py`** — the loader.
  - **Dataset:** `usda_raw` in project `usda-food-prices` (location `US`); created on first
    run if missing (`ensure_dataset`). Overridable via `--dataset`/`BIGQUERY_DATASET`,
    `--location`/`BIGQUERY_LOCATION`, project via `BIGQUERY_PROJECT` (else inferred from the
    service-account credentials). Auth via `GOOGLE_APPLICATION_CREDENTIALS`.
  - **Three raw tables** (explicit schemas, autodetect off):
    - `raw_nutrition` ← `data/raw/nutrition/*.json`: one row per food (the `foods` array);
      cols `source_file, fdc_id, raw_json (full food verbatim), loaded_at`.
    - `raw_prices_bls` ← `data/raw/prices/bls/*.json`: flattens `Results.series[].data[]`,
      one row per observation; cols `source_file, series_id, year, period, period_name,
      value, latest, footnotes (JSON str), loaded_at` (values kept as API strings).
    - `raw_prices_fmap` ← `data/raw/prices/fmap/*.xlsx`: every sheet, header row → a
      `{header: cell}` JSON record; cols `source_file, sheet_name, row_index, raw_json,
      loaded_at`. Reads `.xlsx` with `openpyxl` (read-only, `data_only`).
  - **Idempotency:** each table is loaded in a single batch load job with `WRITE_TRUNCATE`
    (free; never streaming inserts), so re-running fully replaces the table — no duplicates.
  - **Verification:** after loading, prints row counts per table via `get_table().num_rows`
    (table metadata, not a billed query). `--dry-run` parses files + prints counts WITHOUT
    touching BigQuery (handy to confirm parsing before any cloud call); `--only` limits sources.
  - Pure row builders (`nutrition_rows_from_page`, `bls_rows_from_response`,
    `fmap_rows_from_sheet`) are unit-tested with no network/BigQuery; the BigQuery client is
    imported lazily. `tests/test_bigquery_loader.py` adds 8 tests (35 total, all pass).
  - **Run + verified for real (2026-06-23):** the user ran the loader; it created dataset
    `usda_raw` and landed `raw_nutrition` 1,500 (30 files × 50), `raw_prices_bls` 327
    (1 file), `raw_prices_fmap` 162,262 (2 files, incl. both `ReadMe` + `Data` sheets) —
    confirmed in the BigQuery console. Merged to `main` via PR #2.

## Current state

**Phases 0–2 are COMPLETE and on `main`.** As of 2026-06-23 the loader has been **run for
real**: BigQuery dataset `usda_raw` exists in project `usda-food-prices` with three populated
raw tables — `raw_nutrition` (1,500 rows), `raw_prices_bls` (327), `raw_prices_fmap`
(162,262) — verified in the console. Phase 2 merged via PR #2. No transformation,
orchestration, or dashboard code exists yet.

**Venv note:** `google-auth` was installed into `.venv` ad hoc for the Phase-0 credential
check (still not pinned). Phase 2 added `google-cloud-bigquery` + `openpyxl` to
`requirements.txt` — always `pip install -r requirements.txt` in the activated `.venv` first.

**Billing note (CONFIRMED 2026-06-23):** the user has **no billing account linked**, so the
project runs in **BigQuery Sandbox = cannot be charged**. Caveat: Sandbox auto-expires every
table ~60 days after creation (re-run ingestion + loader to recreate; raw files in
`data/raw/` are the source of truth) and forbids streaming inserts (we only batch-load, so
fine). USDA APIs are free, capped at 1,000 requests/hour per key (HTTP 429 when exceeded).

## Next: Phase 3 — Transformation (dbt on BigQuery)

Build staging + analytics models on top of the `usda_raw.*` tables, with dbt tests + docs.
No cleaning/casting/joining was done in Phase 2 by design — it ALL belongs here. Start a
fresh session focused only on Phase 3.

**Setup the next session will need:**
- Add the dbt adapter to `requirements.txt` (phase-scoped): `dbt-bigquery` (pulls in `dbt-core`).
- Create a dbt project (e.g. under `transform/` or `dbt/`). Configure `profiles.yml` to use
  BigQuery with `method: service-account` and `keyfile` = the same
  `secrets/gcp-service-account.json` (or `oauth`); project `usda-food-prices`, location `US`.
  Use dbt target datasets like `usda_staging` / `usda_analytics` (dbt creates them; in Sandbox
  they get the 60-day expiry too). Keep secrets out of git.
- Source = the three `usda_raw` tables. The payloads are JSON strings — parse with BigQuery
  `JSON_VALUE` / `PARSE_JSON` / `JSON_QUERY` (or dbt macros).

**Source shapes to parse (already verified):**
- `raw_nutrition.raw_json` = one full FDC food object: `description`, `dataType`
  (Branded / Foundation / SR Legacy / Survey), `foodCategory`, and `foodNutrients[]` (each has
  `nutrientName`/`nutrientNumber`, `value`, `unitName`). `fdc_id` is promoted to its own column.
- `raw_prices_bls`: already flat-ish — `series_id` (maps to a food via `DEFAULT_SERIES` in
  `ingestion/prices_bls.py`), `year`, `period` (`M01`–`M12` monthly, `M13` = annual avg),
  `value` (price string — cast to numeric), `latest`.
- `raw_prices_fmap`: filter to `sheet_name = 'Data'` (drop `ReadMe`). Parse `raw_json` keys
  `Year, Month, EFPG_name, EFPG_code, Metroregion_name, Metroregion_code,
  Purchase_dollars_wtd, Purchase_grams_wtd` (price ≈ dollars / grams). The supplemental file
  adds a few extra index columns; the core columns above are shared by both files.

**Deliverables:** staging models (one per source, typed/cleaned), analytics models incl. the
nutrition-per-dollar join, dbt tests (not_null/unique/relationships) + `dbt docs`. Build one
phase at a time — do not start Phase 4 in the same session.
