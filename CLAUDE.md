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

**Phase 3 (transformation — dbt on BigQuery) — COMPLETE 2026-06-27; `dbt build` ran clean
(8 models, 2 seeds, 46 tests — all PASS) and the work is committed on a Phase-3 branch.** dbt project at
**`transform/`** (profile `usda_food_prices`; `dbt-bigquery` added to `requirements.txt`,
`dbt_utils` in `packages.yml`). Connects to BigQuery via `method: service-account` reusing
`secrets/gcp-service-account.json`; the real `profiles.yml` is gitignored and
`transform/profiles.example.yml` is the committed template (run dbt from `transform/` with
`--profiles-dir .`). A `macros/generate_schema_name.sql` override makes custom schemas verbatim,
so models land in datasets **`usda_staging`** (views) and **`usda_analytics`** (tables); dbt
creates both on first run (Sandbox 60-day expiry applies).

- **Source:** the three `usda_raw.*` tables (declared in `models/staging/_sources.yml`).
- **Staging (views, 4)** — typed/cleaned/de-duped (`SAFE_CAST`; `LOWER`/`TRIM` + whitespace
  collapse; deterministic `ROW_NUMBER` newest-wins on `loaded_at`):
  - `stg_nutrition` — one row per `fdc_id`; standardized `food_category`; `foodNutrients` kept as JSON.
  - `stg_prices_bls` — one row per (series_id, month); monthly only (`M01`–`M12`); `price_usd` numeric; item label via seed.
  - `stg_prices_fmap` — F-MAP **main** workbook (2012–2018); `mean_unit_value` (USD/100 g, = ERS `Unit_value_mean_wtd`) + `price_index_geks`.
  - `stg_fmap_price_index` — F-MAP **supplemental** workbook (2016–2018); alternative index methods only (Laspeyres/Paasche/Törnqvist/Fisher/GEKS/CCD).
- **Analytics (tables, 4):**
  - `fct_fmap_prices` — monthly price by category × region; main LEFT JOIN supplemental indexes; grain **(efpg_code, region_code, month_date)**.
  - `fct_bls_prices` — current monthly price series by item (the **Phase-5 forecast input**); grain (series_id, month_date).
  - `dim_nutrition` — nutrition per FDC `food_category` (per 100 g), **median** across non-Branded foods (Foundation/SR Legacy/Survey); grain (food_category).
  - **`fct_nutrition_per_dollar`** — the combined model. Joins `fct_fmap_prices` → `category_crosswalk` (on `efpg_code`) → `dim_nutrition` (on `food_category`); computes `nutrient_g_per_100g / mean_unit_value` = grams per dollar; `protein_rank` window over (region, month). **Grain (efpg_code, region_code, month_date).** CAVEAT in its docs: HISTORICAL 2012–2018 prices + static nutrition (NOT current — `fct_bls_prices` is the current feed).
- **Seeds:** `category_crosswalk.csv` (~20 rows; **F-MAP EFPG code → broad FDC `foodCategory`**;
  intentionally lossy — FDC categories are broad while F-MAP is granular, so several priced
  categories share one nutrition profile; only the food-basket overlap is mapped; every target is
  a non-Branded category present in `dim_nutrition`) and `bls_series_items.csv` (8 series → label/unit,
  mirrors `DEFAULT_SERIES`).
- **Tests** (`_staging.yml`/`_analytics.yml`/`_seeds.yml`): not_null + unique on keys;
  `dbt_utils.unique_combination_of_columns` on the F-MAP **(efpg_code, region_code, month_date)**
  grain (also BLS + per-dollar grains); `dbt_utils.accepted_range(min_value:0)` on every
  price/per-dollar column + plausible nutrient bounds; safe `relationships`
  (`stg_prices_bls.series_id`→seed, `fct_nutrition_per_dollar.efpg_code`→crosswalk). Model +
  column docs on all four analytics tables for `dbt docs`.
- **Step 0 introspection (recorded):** F-MAP main `Data` sheet already carries the price
  (`Unit_value_mean_wtd`) **and** `Price_index_GEKS` for all years (the supplemental file only
  adds extra index methods, 2016–2018); 90 EFPG categories × 15 regions; nutrition data types
  Branded 513 / Survey 502 / SR Legacy 442 / Foundation 34 / Experimental 9 (Branded+Experimental
  excluded from nutrition); nutrientNumbers 203/204/205/208/291/301/303/307 confirmed present.

## Current state

**Phases 0–3 are COMPLETE.** Phase 3 (dbt) was built, run, and validated on 2026-06-27:
`dbt build` from `transform/` created datasets `usda_staging` (4 staging views) and
`usda_analytics` (4 analytics tables + 2 seeds) and **all 46 tests passed** (56 nodes, 0 errors).
The work is committed on a Phase-3 branch (push/PR pending). BigQuery `usda_raw` still holds the
Phase-2 raw tables. No orchestration or dashboard code exists yet.

**Venv note:** `google-auth` was installed into `.venv` ad hoc for the Phase-0 credential
check (still not pinned). Phase 2 added `google-cloud-bigquery` + `openpyxl`; Phase 3 added
`dbt-bigquery` (pulls in `dbt-core`) to `requirements.txt` — always `pip install -r
requirements.txt` in the activated `.venv` first, then `dbt deps` (installs `dbt_utils`).

**Billing note (CONFIRMED 2026-06-23):** the user has **no billing account linked**, so the
project runs in **BigQuery Sandbox = cannot be charged**. Caveat: Sandbox auto-expires every
table ~60 days after creation (re-run ingestion + loader to recreate; raw files in
`data/raw/` are the source of truth) and forbids streaming inserts (we only batch-load, so
fine). USDA APIs are free, capped at 1,000 requests/hour per key (HTTP 429 when exceeded).

## Next: Phase 4 — Orchestration (Airflow + Docker)

Phase 3 is built, validated, and committed (Phase-3 branch; PR pending). Start a **fresh session
for Phase 4**: one Airflow DAG (run locally via Docker Compose) that runs
ingest → load → `dbt build` → `dbt test` on a daily schedule with retries. Do not write Phase 4
code until that phase is explicitly started.

**What Phase 4 will wrap:** `dbt build` executed from `transform/` with the service-account
profile (`transform/profiles.example.yml` → real `profiles.yml`). Keep everything on the free
tier / Sandbox (batch + query jobs only; no streaming). The forecast/dashboard remain Phase 5.
