# CLAUDE.md

Guidance for Claude Code (and humans) working in this repository.

## Project goal

An automated, free-tier data pipeline that pulls US food **price** (USDA ERS F-MAP + BLS) and
**nutrition** (USDA FoodData Central) data from public APIs/files, loads it into a cloud data
warehouse (BigQuery), transforms it into analytics-ready tables (dbt), orchestrates the flow
(Airflow), and serves a **dashboard** + a **food-price forecast**. **Status: complete — all phases
(0–6) are built, run, tested, and merged to `main`.** For a narrative walkthrough of the sources,
every process, the goal, and the results, see [`docs/PROJECT_OVERVIEW.md`](docs/PROJECT_OVERVIEW.md).

## Architecture & tech stack

```
SOURCES                 INGESTION          WAREHOUSE           TRANSFORM            SERVE
───────                 ─────────          ─────────           ─────────            ─────
FoodData Central API ─┐  nutrition_fdc ─┐                      dbt staging views    Streamlit
(nutrition, JSON)     │  prices_fmap    ─┼─► data/raw/* ─load─► usda_raw ──dbt──► usda_analytics ─┬─► dashboard
ERS F-MAP file (.xlsx)├─ prices_bls     ─┘  (gitignored)       (raw tables)   (fct/dim tables)   │   (4 tabs)
BLS APU API (JSON) ───┘                                                                           │
                                                                       bls_forecast.py ──► usda_forecast ┘
        ORCHESTRATION: Airflow (LocalExecutor) in Docker runs ingest→load→dbt build→dbt test daily.
```

- **Language:** Python 3.11 (venv at `.venv/`). **Ingestion:** `requests`, `python-dotenv`.
- **Warehouse:** Google **BigQuery** (project `usda-food-prices`, location `US`), authenticated via
  the service-account JSON at `secrets/gcp-service-account.json` (`GOOGLE_APPLICATION_CREDENTIALS`).
- **Transform:** **dbt** (`dbt-bigquery` + `dbt_utils`). **Orchestration:** **Apache Airflow** via
  **Docker Compose**. **Serving:** **Streamlit** dashboard + a **scikit-learn** forecast.
- **CI:** **GitHub Actions** (`.github/workflows/ci.yml`). **Tests:** `pytest` (fully mocked) + dbt tests.
- Full deps in `requirements.txt` (phase-scoped + commented); extra Airflow libs in `requirements-airflow.txt`.

## What exists (by component)

**Ingestion — `src/usda_food_price_pipeline/ingestion/`** (run as modules with `PYTHONPATH=src`;
all writes go to gitignored `data/raw/`):
- `common.py` — shared helpers (unit-tested): `raw_dir`, `utc_timestamp`, `slugify`, `make_session`,
  `retry_request` (retries 429/5xx), `RateLimiter` (1000/hr), `save_json`, `load_environment`.
- `nutrition_fdc.py` — FoodData Central `/foods/search`; paginates 15 common foods; one raw page per
  file → `data/raw/nutrition/`. Needs `FDC_API_KEY`.
- `prices_fmap.py` — ERS F-MAP **file download** (no API/key), 2012–2018 XLSX, saved as-is →
  `data/raw/prices/fmap/`. `--from-file` offline fallback.
- `prices_bls.py` — BLS Average Price API (NOT USDA); 8 curated `APU…` food series → `data/raw/prices/bls/`.
  `BLS_API_KEY` optional (v2 vs v1).

**Load — `src/.../load/bigquery_loader.py`:** reads the raw files and **batch-loads them as-is**
(`WRITE_TRUNCATE`, idempotent, no streaming) into dataset **`usda_raw`** (created on first run):
`raw_nutrition` (one row/food), `raw_prices_bls` (one row/observation), `raw_prices_fmap` (one
row/worksheet-row, `.xlsx` via `openpyxl`). `--dry-run` parses without touching BigQuery; `--only` limits sources.

**Transform — dbt project at `transform/`** (profile `usda_food_prices`; run from `transform/` with
`--profiles-dir .`; real `profiles.yml` is gitignored, `profiles.example.yml` is the template; a
`macros/generate_schema_name.sql` override makes custom schemas verbatim). **4 staging views** in
`usda_staging` (typed/cleaned/de-duped) and **4 analytics tables** in `usda_analytics`:
- `fct_fmap_prices` — monthly F-MAP price by category × region (2012–2018); grain (efpg_code, region_code, month_date).
- `fct_bls_prices` — current monthly BLS price by item (the **forecast input**); grain (series_id, month_date).
- `dim_nutrition` — **LONG**: median amount per 100 g for **every** reported nutrient across non-Branded
  foods; grain **(food_category, nutrient_number, unit)**; carries `nutrient_name`, `unit`, `amount_per_100g`.
- `fct_nutrition_per_dollar` — **per-nutrient**: F-MAP price ⋈ `category_crosswalk` ⋈ `dim_nutrition`;
  `amount_per_dollar = amount_per_100g / mean_unit_value`; `nutrient_rank` per (region, month, nutrient);
  grain **(efpg_code, region_code, month_date, nutrient_number, unit)**; `cluster_by` nutrient_number.
  ~3.2M rows; ~214 nutrient×unit series reachable. CAVEAT: HISTORICAL prices + static nutrition.
- Seeds: `category_crosswalk.csv` (F-MAP EFPG code → broad FDC `foodCategory`; intentionally lossy) and
  `bls_series_items.csv`. **`dbt build` runs clean: 8 models, 2 seeds, 47 data tests — all PASS.**

**Orchestration — Airflow + Docker (`docker-compose.yml`, `docker/airflow/`, `dags/`):** one DAG
`usda_food_price_pipeline` (`@daily`, LocalExecutor, four containers: postgres + one-shot init +
scheduler + webserver) runs six BashOperator tasks in a line:
`ingest_nutrition → ingest_bls → ingest_fmap → load_bigquery → dbt_run → dbt_test`, with retries +
timeouts. F-MAP `ingest_fmap` skips (exit 99) when the static file exists; ingest tasks `rm -f` their
raw folder so each run loads one fresh snapshot. Secrets come from `.env`/mounted `secrets/`, never the
DAG. **The image bakes the dbt project** (`COPY transform/` + `dbt deps` at build), so editing models
needs `docker compose up -d --build`.

**Serve — `dashboard/app.py` (Streamlit) + `src/.../forecast/bls_forecast.py`:** read-only over
`usda_analytics`. The dashboard has 4 tabs (F-MAP trends, BLS inflation, **Nutrition-per-dollar with a
nutrient dropdown** over all ~214 nutrients via per-nutrient cached queries, Forecast); BigQuery reads are
`st.cache_data`-cached (1 hr). The forecast fits a per-series **AR(1) + month-seasonality** Ridge model,
reports **MAPE of an expanding one-step backtest** vs a naive baseline (overall ≈ **2.8%** vs ≈ **2.1%**),
and writes one row/series to **`usda_forecast.fct_bls_forecast`** (`WRITE_TRUNCATE` batch load).

**CI — `.github/workflows/ci.yml`:** on push/PR, two credential-free jobs — `python-tests`
(`pip install -r requirements.txt` → `pytest`, fully mocked) and `dbt-validate` (`dbt deps` + `dbt parse`
against the committed dummy profile `.github/dbt/profiles.yml`, no warehouse connection). No secrets in CI.

## Conventions

- **Secrets:** real secrets in `.env` (gitignored); `.env.example` is the template. Service-account
  JSON in `secrets/` (gitignored except `.gitkeep`). Verified via `git ls-files` that only `.env.example`
  + `secrets/.gitkeep` are tracked — never commit credentials. Env vars: `FDC_API_KEY`, `BLS_API_KEY`
  (optional), `GOOGLE_APPLICATION_CREDENTIALS`, `ERS_API_KEY` (validated, unused), and Phase-4 `AIRFLOW_*`.
- **Layout (src layout):** `src/usda_food_price_pipeline/` (package: `ingestion/`, `load/`, `forecast/`);
  `transform/` (dbt); `dashboard/`; `dags/`; `docker/`; `config/`; `tests/`; `docs/`.
- **Naming:** `snake_case` modules/functions; tests in `tests/` as `test_*.py`; repo/distribution name
  hyphenated (`usda-food-price-pipeline`), package underscored. Keep `requirements.txt` minimal + commented.
- **Tests:** `python -m pytest` (41 tests, fully mocked — no network/keys; `pyproject.toml` sets
  `pythonpath=["src"]`). dbt tests run inside `dbt build`.
- **End-of-session update (REQUIRED):** at the end of **every** session, update this `CLAUDE.md` to reflect
  what changed (keep "Current state" accurate; convert relative dates to absolute), then **commit and push
  to `main`**. A docs-only `CLAUDE.md` update may go directly to `main`; reserve branch + PR for code. End
  commit messages with the `Co-Authored-By: Claude …` trailer.

## API reference (verified working)

- **FoodData Central (nutrition)** — base `https://api.nal.usda.gov/fdc/v1`; used:
  `GET /foods/search?query=<term>&pageSize=<n>&api_key=$FDC_API_KEY`. Docs: https://fdc.nal.usda.gov/api-guide.html
- **USDA ERS F-MAP** — **file download, NOT an API** (2012–2018; 90 categories × 15 areas, monthly).
  Page: https://www.ers.usda.gov/data-products/food-at-home-monthly-area-prices
- **BLS Average Price (APU)** — JSON API (`.../publicAPI/v2/timeseries/data/` with key, else v1). The
  live/forecastable feed. NOTE: **BLS is not USDA.** Docs: https://www.bls.gov/developers/
- **BigQuery** — project `usda-food-prices`; OAuth token minted from the service-account JSON.
- Both USDA keys are api.data.gov keys: 1,000 requests/hour (HTTP 429 when exceeded).

## Current state

**All phases (0–6) are COMPLETE and merged to `main`** (Phase 3 PR #3, Phase 4 PR #4, Phase 5 PR #5;
Phase 6 PR #6, squash-merged 2026-07-01). BigQuery holds: `usda_raw` (raw), `usda_staging`/`usda_analytics` (dbt
models), `usda_forecast` (forecast). Phase 6 added: the **all-nutrients** nutrition-per-dollar feature
(LONG `dim_nutrition`; per-nutrient `fct_nutrition_per_dollar`, new grains, ~214 nutrients, dbt tests
47/47 PASS, dashboard nutrient dropdown — validated headless against live BigQuery), **GitHub Actions CI**,
a portfolio README (with a **Results** section whose run-dependent metrics are left as fill-in
placeholders), and a repo review (no tracked secrets, no stale refs).

**Airflow-rebuild note:** the running DAG won't pick up the Phase-6 dbt model changes until the image is
rebuilt (`docker compose up -d --build`) — the dbt project is baked into the image. Local `dbt build`
already validated the new models; re-running the stack was out of scope for Phase 6.

**Billing / Sandbox (CONFIRMED 2026-06-23):** no billing account linked → **BigQuery Sandbox** (cannot be
charged). Caveat: Sandbox auto-expires tables ~60 days after creation (re-run ingestion + loader + dbt to
recreate; `data/raw/` is the source of truth) and forbids streaming inserts (we only batch-load + query).

**Data caveats (honest):** F-MAP prices are historical 2012–2018; BLS prices are current but U.S.
city-average (no regional breakdown) and **BLS is not USDA**; the FDC↔F-MAP nutrition crosswalk is
intentionally lossy (broad FDC categories vs granular F-MAP); the forecast is small-data (~48 points/series,
near-random-walk → close to naive expected).

**Known benign warning:** `dbt build`/`dbt parse` emit a `MissingArgumentsPropertyInGenericTestDeprecation`
(dbt ≥1.10 wants `dbt_utils.accepted_range` args nested under `arguments:`). Pre-existing across all
`accepted_range` tests; a non-blocking forward-compat warning, not an error — optional future cleanup.

## Possible future work (backlog, not started)

- Loader `--latest-only` flag so snapshot de-duping happens in the loader instead of the DAG `rm -f` step.
- Wire the forecast into the Airflow DAG as a final task so `usda_forecast` refreshes daily.
- Add `google-cloud-bigquery-storage` to silence the dashboard's REST-fallback warning.
- Migrate the `accepted_range` tests to the new `arguments:` syntax (clears the deprecation warning).
