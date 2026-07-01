# Project Overview — USDA Food Price & Nutrition Pipeline

A single-page narrative of **what this project is, where its data comes from, every process it
runs, and what it produces.** For hands-on run instructions see the [README](../README.md); for
working-in-the-repo guidance see [CLAUDE.md](../CLAUDE.md).

---

## 1. Goal

Build an automated, **free-tier** data pipeline that answers a practical question — *how much
nutrition do you get per dollar of food, and where are food prices heading?* — by combining public
U.S. government data on food **prices** and **nutrition** into an analytics-ready warehouse, a live
dashboard, and a short-term price forecast.

It is a portfolio project, so a second goal is to demonstrate a realistic, end-to-end **modern data
stack** — ingestion → warehouse → transformation → orchestration → serving/ML → CI — running
entirely on infrastructure that cannot incur a bill (Google BigQuery **Sandbox**).

---

## 2. Data sources

| Source | What it provides | Access method | Coverage / notes |
| ------ | ---------------- | ------------- | ---------------- |
| **USDA FoodData Central (FDC)** | Nutrition — every reported nutrient per food (protein, fats, all vitamins & minerals, energy, …) | JSON REST API (`/foods/search`) with an `api.data.gov` key | ~221 distinct nutrients across a sample of 15 common foods, paginated |
| **USDA ERS F-MAP** (Food-at-Home Monthly Area Prices) | Historical retail food **prices** by category × geographic area | **File download** (`.xlsx`) — *not* an API | 90 food categories × 15 areas, monthly, **2012–2018 only** (static) |
| **BLS Average Price Data (APU)** | **Current** monthly retail food prices | JSON REST API (v2 with key, else v1) | 8 curated food series, U.S. **city-average** (no regional breakdown). **BLS is not USDA.** |
| **Google BigQuery** | Cloud data warehouse (storage + SQL compute) | Service-account JSON → OAuth token | Project `usda-food-prices`, location `US`, on the **Sandbox** (no billing) |

All three data sources are **public and free**. The two USDA keys are `api.data.gov` keys rate-limited
to 1,000 requests/hour; the BLS key is optional (raises the rate limit).

---

## 3. Processes (end-to-end pipeline)

The pipeline is six stages. Stages 1–4 run automatically as one daily Airflow DAG; stages 5–6 are
the serving/quality layers.

### 3.1 Ingestion — pull raw data to local files
*(`src/usda_food_price_pipeline/ingestion/`)*

Three Python scripts call the sources and write raw responses, unmodified, into a gitignored
`data/raw/` folder (one timestamped file per API page / download):

- `nutrition_fdc.py` — paginates the FDC search API over 15 common foods.
- `prices_fmap.py` — downloads the ERS F-MAP `.xlsx` file as-is (with an offline `--from-file` fallback).
- `prices_bls.py` — pulls the 8 BLS APU price series.

Shared plumbing in `common.py` handles retries (429/5xx back-off), a 1,000/hr rate limiter, timestamped
filenames, and loading secrets from `.env`. Nothing here touches the cloud — it just lands raw JSON/XLSX
locally, so the raw data is the reproducible source of truth.

### 3.2 Load — raw files → BigQuery raw tables
*(`src/.../load/bigquery_loader.py`)*

A loader reads those raw files and **batch-loads them essentially as-is** into the `usda_raw` dataset —
no cleaning, no joining (that is dbt's job). Three raw tables: `raw_nutrition` (one row per food),
`raw_prices_bls` (one row per observation), `raw_prices_fmap` (one row per worksheet row). Every load uses
`WRITE_TRUNCATE` in a single **batch** job, so it is **idempotent** (re-running replaces the table with no
duplicates) and **free** (no streaming inserts — Sandbox-safe). `--dry-run` parses without touching BigQuery.

### 3.3 Transform — clean, model, and test with dbt
*(`transform/` — a dbt project)*

dbt turns the raw tables into typed, tested, documented analytics tables in two layers:

- **Staging** (`usda_staging`, 4 views) — one view per source: cast types, standardize keys, de-duplicate.
- **Analytics** (`usda_analytics`, 4 tables):
  - `fct_fmap_prices` — monthly F-MAP price (USD/100 g) by category × region.
  - `fct_bls_prices` — current monthly BLS price per item (the forecast input).
  - `dim_nutrition` — **LONG**: the median amount per 100 g of **every** reported nutrient, one row per
    `(food_category, nutrient_number, unit)`, carrying `nutrient_name` and `unit` (G/MG/UG/KCAL).
  - `fct_nutrition_per_dollar` — the headline model: F-MAP price ⋈ (via a curated crosswalk) ⋈
    `dim_nutrition`, giving **every nutrient per dollar** (`amount_per_dollar = amount_per_100g /
    mean_unit_value`) per `(efpg_code, region_code, month_date, nutrient_number, unit)`, ranked per nutrient.

Two seeds support the join: `category_crosswalk.csv` (F-MAP price category → broad FDC nutrition category —
**intentionally lossy**) and `bls_series_items.csv`. `dbt build` runs seeds → models → **data tests** in
dependency order; tests assert not-null/unique keys, non-negative prices and amounts, and each table's grain.

### 3.4 Orchestration — run it daily with Airflow
*(`docker-compose.yml`, `docker/airflow/`, `dags/`)*

One Apache Airflow DAG (`usda_food_price_pipeline`, `@daily`, LocalExecutor) chains six tasks in a line:
`ingest_nutrition → ingest_bls → ingest_fmap → load_bigquery → dbt_run → dbt_test`, with retries and
timeouts. It runs locally in Docker (four containers: Postgres + one-shot init + scheduler + webserver).
Secrets are fed from `.env` and a read-only mount of the service-account key — never hardcoded in the DAG.
If a dbt test fails, the run is marked failed and stops rather than reporting a false success.

### 3.5 Serving — the dashboard
*(`dashboard/app.py`, Streamlit)*

A read-only Streamlit dashboard over `usda_analytics` with four tabs:

1. **F-MAP price trends** — price over time by category × region (historical 2012–2018).
2. **BLS inflation** — current prices + month-over-month change (U.S. city-average).
3. **Nutrition per dollar** — a **dropdown over all ~214 nutrients**; bars show the amount per dollar in
   that nutrient's own unit.
4. **Forecast** — next-month BLS price vs. actuals, with the accuracy metric.

BigQuery reads are cached (`st.cache_data`, 1-hour TTL); the nutrition tab loads one nutrient's slice at a
time so the (large) per-nutrient table is never pulled whole.

### 3.6 Forecasting — next-month price prediction
*(`src/.../forecast/bls_forecast.py`, scikit-learn)*

For each BLS series (~48 monthly points), a deliberately simple model — **AR(1) + month-seasonality**
(last month's price + sin/cos of the month, scaled → Ridge) — predicts next month's price. Accuracy is the
**MAPE of an expanding one-step-ahead backtest** over held-out months, reported against a last-value naive
baseline. Results are written one row per series to `usda_forecast.fct_bls_forecast` via a batch load.

### 3.7 Quality & CI — tests everywhere
*(`tests/`, `.github/workflows/ci.yml`)*

- **Unit tests** — `pytest`, fully mocked (no network, no cloud credentials) over the ingestion, loader,
  and forecast code.
- **dbt data tests** — run inside every `dbt build`.
- **GitHub Actions CI** — on every push/PR, two **credential-free** jobs: `python-tests` (the mocked pytest
  suite) and `dbt-validate` (`dbt deps` + `dbt parse` against a dummy profile, validating the project
  without opening a warehouse connection). No secrets are used in CI.

---

## 4. Results

Measured metrics come from real runs in this repo; run-dependent operational metrics are left as
clearly-labeled placeholders to fill in from your own environment.

| Metric | Value |
| ------ | ----- |
| Analytics rows — `fct_fmap_prices` | **113.4k** (category × region × month, 2012–2018) |
| Analytics rows — `fct_bls_prices` | **~319** (item × month, current) |
| Analytics rows — `dim_nutrition` (LONG) | **~6.8k** (food_category × nutrient) |
| Analytics rows — `fct_nutrition_per_dollar` | **~3.2M** (category × region × month × nutrient) |
| Nutrients surfaced (nutrition-per-dollar) | **214** distinct nutrient×unit series (of ~221 reported) |
| dbt data-quality tests | **47** (all passing) |
| Python unit tests | **41** (all passing, fully mocked) |
| Forecast accuracy (MAPE, held-out backtest) | ≈ **2.8%** vs. naive baseline ≈ **2.1%** |
| Total raw records processed | _(fill in — the loader prints `raw_nutrition` + `raw_prices_bls` + `raw_prices_fmap` counts)_ |
| Food categories × regions (F-MAP) | _(fill in — F-MAP covers 90 categories × 15 areas)_ |
| Pipeline run time (Airflow DAG) | _(fill in from a real DAG run)_ |
| Pipeline success rate | _(fill in — e.g. N / N successful daily runs)_ |

**What the results mean.** The pipeline reliably ingests, models, and tests three public food datasets and
exposes them interactively. The nutrition-per-dollar model lets you ask, for any nutrient, *which food
category gives the most of it per dollar* in a given region and month. The forecast is honestly modest — on
~48 points of near-random-walk data, beating a naive baseline by much is not expected, and the reported MAPE
(~2.8% vs ~2.1% naive) reflects that.

---

## 5. Data caveats (stated honestly)

- **F-MAP prices are historical (2012–2018)** — a static file, not a live feed.
- **BLS prices are current but national** — U.S. city-average, no regional breakdown.
- **BLS is not USDA** — the current/forecastable price feed is from the Bureau of Labor Statistics.
- **The nutrition↔price crosswalk is intentionally lossy** — broad FDC nutrition categories are mapped to
  granular F-MAP price categories through a small curated seed; several priced categories share one broad
  nutrition profile, and unmapped categories are dropped. Adding more nutrients enriches the menu but does
  **not** make the join more precise.
- **The forecast is small-data** — ~48 monthly points per series; accuracy close to naive is expected.

---

## 6. Tech stack & key libraries (by phase)

| Layer (phase) | Platform / tools | Notable Python libraries |
| ------------- | ---------------- | ------------------------ |
| Language & runtime | Python 3.11, `venv` | — |
| Ingestion (P1) | USDA/BLS public APIs + file download | `requests` (HTTP client + retries), `python-dotenv` (load secrets from `.env`) |
| Load (P2) | Google BigQuery batch loads | `google-cloud-bigquery` (client + `WRITE_TRUNCATE` batch jobs), **`openpyxl`** (parse the F-MAP `.xlsx` — BigQuery can't load Excel directly) |
| Warehouse | Google BigQuery (Sandbox / free tier; batch + query jobs only) | — |
| Transformation (P3) | dbt on BigQuery | `dbt-bigquery` (dbt-core + BigQuery adapter), **`dbt_utils`** (unique-combination / accepted-range tests) |
| Orchestration (P4) | Apache Airflow (LocalExecutor) on Docker Compose | `apache-airflow` (BashOperator DAG); ingestion/loader libs re-installed in the image under Airflow's constraints |
| Serving (P5) | Streamlit dashboard | `streamlit` (UI + caching), **`altair`** (charts), **`pandas`** (in-memory frames/filtering), **`db-dtypes`** (BigQuery NUMERIC/DATE → pandas) |
| Forecasting (P5) | scikit-learn | `scikit-learn` (`Ridge`, `StandardScaler`, `make_pipeline`), **`numpy`** (seasonality features + MAPE) |
| CI / testing (P6) | GitHub Actions | `pytest` (fully-mocked unit tests), dbt data tests, `dbt parse` (credential-free project validation) |

Bolded libraries are the ones easy to overlook but genuinely load-bearing: `openpyxl` (the only way
the F-MAP Excel file gets into the warehouse), `dbt_utils` (all the grain/range data tests),
`db-dtypes` (without it BigQuery results won't convert to pandas), and `numpy` (the forecast's
feature math and error metric).
