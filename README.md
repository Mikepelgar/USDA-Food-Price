# USDA Food Price & Nutrition Pipeline

## Goal

An automated data pipeline that pulls US food price and nutrition data from public
USDA APIs and loads it into a cloud data warehouse. The warehouse data is transformed
into analytics-ready tables that power a dashboard and a food-price forecast.

## Planned Architecture (high level)

```
USDA APIs            Ingestion          Warehouse         Transform         Serve
─────────            ─────────          ─────────         ─────────         ─────
FoodData Central ─┐
(nutrition)       ├─► Python scripts ─► raw tables ────► cleaned/ ───────► Dashboard
ERS via           │   (requests)        (BigQuery)       modeled tables    + Price forecast
api.data.gov ─────┘                                      (SQL)
(prices)
```

1. **Ingestion** — Python scripts call the USDA FoodData Central and ERS APIs and land
   raw responses in the cloud warehouse.
2. **Warehouse** — Google Cloud (BigQuery) holds raw and transformed data.
3. **Transformation** — SQL transforms raw data into clean, modeled tables.
4. **Serving** — a dashboard visualizes trends; a forecasting model projects food prices.

> Status: Phase 1 (ingestion → local raw files) and Phase 2 (load raw files → BigQuery)
> are built. Transformation (dbt), orchestration, and dashboard code come in later phases.

## Setup

```bash
# Create and activate the virtual environment (Windows PowerShell)
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Install ingestion-phase dependencies
pip install -r requirements.txt

# Configure secrets
copy .env.example .env   # then fill in your keys
```

## Phase 1 — Ingestion (raw local files)

Three scripts pull raw data and write it under `data/raw/` (gitignored — nothing is
committed). Run them as modules; on Windows PowerShell set `PYTHONPATH` first so the
`src/`-layout package is importable:

```powershell
$env:PYTHONPATH = "src"     # bash: export PYTHONPATH=src
```

| Script | Source | Needs | Output |
| ------ | ------ | ----- | ------ |
| `nutrition_fdc` | FoodData Central API (`/foods/search`) | `FDC_API_KEY` | `data/raw/nutrition/fdc_search_<query>_p<NN>_<timestamp>.json` |
| `prices_fmap` | ERS F-MAP (file download, no API) | none | `data/raw/prices/fmap/<timestamp>_<filename>.xlsx` |
| `prices_bls` | BLS Average Price API (APU series) | `BLS_API_KEY` (optional) | `data/raw/prices/bls/bls_ap_<timestamp>.json` |

```powershell
# 1. Nutrition — paginates a sample of food queries, rate-limited + retried
python -m usda_food_price_pipeline.ingestion.nutrition_fdc
#    options: --queries "apple" "milk"  --page-size 50  --max-pages 2

# 2. Prices, source A — ERS F-MAP raw file(s) (downloaded as-is, not parsed)
python -m usda_food_price_pipeline.ingestion.prices_fmap
#    offline fallback: copy a file you already downloaded
python -m usda_food_price_pipeline.ingestion.prices_fmap --from-file $HOME\Downloads\FMAP.xlsx

# 3. Prices, source B — BLS retail food prices (v2 if BLS_API_KEY set, else v1)
python -m usda_food_price_pipeline.ingestion.prices_bls
#    options: --series APU0000708111 ...  --start-year 2020 --end-year 2026
```

Run the tests (HTTP is fully mocked — no network, no keys needed):

```powershell
python -m pytest
```

## Phase 2 — Load to BigQuery

A loader reads the Phase-1 raw files and **batch-loads them essentially as-is** into
three raw tables in BigQuery (no cleaning/joining — that's Phase 3/dbt):

| Raw table | Source files | Reshaping (minimal) |
| --------- | ------------ | ------------------- |
| `raw_nutrition`  | `data/raw/nutrition/*.json`    | one row per food (the `foods` array); full food kept in `raw_json` |
| `raw_prices_bls` | `data/raw/prices/bls/*.json`   | one row per observation (`Results.series[].data[]` flattened) |
| `raw_prices_fmap`| `data/raw/prices/fmap/*.xlsx`  | one row per worksheet row (header → a JSON record in `raw_json`) |

Loads are **idempotent**: each table is loaded with `WRITE_TRUNCATE` in a single
batch load (free — no streaming inserts), so re-running fully replaces the table
with no duplicates. The loader then prints row counts per table to verify.

**One-time Google Cloud setup (beginner, minimal):**

1. **Service-account JSON** — already at `secrets/gcp-service-account.json`
   (gitignored). `.env` points to it: `GOOGLE_APPLICATION_CREDENTIALS=./secrets/gcp-service-account.json`.
2. **Dataset** — you don't create it by hand; the loader creates dataset `usda_raw`
   in project `usda-food-prices` (US) on first run if it's missing. (To make it
   manually instead: `bq --location=US mk -d usda-food-prices:usda_raw`.)
3. **Free tier / billing** — confirm in the Cloud Console under *Billing*:
   - **BigQuery Sandbox** (no billing account linked) = you *cannot* be charged;
     tables get a 60-day expiry. This is the safest posture for this project.
   - With a billing account linked, you're on the **free tier** (10 GB storage +
     1 TB queries/month); batch loads cost nothing. Set a budget alert to be safe.

```powershell
$env:PYTHONPATH = "src"     # bash: export PYTHONPATH=src

# Parse the raw files and print row counts WITHOUT touching BigQuery:
python -m usda_food_price_pipeline.load.bigquery_loader --dry-run

# Load all three raw tables into BigQuery (idempotent; prints row counts):
python -m usda_food_price_pipeline.load.bigquery_loader
#    options: --dataset usda_raw  --location US  --only nutrition bls fmap
```

## Repository Layout

| Path                              | Purpose                                              |
| --------------------------------- | ---------------------------------------------------- |
| `src/usda_food_price_pipeline/`   | Python package (importable source code)              |
| `src/.../ingestion/`              | Ingestion scripts that pull from the USDA APIs       |
| `src/.../load/`                   | Phase-2 loader: raw files → BigQuery raw tables       |
| `config/`                         | Non-secret configuration files                       |
| `tests/`                          | Automated tests (pytest)                             |
| `docs/`                           | Project documentation                                |
| `secrets/`                        | Local-only credentials (gitignored)                  |
| `.env.example` / `.env`           | Environment-variable template / your real values     |
| `requirements.txt`                | Python dependencies                                  |
