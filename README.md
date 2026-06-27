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

> Status: Phases 1–3 are built — ingestion (→ local raw files), load (raw files → BigQuery),
> and transformation (dbt models on BigQuery). Orchestration and dashboard code come in later phases.

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

## Phase 3 — Transformation (dbt)

A [dbt](https://docs.getdbt.com/) project under `transform/` turns the three raw `usda_raw`
tables into clean, tested, documented analytics tables on BigQuery: **4 staging views**
(`stg_nutrition`, `stg_prices_bls`, `stg_prices_fmap`, `stg_fmap_price_index`) in dataset
`usda_staging`, and **4 analytics tables** in `usda_analytics`:

| Analytics table | Grain | What it is |
| --------------- | ----- | ---------- |
| `fct_fmap_prices` | category × region × month | ERS F-MAP monthly mean-unit-value price (USD/100 g) + price index, 2012–2018 |
| `fct_bls_prices` | item × month | BLS current monthly retail price series — the forecast input for Phase 5 |
| `dim_nutrition` | FDC food category | median nutrients per 100 g (non-Branded foods) |
| `fct_nutrition_per_dollar` | category × region × month | nutrients **per dollar**, ranked by protein/$ — F-MAP price ⋈ nutrition via a category crosswalk |

Two seeds support these: `category_crosswalk.csv` (F-MAP category → FDC food category — see the
note below) and `bls_series_items.csv` (BLS series id → item label).

### How dbt connects to BigQuery (beginner notes)

- **`profiles.yml` is dbt's database-connection file.** `dbt_project.yml` (committed, in
  `transform/`) names a `profile`; dbt looks that name up in `profiles.yml` to learn *which
  warehouse, project/dataset, and credentials* to use. Keeping the connection separate from the
  SQL is what lets the same models run against different targets.
- **The connection** uses BigQuery `method: service-account` with `keyfile:` pointing at the
  **same** `secrets/gcp-service-account.json` the Phase-2 loader uses. dbt mints a token from
  that key and runs query jobs in project `usda-food-prices` (free tier / Sandbox; no streaming).
- **Secrets stay out of git.** The keyfile is already gitignored, and `profiles.yml` only stores
  a *path* to it (never the key). The committed template is
  [`transform/profiles.example.yml`](transform/profiles.example.yml); your real `profiles.yml` is
  gitignored. Easiest setup: copy it to `transform/profiles.yml` and run dbt **from** `transform/`
  with `--profiles-dir .` (so the relative `keyfile` path resolves). Alternatively keep it at
  dbt's default `~/.dbt/profiles.yml` with an absolute keyfile path.

### Run it

```powershell
# from the repo root, in the activated .venv:
pip install -r requirements.txt          # now includes dbt-bigquery

cd transform
copy profiles.example.yml profiles.yml   # then confirm the keyfile path is correct
dbt deps                                 # installs dbt_utils (see packages.yml)
dbt debug --profiles-dir .               # verifies the BigQuery connection ("Connection test: OK")

# build everything (seeds -> staging views -> analytics tables) AND run all tests, in order:
dbt build --profiles-dir .
# or run just the tests:
dbt test  --profiles-dir .
```

`dbt build` already runs seeds, models, and tests together in dependency order — you do **not**
need to run `dbt seed` / `dbt run` / `dbt test` separately (each exists if you want a single
stage). Tests cover not-null/unique keys, prices never negative, plausible nutrient ranges, and a
uniqueness check on the F-MAP category-month-region grain.

Browse the generated docs (model + column descriptions and the lineage graph):

```powershell
dbt docs generate --profiles-dir .
dbt docs serve    --profiles-dir .
```

> **Category crosswalk note.** FDC food categories (e.g. *dairy and egg products*) and F-MAP
> price categories (e.g. *whole milk*) do **not** line up one-to-one, so the nutrition-per-dollar
> model joins them through `transform/seeds/category_crosswalk.csv` rather than assuming the keys
> match. It is a small, curated, intentionally-lossy mapping over the overlapping food basket;
> categories with no clean match are left out. `fct_nutrition_per_dollar` therefore uses
> **historical** F-MAP prices (2012–2018) — `fct_bls_prices` is the current/forecastable feed.

## Repository Layout

| Path                              | Purpose                                              |
| --------------------------------- | ---------------------------------------------------- |
| `src/usda_food_price_pipeline/`   | Python package (importable source code)              |
| `src/.../ingestion/`              | Ingestion scripts that pull from the USDA APIs       |
| `src/.../load/`                   | Phase-2 loader: raw files → BigQuery raw tables       |
| `transform/`                      | Phase-3 dbt project: staging + analytics models, seeds, tests |
| `config/`                         | Non-secret configuration files                       |
| `tests/`                          | Automated tests (pytest)                             |
| `docs/`                           | Project documentation                                |
| `secrets/`                        | Local-only credentials (gitignored)                  |
| `.env.example` / `.env`           | Environment-variable template / your real values     |
| `requirements.txt`                | Python dependencies                                  |
