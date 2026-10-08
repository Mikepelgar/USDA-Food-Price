# USDA Food Price & Nutrition Pipeline

[![CI](https://github.com/Mikepelgar/USDA-Food-Price/actions/workflows/ci.yml/badge.svg)](https://github.com/Mikepelgar/USDA-Food-Price/actions/workflows/ci.yml)

A batch pipeline that pulls U.S. food prices and nutrition data from three public sources into
BigQuery, models it with dbt, and runs daily on Airflow in Docker. A Streamlit dashboard reads the
modeled tables, and a small forecast predicts next month's price for 8 grocery items. Everything
runs locally against BigQuery's free Sandbox tier. Nothing is hosted: Airflow runs while Docker is
up, and the dashboard runs on localhost.

## Sources

- **USDA FoodData Central API:** nutrient profiles from searches for 15 common foods.
- **USDA ERS Food-at-Home Monthly Area Prices (F-MAP):** monthly prices for 90 food categories in
  15 U.S. areas, 2012 to 2018. An Excel download, not an API, and it ends in 2018.
- **BLS Average Price data:** current monthly prices for 8 items, from eggs to chicken breast.
  National average only. BLS is not part of USDA.

## How it works

```
FoodData Central API ─┐
ERS F-MAP (.xlsx) ────┼─> ingestion ─> data/raw/ ─> BigQuery usda_raw
BLS APU API ──────────┘                                  │ dbt
                                                         ▼
                              usda_staging (4 views) ─> usda_analytics (4 tables)
                                                         │
                               bls_forecast.py ─> usda_forecast ─> Streamlit dashboard

Airflow, daily:
ingest_nutrition > ingest_bls > ingest_fmap > load_bigquery > dbt_run > dbt_test
```

The loader copies raw records into BigQuery with almost no reshaping, using `WRITE_TRUNCATE` so a
rerun replaces rows instead of duplicating them. All cleaning happens in dbt. `dbt_test` is the
last task, so a failed data test fails the run.

Price and nutrition data don't share keys: F-MAP has "whole milk", FoodData Central has "dairy and
egg products". [`category_crosswalk.csv`](transform/seeds/category_crosswalk.csv) maps 20 F-MAP
categories onto FoodData Central categories by hand, so several price categories share one
nutrition profile and categories without a reasonable match are left out.

## Design notes

### Nutrition is stored long, not wide

The first version of `dim_nutrition` had one row per food category and one column per nutrient,
for eight hand-picked nutrients: protein, energy, fat, carbs, fiber, calcium, iron and sodium. The
dashboard offered three of them. The raw data has 221 nutrient series, and adding any of the
others meant adding a column.

The current version is long: one row per food category, nutrient number and unit, with the unit
carried through so the dashboard can label each value. 214 series now reach the per-dollar table.
The trade-off is size: `fct_nutrition_per_dollar` has about 3.2 million rows, one per category,
area, month and nutrient. Since the dashboard reads one nutrient at a time, the table is clustered
on `nutrient_number` and the dashboard queries one nutrient's slice from a dropdown.

### The Airflow image keeps its own copy of the models

One image runs the Airflow scheduler, webserver and init container. The ingestion libraries are
installed into Airflow's Python against Airflow's constraints file. dbt is not: it lives in a
separate venv at `/opt/dbt-venv` so its pins can't clash with Airflow's, and the DAG calls it by
full path. The dbt project and `dbt_utils` are copied in at build time, so a run never downloads
packages.

The downside is that editing `transform/` has no effect on the DAG until the image is rebuilt with
`docker compose up -d --build`. The one failure in the DAG's four runs was a run stopped because it
was using an outdated image.

### The forecast does not beat a naive baseline

Each BLS series has 42 or 43 monthly points, because ingestion pulls the current year plus the
three before it. The model is a per-series Ridge regression on last month's price plus the sine
and cosine of the month. It is scored with an expanding one-step backtest over the last six
months, and the same loop scores a naive forecast that repeats last month's price.

The naive forecast is more accurate: 1.51% MAPE against 1.79% for the model, which is better on
only 1 of the 8 series. Retail food prices move close to a random walk from month to month, and
with this little history the last value is hard to beat. The dashboard shows both numbers next
to each forecast.

## Results

From a full rebuild and Airflow run on 2026-10-05 and 2026-10-06:

| Measure | Value |
|---|---|
| Raw records loaded | 164,113 (1,500 nutrition foods, 351 BLS observations, 162,262 F-MAP rows) |
| dbt tests | 47, all passing |
| Python unit tests | 41, with HTTP and BigQuery mocked |
| Airflow run time | About 3.5 minutes |

## Known limitations

- The forecast is not a DAG task. It runs by hand, so `usda_forecast` is only as fresh as the
  last manual run. It belongs at the end of the DAG, after `dbt_test`.
- The loader reads every file in a source's raw folder, so the nutrition and BLS tasks `rm -f`
  the previous snapshot first, or each daily run would load another copy. A `--latest-only` flag
  on the loader would move that logic out of the DAG.
- The forecast pairs each price with the previous row, not the previous calendar month, so a gap
  in a series is treated as a single step.

## Running it

Requirements: Python 3.11, Docker, a Google Cloud project with BigQuery (the Sandbox is enough), a
service-account key for that project, and a FoodData Central API key. A BLS key is optional;
without one the BLS script uses the v1 API.

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
cp .env.example .env
```

Put the API keys in `.env` and save the service-account key as
`secrets/gcp-service-account.json`. Both are gitignored. The Python scripts take the BigQuery
project from the service account, but both dbt profiles name `usda-food-prices`, so change
`project` in `transform/profiles.yml` (copied below) and `docker/airflow/dbt_profile/profiles.yml`.

```bash
export PYTHONPATH=src              # PowerShell: $env:PYTHONPATH = "src"
python -m usda_food_price_pipeline.ingestion.nutrition_fdc
python -m usda_food_price_pipeline.ingestion.prices_bls
python -m usda_food_price_pipeline.ingestion.prices_fmap
python -m usda_food_price_pipeline.load.bigquery_loader     # --dry-run parses without loading

cd transform
cp profiles.example.yml profiles.yml
dbt deps
dbt build --profiles-dir .
cd ..

python -m usda_food_price_pipeline.forecast.bls_forecast
streamlit run dashboard/app.py     # http://localhost:8501
```

To run it under Airflow instead:

```bash
docker compose up -d --build       # UI at http://localhost:8080
docker compose exec airflow-scheduler airflow dags unpause usda_food_price_pipeline
docker compose exec airflow-scheduler airflow dags trigger usda_food_price_pipeline
docker compose down
```

The web login comes from `_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD` in `.env`.
Sandbox tables expire after 60 days; rerunning the pipeline rebuilds them from `data/raw/`. Tests
run with `python -m pytest`, and GitHub Actions runs them plus `dbt parse` on every push and pull
request, without cloud credentials.

## Data

All data comes from public USDA and BLS sources.
