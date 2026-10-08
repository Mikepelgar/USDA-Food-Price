# USDA Food Price & Nutrition Pipeline

[![CI](https://github.com/Mikepelgar/USDA-Food-Price/actions/workflows/ci.yml/badge.svg)](https://github.com/Mikepelgar/USDA-Food-Price/actions/workflows/ci.yml)

A batch pipeline that pulls U.S. food prices and nutrition data from three public sources into
BigQuery, models it with dbt, and runs daily on Airflow in Docker. A Streamlit dashboard reads the
modeled tables, and a small forecast predicts next month's price for 8 grocery items. Everything
runs locally against BigQuery's free Sandbox tier. Nothing is hosted: Airflow runs while Docker is
up, and the dashboard runs on localhost.

## Sources

- USDA FoodData Central API: nutrient profiles from searches for 15 common foods.
- USDA ERS Food-at-Home Monthly Area Prices (F-MAP): monthly prices for 90 food categories in 15
  U.S. areas, 2012 to 2018. An Excel download, not an API, and it ends in 2018.
- BLS Average Price data: current monthly prices for 8 items, from eggs to chicken breast. National
  average only. BLS is not part of USDA.

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

Raw records load with `WRITE_TRUNCATE`, so a rerun replaces rows instead of duplicating them. All
cleaning happens in dbt, and `dbt_test` is the last task, so a failed data test fails the run.

## How nutrition per dollar is calculated

For each food category, `dim_nutrition` takes the median amount of every nutrient per 100 g across
the category's Foundation, SR Legacy and Survey foods. Branded foods are left out. F-MAP gives each
price category a weighted mean price in dollars per 100 g, so dividing one by the other cancels
the 100 g:

```
amount_per_dollar = amount_per_100g / mean_unit_value
```

The result is in the nutrient's own unit per dollar, such as grams of protein or milligrams of
calcium. `fct_nutrition_per_dollar` ranks categories by it within each area, month and nutrient.

## Common problems and how they're solved

**Price and nutrition don't share keys.** F-MAP has "whole milk" and FoodData Central has "dairy and
egg products". [`category_crosswalk.csv`](transform/seeds/category_crosswalk.csv) maps 20 F-MAP
categories onto 14 FoodData Central categories by hand. Pairs like the two milks share one
nutrition profile, so their ranking differs only by price, and categories without a reasonable
match are left out.

**Eight nutrients wasn't enough.** The first `dim_nutrition` had one column per nutrient for eight
hand-picked ones, and the raw data has 221 nutrient series. The table is now long, one row per
category, nutrient number and unit, and 214 series reach the per-dollar table. That table has about
3.2 million rows, so it's clustered on `nutrient_number` and the dashboard queries one nutrient at
a time ([`dim_nutrition.sql`](transform/models/analytics/dim_nutrition.sql)).

**Foods report the same nutrient more than once.** FoodData Central can list a nutrient several
times for one food, so each food is collapsed to one value with `max()` before the median.

**Stray values in the BLS feed.** Blanks and footnote markers are cast with `safe_cast`, so they
become NULL and are dropped instead of failing the run. The annual average (period `M13`) is
filtered out to keep one row per month
([`stg_prices_bls.sql`](transform/models/staging/stg_prices_bls.sql)).

**The F-MAP file never changes.** Its task exits with code 99 when the file is already there, which
Airflow treats as a skip. `load_bigquery` uses `trigger_rule=none_failed`, so the skip doesn't stop
the load ([`usda_pipeline_dag.py`](dags/usda_pipeline_dag.py)).

**dbt and Airflow in one image.** The ingestion libraries are installed into Airflow's Python against
its constraints file. dbt goes into a separate venv at `/opt/dbt-venv` so its pins can't clash with
Airflow's, and the dbt project is copied in at build time
([`Dockerfile`](docker/airflow/Dockerfile)). The downside is that editing `transform/` has no effect
until the image is rebuilt. The one failure in the DAG's four runs was a run stopped because it was
using an outdated image.

## Results

From a full rebuild and Airflow run on 2026-10-05 and 2026-10-06:

| Measure | Value |
|---|---|
| Raw records loaded | 164,113 (1,500 nutrition foods, 351 BLS observations, 162,262 F-MAP rows) |
| dbt tests | 47, all passing |
| Python unit tests | 41, with HTTP and BigQuery mocked |
| Airflow run time | About 3.5 minutes |
| Forecast MAPE | 1.79%, against 1.51% for a naive baseline |

The forecast is a per-series Ridge regression on last month's price plus the sine and cosine of
the month, scored with an expanding one-step backtest over the last six months of each 42 or
43-point series. The naive forecast, which repeats last month's price, is more accurate, and the
model wins on only 1 of the 8 series. Month to month, retail food prices move close to a random
walk, so with this little history the last value is hard to beat.

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
