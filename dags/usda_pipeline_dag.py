"""Phase 4 orchestration: the USDA food-price pipeline as one daily Airflow DAG.

This DAG is a *thin wrapper* — every task just runs an existing module/command from the
repo (Phases 1–3). No ingestion/loader/dbt business logic lives here; the design notes are
in CLAUDE.md ("Phase 4").

Flow (strictly in order, daily):
    ingest_nutrition -> ingest_bls -> ingest_fmap -> load_bigquery -> dbt_run -> dbt_test

Why a few tasks look the way they do:
  * ingest_nutrition / ingest_bls clear their data/raw/<source> folder *before* re-ingesting.
    Each run writes a new timestamped file, and the Phase-2 loader globs ALL files for a
    source with WRITE_TRUNCATE — so without clearing, daily runs would pile duplicate
    snapshots into the raw tables and grow data/ unbounded. Clearing first means each run
    loads exactly one fresh snapshot. (A loader `--latest-only` flag is the cleaner Phase-6
    fix; we keep ingestion/loader internals untouched here.)
  * ingest_fmap is a static 2012–2018 file download, so it SKIPS (exit 99 -> "skipped") when
    a file already exists. Because a skipped upstream would otherwise skip its downstream,
    load_bigquery uses trigger_rule="none_failed" so it still runs after a skip.
  * dbt models are built in dbt_run (seed + run); dbt_test is a dedicated gate — if any test
    fails it returns non-zero, the task fails, and the DAG run is marked failed (the pipeline
    stops there rather than reporting success). `dbt deps` is baked into the image, not run
    here.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

# The repo is mounted under AIRFLOW_HOME (/opt/airflow): src/ on PYTHONPATH, data/ for raw
# files, transform/ baked into the image, dbt in an isolated venv.
PROJECT_DIR = "/opt/airflow"
TRANSFORM_DIR = f"{PROJECT_DIR}/transform"
DBT = "/opt/dbt-venv/bin/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt_profile"

# Reliability defaults applied to every task: a couple of retries with exponential backoff.
default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="usda_food_price_pipeline",
    description="Ingest USDA nutrition + food prices, load to BigQuery, build & test dbt models.",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),  # static — never datetime.now()
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["usda", "etl", "bigquery", "dbt"],
) as dag:

    ingest_nutrition = BashOperator(
        task_id="ingest_nutrition",
        # Clear the prior snapshot first (see module docstring), then ingest.
        bash_command=(
            "rm -f data/raw/nutrition/*.json && "
            "python -m usda_food_price_pipeline.ingestion.nutrition_fdc"
        ),
        cwd=PROJECT_DIR,
        retries=3,  # network-prone: a few extra retries
        execution_timeout=timedelta(minutes=15),
    )

    ingest_bls = BashOperator(
        task_id="ingest_bls",
        bash_command=(
            "rm -f data/raw/prices/bls/*.json && "
            "python -m usda_food_price_pipeline.ingestion.prices_bls"
        ),
        cwd=PROJECT_DIR,
        retries=3,
        execution_timeout=timedelta(minutes=15),
    )

    ingest_fmap = BashOperator(
        task_id="ingest_fmap",
        # Static dataset: skip the download (exit 99 -> "skipped") if a file is already here.
        bash_command=(
            "if ls data/raw/prices/fmap/*.xlsx >/dev/null 2>&1; then "
            '  echo "F-MAP file already present; skipping download."; exit 99; '
            "fi; "
            "python -m usda_food_price_pipeline.ingestion.prices_fmap"
        ),
        cwd=PROJECT_DIR,
        skip_on_exit_code=99,
        retries=3,
        execution_timeout=timedelta(minutes=15),
    )

    load_bigquery = BashOperator(
        task_id="load_bigquery",
        bash_command="python -m usda_food_price_pipeline.load.bigquery_loader --dataset usda_raw",
        cwd=PROJECT_DIR,
        # Run even when ingest_fmap was skipped; only an actual upstream failure stops us.
        trigger_rule=TriggerRule.NONE_FAILED,
        execution_timeout=timedelta(minutes=20),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        # Build the models (seeds + views + tables); tests are the separate gate below.
        bash_command=(
            f"{DBT} seed --profiles-dir {DBT_PROFILES_DIR} && "
            f"{DBT} run --profiles-dir {DBT_PROFILES_DIR}"
        ),
        cwd=TRANSFORM_DIR,
        retries=1,
        execution_timeout=timedelta(minutes=20),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        # The gate: a failing test exits non-zero -> this task fails -> the run is marked
        # failed, so the pipeline surfaces the problem instead of reporting success.
        bash_command=f"{DBT} test --profiles-dir {DBT_PROFILES_DIR}",
        cwd=TRANSFORM_DIR,
        retries=1,
        execution_timeout=timedelta(minutes=20),
    )

    ingest_nutrition >> ingest_bls >> ingest_fmap >> load_bigquery >> dbt_run >> dbt_test
