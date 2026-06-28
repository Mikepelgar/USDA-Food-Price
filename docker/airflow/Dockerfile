# Phase 4 — custom Airflow image for the USDA food-price pipeline.
#
# One image runs every Airflow service (scheduler, webserver, init). It adds:
#   1. the ingestion/loader Python deps to Airflow's own env (constraint-pinned), so the
#      DAG's `python -m usda_food_price_pipeline...` tasks work;
#   2. an ISOLATED venv (/opt/dbt-venv) holding dbt-bigquery, so dbt's pins never clash
#      with Airflow's;
#   3. the dbt project (transform/) baked in, with `dbt deps` (dbt_utils) run at BUILD time
#      so no DAG run ever re-downloads packages.
#
# Pinned to a specific Airflow + Python so the constraints URL below resolves.
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

# 1) Ingestion + loader deps in Airflow's own env, pinned with Airflow's constraints file
#    (the documented, conflict-safe way to add libraries to an Airflow image).
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# 2) Isolated dbt environment (NO Airflow constraints — keep dbt's deps to itself).
USER root
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-bigquery>=1.7,<2.0"
USER airflow

# 3) Bake the dbt project + install its packages (dbt_utils) at build time. dbt deps needs
#    no warehouse credentials. .dockerignore keeps target/, logs/, dbt_packages/, and the
#    host profiles.yml out of the image. Owned by airflow:0 so dbt can write target/ + logs/
#    at run time.
COPY --chown=airflow:0 transform/ /opt/airflow/transform/
RUN cd /opt/airflow/transform && /opt/dbt-venv/bin/dbt deps
