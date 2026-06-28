"""Phase 5: forecast next month's price for each BLS retail food series.

Reads the analytics table ``usda_analytics.fct_bls_prices`` (the current, ongoing
monthly BLS Average-Price feed — the only forecastable source; F-MAP ends in 2018),
fits a simple per-series model, and writes one next-month forecast per series to
``usda_forecast.fct_bls_forecast``.

Why this is a *simple* model: each BLS series has only ~4 years of monthly history
(~48 points), so a small, interpretable model is appropriate and held-out accuracy
is inherently noisy. Retail food prices behave close to a random walk, so the model
is a one-step **AR(1) + seasonality**: predict next month from last month's price
plus the sine/cosine of the month-of-year, via a scikit-learn ``StandardScaler`` →
``Ridge`` pipeline. Anchoring on the last actual keeps the forecast stable on
volatile series (e.g. eggs) where a raw trend line would over-extrapolate. Accuracy
is reported as the **MAPE of an expanding one-step-ahead backtest** over the most
recent ``--holdout`` months (the honest "next-month" error), with a last-value
(random-walk) naive baseline for context.

Idempotency: the forecast table is replaced every run with a single ``WRITE_TRUNCATE``
batch load job (free; never streaming inserts — BigQuery Sandbox forbids them), so
re-running fully overwrites it with no duplicates.

Run (as a module, with the src/ layout on the path):
    # PowerShell:  $env:PYTHONPATH = "src"
    # bash:        export PYTHONPATH=src
    python -m usda_food_price_pipeline.forecast.bls_forecast
    python -m usda_food_price_pipeline.forecast.bls_forecast --dry-run   # compute + print, no write

Needs ``GOOGLE_APPLICATION_CREDENTIALS`` (service-account JSON) in ``.env`` — the same
key the Phase-2 loader and dbt use.
"""

from __future__ import annotations

import argparse
import os
from datetime import date, datetime, timezone

import numpy as np

from ..ingestion import common

# Source: the dbt-built current price feed (Phase 3). Forecasts go to a SEPARATE,
# Python-owned dataset so they never collide with the dbt-managed usda_analytics.
DEFAULT_SOURCE_DATASET = "usda_analytics"
DEFAULT_SOURCE_TABLE = "fct_bls_prices"
DEFAULT_FORECAST_DATASET = "usda_forecast"
DEFAULT_FORECAST_TABLE = "fct_bls_forecast"
DEFAULT_LOCATION = "US"  # match the analytics datasets (BigQuery free tier lives in US).

DEFAULT_HOLDOUT = 6        # months held out for the one-step-ahead backtest
MIN_TRAIN_MONTHS = 12      # need >= 1 year to fit AR(1) + annual seasonality
RIDGE_ALPHA = 1.0          # mild regularization (features are standardized first)
MODEL_NAME = "ridge_ar1_seasonal"


# --------------------------------------------------------------------------- #
# Pure model functions (numpy + scikit-learn only; no BigQuery) — unit-tested.
# Model: one-step AR(1) + seasonality. Predict price[t] from
#   features = [ price[t-1], sin(month_t), cos(month_t) ]
# fit with StandardScaler -> Ridge. Anchoring on the previous actual keeps the
# forecast stable on volatile series (a raw trend line over-extrapolates).
# --------------------------------------------------------------------------- #
def add_one_month(d: date) -> date:
    """First day of the month after ``d`` (forecasts are keyed to first-of-month)."""
    return date(d.year + 1, 1, 1) if d.month == 12 else date(d.year, d.month + 1, 1)


def _design(prev_prices, month_nums) -> np.ndarray:
    """Design matrix: [previous-month price, sin(month), cos(month)]."""
    prev = np.asarray(prev_prices, dtype=float)
    month_nums = np.asarray(month_nums, dtype=float)
    radians = 2.0 * np.pi * (month_nums - 1.0) / 12.0
    return np.column_stack([prev, np.sin(radians), np.cos(radians)])


def _fit(prev_prices, month_nums, targets):
    """Fit the StandardScaler -> Ridge pipeline on (prev, month) -> target samples."""
    from sklearn.linear_model import Ridge
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    model = make_pipeline(StandardScaler(), Ridge(alpha=RIDGE_ALPHA))
    model.fit(_design(prev_prices, month_nums), np.asarray(targets, dtype=float))
    return model


def _one_step(prices, month_nums, upto: int) -> float:
    """Train on months 0..upto-1 and predict month ``upto`` (one step ahead), using the
    known previous actual price[upto-1] and the target month's number."""
    model = _fit(prices[0 : upto - 1], month_nums[1:upto], prices[1:upto])
    pred = model.predict(_design([prices[upto - 1]], [month_nums[upto]]))[0]
    return max(float(pred), 0.0)


def forecast_next(prices, month_nums, next_month_num: int) -> float:
    """Fit on all consecutive (prev -> next) pairs and predict the month after the last
    observation, anchored on the last actual price."""
    n = len(prices)
    model = _fit(prices[0 : n - 1], month_nums[1:n], prices[1:n])
    pred = model.predict(_design([prices[n - 1]], [next_month_num]))[0]
    return max(float(pred), 0.0)


def backtest_one_step(prices, month_nums, *, holdout: int, min_train: int) -> dict:
    """Expanding one-step-ahead backtest over the most recent ``holdout`` months.

    For each held-out month i, fit on months 0..i-1 and predict month i (a true
    out-of-sample next-month forecast). Returns the model MAPE, the last-value naive
    MAPE (both anchored on price[i-1], so the comparison is apples-to-apples), and the
    number of evaluated points.
    """
    n = len(prices)
    start = max(min_train, n - holdout)
    model_apes: list[float] = []
    naive_apes: list[float] = []
    for i in range(start, n):
        actual = float(prices[i])
        if actual == 0:
            continue  # MAPE is undefined when the actual is zero
        model_apes.append(abs(actual - _one_step(prices, month_nums, i)) / abs(actual))
        naive_apes.append(abs(actual - float(prices[i - 1])) / abs(actual))  # last-value baseline

    return {
        "mape": (sum(model_apes) / len(model_apes) * 100.0) if model_apes else None,
        "naive_mape": (sum(naive_apes) / len(naive_apes) * 100.0) if naive_apes else None,
        "n_eval": len(model_apes),
    }


def forecast_series(
    series_id: str,
    item_label,
    unit,
    dates: list[date],
    prices: list[float],
    *,
    holdout: int = DEFAULT_HOLDOUT,
    min_train: int = MIN_TRAIN_MONTHS,
    generated_at: str,
) -> dict | None:
    """Backtest + next-month forecast for one series. Returns a forecast row, or None
    if the series has fewer than ``min_train`` months of history."""
    n = len(prices)
    if n < min_train:
        return None

    prices = [float(p) for p in prices]
    month_nums = [d.month for d in dates]

    backtest = backtest_one_step(prices, month_nums, holdout=holdout, min_train=min_train)

    next_month = add_one_month(dates[-1])
    forecast_price = forecast_next(prices, month_nums, next_month.month)
    last_price = prices[-1]
    pct_change = (forecast_price - last_price) / last_price * 100.0 if last_price else None

    return {
        "series_id": series_id,
        "item_label": item_label,
        "unit": unit,
        "forecast_month": next_month.isoformat(),
        "forecast_price_usd": round(forecast_price, 4),
        "last_actual_month": dates[-1].isoformat(),
        "last_actual_price_usd": round(last_price, 4),
        "pct_change_vs_last": round(pct_change, 2) if pct_change is not None else None,
        "model": MODEL_NAME,
        "mape_backtest": round(backtest["mape"], 2) if backtest["mape"] is not None else None,
        "naive_mape_backtest": (
            round(backtest["naive_mape"], 2) if backtest["naive_mape"] is not None else None
        ),
        "n_backtest_points": backtest["n_eval"],
        "n_train_months": n,
        "generated_at": generated_at,
    }


# --------------------------------------------------------------------------- #
# BigQuery I/O (lazy import so the pure functions/tests need no client or creds).
# --------------------------------------------------------------------------- #
def read_bls_prices(client, source_dataset: str, source_table: str) -> dict:
    """Read the monthly BLS price feed, grouped per series (sorted by month)."""
    table_id = f"{client.project}.{source_dataset}.{source_table}"
    sql = f"""
        select series_id, item_label, unit, month_date, price_usd
        from `{table_id}`
        where price_usd is not null
        order by series_id, month_date
    """
    series: dict[str, dict] = {}
    for row in client.query(sql).result():
        bucket = series.setdefault(
            row["series_id"],
            {"item_label": row["item_label"], "unit": row["unit"], "dates": [], "prices": []},
        )
        bucket["dates"].append(row["month_date"])      # datetime.date
        bucket["prices"].append(float(row["price_usd"]))
    return series


def _forecast_schema(bq):
    SF = bq.SchemaField
    return [
        SF("series_id", "STRING"),
        SF("item_label", "STRING"),
        SF("unit", "STRING"),
        SF("forecast_month", "DATE"),
        SF("forecast_price_usd", "FLOAT64"),
        SF("last_actual_month", "DATE"),
        SF("last_actual_price_usd", "FLOAT64"),
        SF("pct_change_vs_last", "FLOAT64"),
        SF("model", "STRING"),
        SF("mape_backtest", "FLOAT64"),
        SF("naive_mape_backtest", "FLOAT64"),
        SF("n_backtest_points", "INT64"),
        SF("n_train_months", "INT64"),
        SF("generated_at", "TIMESTAMP"),
    ]


def write_forecasts(client, bq, rows: list[dict], dataset: str, table: str, location: str) -> int:
    """Batch-load forecast rows into the forecast table with WRITE_TRUNCATE.

    Reuses the Phase-2 loader's idempotent pattern: a single load job fully replaces
    the table (no duplicates), and it's a batch load — never a streaming insert.
    """
    from ..load.bigquery_loader import ensure_dataset

    ensure_dataset(client, bq, dataset, location)
    table_id = f"{client.project}.{dataset}.{table}"
    job_config = bq.LoadJobConfig(
        schema=_forecast_schema(bq),
        write_disposition=bq.WriteDisposition.WRITE_TRUNCATE,
        source_format=bq.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    client.load_table_from_json(rows, table_id, job_config=job_config).result()
    return client.get_table(table_id).num_rows


# --------------------------------------------------------------------------- #
# Orchestration / CLI.
# --------------------------------------------------------------------------- #
def _fmt(value) -> str:
    return f"{value:.2f}" if value is not None else "n/a"


def _print_report(rows: list[dict], skipped: list[str]) -> None:
    if not rows:
        print("  (no series had enough history to forecast)")
    else:
        print("\nNext-month forecast per series (accuracy = one-step backtest MAPE):")
        print(f"  {'item':30s} {'next':>9s} {'last':>9s} {'chg%':>7s} {'MAPE%':>7s} {'naive%':>7s}")
        for r in rows:
            print(
                f"  {str(r['item_label'])[:30]:30s} "
                f"{r['forecast_price_usd']:>9.3f} {r['last_actual_price_usd']:>9.3f} "
                f"{_fmt(r['pct_change_vs_last']):>7s} "
                f"{_fmt(r['mape_backtest']):>7s} {_fmt(r['naive_mape_backtest']):>7s}"
            )
        mapes = [r["mape_backtest"] for r in rows if r["mape_backtest"] is not None]
        naive = [r["naive_mape_backtest"] for r in rows if r["naive_mape_backtest"] is not None]
        if mapes:
            naive_txt = f"{sum(naive) / len(naive):.2f}%" if naive else "n/a"
            print(
                f"\n  Overall mean MAPE: {sum(mapes) / len(mapes):.2f}%  "
                f"(last-value naive baseline: {naive_txt})"
            )
    if skipped:
        print(f"  Skipped {len(skipped)} series with too little history: {', '.join(skipped)}")


def run(
    *,
    source_dataset: str,
    source_table: str,
    forecast_dataset: str,
    forecast_table: str,
    location: str,
    holdout: int,
    min_train: int,
    dry_run: bool,
) -> int:
    generated_at = datetime.now(timezone.utc).isoformat()

    from google.cloud import bigquery

    project = os.environ.get("BIGQUERY_PROJECT")  # else inferred from the credentials
    client = bigquery.Client(project=project) if project else bigquery.Client()

    print(f"Reading {client.project}.{source_dataset}.{source_table} …")
    series = read_bls_prices(client, source_dataset, source_table)
    print(f"  {len(series)} series read.")

    rows: list[dict] = []
    skipped: list[str] = []
    for series_id, s in sorted(series.items()):
        row = forecast_series(
            series_id,
            s["item_label"],
            s["unit"],
            s["dates"],
            s["prices"],
            holdout=holdout,
            min_train=min_train,
            generated_at=generated_at,
        )
        (rows if row is not None else skipped).append(row if row is not None else series_id)

    _print_report(rows, skipped)

    if dry_run:
        print("\n[DRY RUN] computed forecasts only; nothing written to BigQuery.")
        return 0
    if not rows:
        print("\nNo forecasts produced; nothing to write.")
        return 1

    n = write_forecasts(client, bigquery, rows, forecast_dataset, forecast_table, location)
    print(
        f"\nWrote {n} forecast row(s) to "
        f"{client.project}.{forecast_dataset}.{forecast_table} (WRITE_TRUNCATE)."
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Forecast next month's BLS retail food price per series and write it to BigQuery."
    )
    parser.add_argument("--source-dataset", default=os.environ.get("BIGQUERY_ANALYTICS_DATASET", DEFAULT_SOURCE_DATASET))
    parser.add_argument("--source-table", default=DEFAULT_SOURCE_TABLE)
    parser.add_argument("--forecast-dataset", default=os.environ.get("BIGQUERY_FORECAST_DATASET", DEFAULT_FORECAST_DATASET))
    parser.add_argument("--forecast-table", default=DEFAULT_FORECAST_TABLE)
    parser.add_argument("--location", default=os.environ.get("BIGQUERY_LOCATION", DEFAULT_LOCATION))
    parser.add_argument(
        "--holdout", type=int, default=DEFAULT_HOLDOUT,
        help=f"Recent months held out for the one-step backtest (default: {DEFAULT_HOLDOUT}).",
    )
    parser.add_argument(
        "--min-train", type=int, default=MIN_TRAIN_MONTHS,
        help=f"Minimum months of history required to forecast a series (default: {MIN_TRAIN_MONTHS}).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Compute + print forecasts and accuracy without writing to BigQuery.",
    )
    args = parser.parse_args(argv)

    common.load_environment()
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        print("ERROR: GOOGLE_APPLICATION_CREDENTIALS is not set (.env). See .env.example.")
        return 1

    print("Forecasting next-month BLS prices" + (" [DRY RUN]" if args.dry_run else ""))
    return run(
        source_dataset=args.source_dataset,
        source_table=args.source_table,
        forecast_dataset=args.forecast_dataset,
        forecast_table=args.forecast_table,
        location=args.location,
        holdout=args.holdout,
        min_train=args.min_train,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    raise SystemExit(main())
