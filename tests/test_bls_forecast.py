"""Tests for the Phase-5 BLS forecast model (pure functions; no BigQuery).

Only the numpy/scikit-learn modelling logic is exercised here — the BigQuery client
is imported lazily inside the script, so these tests need no credentials. They do
require ``numpy`` + ``scikit-learn`` (Phase-5 deps in requirements.txt).
"""

from __future__ import annotations

import math
from datetime import date

from usda_food_price_pipeline.forecast import bls_forecast as fc

GENERATED_AT = "2026-06-28T00:00:00+00:00"


def _monthly_dates(start: date, n: int) -> list[date]:
    dates = [start]
    for _ in range(n - 1):
        dates.append(fc.add_one_month(dates[-1]))
    return dates


def _trend_seasonal_prices(n: int) -> list[float]:
    """A clean linear-trend + annual-seasonality series the model should fit well."""
    return [2.00 + 0.01 * t + 0.20 * math.sin(2 * math.pi * (t % 12) / 12) for t in range(n)]


def test_add_one_month_rolls_over_year():
    assert fc.add_one_month(date(2018, 1, 1)) == date(2018, 2, 1)
    assert fc.add_one_month(date(2018, 12, 1)) == date(2019, 1, 1)


def test_design_shape_and_seasonality():
    feats = fc._design([2.0, 2.1, 2.2], [1, 4, 7])
    assert feats.shape == (3, 3)
    # First column is the previous-month price; columns 2-3 are sin/cos of the month.
    assert feats[0, 0] == 2.0
    # Month 1 -> angle 0 -> sin=0, cos=1.
    assert abs(feats[0, 1] - 0.0) < 1e-9
    assert abs(feats[0, 2] - 1.0) < 1e-9


def test_forecast_series_on_clean_series_is_accurate():
    n = 48
    dates = _monthly_dates(date(2014, 1, 1), n)
    prices = _trend_seasonal_prices(n)

    row = fc.forecast_series(
        "APUTEST", "test item", "per lb", dates, prices, generated_at=GENERATED_AT
    )

    assert row is not None
    # Grain / shape: one row, keyed by series, predicting the month after the last actual.
    assert row["series_id"] == "APUTEST"
    assert row["forecast_month"] == "2018-01-01"   # month after 2017-12
    assert row["last_actual_month"] == "2017-12-01"
    assert row["n_train_months"] == n
    assert row["model"] == fc.MODEL_NAME
    # The model matches this clean process closely, so backtest error is tiny and the
    # forecast is a sensible positive price near the recent level.
    assert row["mape_backtest"] is not None
    assert row["mape_backtest"] < 5.0
    assert row["n_backtest_points"] == fc.DEFAULT_HOLDOUT
    assert 1.5 < row["forecast_price_usd"] < 3.5


def test_backtest_reports_naive_baseline():
    n = 36
    dates = _monthly_dates(date(2015, 1, 1), n)
    prices = _trend_seasonal_prices(n)
    month_nums = [d.month for d in dates]

    bt = fc.backtest_one_step(prices, month_nums, holdout=6, min_train=12)

    assert bt["n_eval"] == 6
    assert bt["mape"] is not None and bt["naive_mape"] is not None
    # On a smooth trend+seasonal series the AR(1)+seasonal model beats a last-value baseline.
    assert bt["mape"] < bt["naive_mape"]


def test_forecast_series_skips_short_history():
    dates = _monthly_dates(date(2017, 1, 1), 6)
    prices = _trend_seasonal_prices(6)
    assert (
        fc.forecast_series("SHORT", "x", "per lb", dates, prices, generated_at=GENERATED_AT)
        is None
    )


def test_forecast_price_never_negative():
    # A steep downward trend would extrapolate below zero; the model clamps to 0.
    n = 24
    dates = _monthly_dates(date(2016, 1, 1), n)
    prices = [max(5.0 - 0.25 * t, 0.05) for t in range(n)]
    row = fc.forecast_series("DROP", "x", "per lb", dates, prices, generated_at=GENERATED_AT)
    assert row is not None
    assert row["forecast_price_usd"] >= 0.0
