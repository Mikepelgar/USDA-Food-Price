"""Tests for the BLS Average Price ingestion (HTTP mocked)."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest import mock

from usda_food_price_pipeline.ingestion import prices_bls


def test_bls_endpoint_selects_version_by_key():
    assert prices_bls.bls_endpoint(None) == prices_bls.BLS_API_V1
    assert prices_bls.bls_endpoint("KEY") == prices_bls.BLS_API_V2


def test_build_bls_payload_without_key():
    payload = prices_bls.build_bls_payload(["A", "B"], 2023, 2026)
    assert payload == {"seriesid": ["A", "B"], "startyear": "2023", "endyear": "2026"}
    assert "registrationkey" not in payload


def test_build_bls_payload_with_key():
    payload = prices_bls.build_bls_payload(["A"], 2023, 2026, api_key="KEY")
    assert payload["registrationkey"] == "KEY"


def test_default_year_range():
    now = datetime(2026, 6, 20, tzinfo=timezone.utc)
    assert prices_bls.default_year_range(now) == (2023, 2026)


def test_fetch_series_posts_payload_and_returns_json():
    resp = mock.Mock()
    resp.status_code = 200
    resp.json.return_value = {"status": "REQUEST_SUCCEEDED", "Results": {"series": []}}
    session = mock.Mock()
    session.post.return_value = resp

    data = prices_bls.fetch_series(session, ["APU0000708111"], 2023, 2026, api_key="KEY")

    assert data["status"] == "REQUEST_SUCCEEDED"
    session.post.assert_called_once_with(
        prices_bls.BLS_API_V2,
        json=prices_bls.build_bls_payload(["APU0000708111"], 2023, 2026, "KEY"),
        timeout=60,
    )
    resp.raise_for_status.assert_called_once()
