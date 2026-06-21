"""Tests for the FoodData Central nutrition ingestion (HTTP mocked)."""

from __future__ import annotations

from unittest import mock

from usda_food_price_pipeline.ingestion import nutrition_fdc


def test_build_search_params():
    params = nutrition_fdc.build_search_params("apple", "KEY123", page_number=2, page_size=25)
    assert params == {
        "query": "apple",
        "pageSize": 25,
        "pageNumber": 2,
        "api_key": "KEY123",
    }


def test_search_filename():
    name = nutrition_fdc.search_filename("Cheddar Cheese", 3, "20260620T000000Z")
    assert name == "fdc_search_cheddar_cheese_p03_20260620T000000Z.json"


def test_fetch_page_calls_endpoint_and_returns_json():
    resp = mock.Mock()
    resp.status_code = 200
    resp.json.return_value = {"foods": [{"description": "Apple"}], "totalPages": 1}
    session = mock.Mock()
    session.get.return_value = resp

    data = nutrition_fdc.fetch_page(session, "apple", "KEY", page_number=1, page_size=50)

    assert data["foods"][0]["description"] == "Apple"
    session.get.assert_called_once_with(
        f"{nutrition_fdc.FDC_BASE_URL}/foods/search",
        params=nutrition_fdc.build_search_params("apple", "KEY", 1, 50),
        timeout=30,
    )
    resp.raise_for_status.assert_called_once()
