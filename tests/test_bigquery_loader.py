"""Tests for the Phase-2 BigQuery loader's pure row builders (no network, no BigQuery).

Only the file-/JSON-shaping logic is exercised here; the BigQuery client is
imported lazily inside the loader, so these tests need no credentials or deps
beyond what's in requirements.
"""

from __future__ import annotations

import json

from usda_food_price_pipeline.load import bigquery_loader as loader

LOADED_AT = "2026-06-23T00:00:00+00:00"


def test_nutrition_rows_extracts_foods_and_keeps_payload():
    page = {
        "totalPages": 1,
        "foods": [
            {"fdcId": 111, "description": "Apple", "foodNutrients": [{"name": "fiber"}]},
            {"fdcId": 222, "description": "Banana"},
        ],
    }
    rows = loader.nutrition_rows_from_page(page, "fdc_apple_p01.json", LOADED_AT)

    assert len(rows) == 2
    assert rows[0]["source_file"] == "fdc_apple_p01.json"
    assert rows[0]["fdc_id"] == 111
    assert rows[0]["loaded_at"] == LOADED_AT
    # raw_json preserves the whole food object verbatim.
    assert json.loads(rows[0]["raw_json"])["foodNutrients"] == [{"name": "fiber"}]


def test_nutrition_rows_empty_page():
    assert loader.nutrition_rows_from_page({}, "empty.json", LOADED_AT) == []


def test_bls_rows_flatten_series_data():
    data = {
        "Results": {
            "series": [
                {
                    "seriesID": "APU0000708111",
                    "data": [
                        {
                            "year": "2026",
                            "period": "M05",
                            "periodName": "May",
                            "latest": "true",
                            "value": "2.191",
                            "footnotes": [{}],
                        },
                        {
                            "year": "2026",
                            "period": "M04",
                            "periodName": "April",
                            "value": "2.050",
                            "footnotes": [{}],
                        },
                    ],
                }
            ]
        }
    }
    rows = loader.bls_rows_from_response(data, "bls_ap.json", LOADED_AT)

    assert len(rows) == 2
    first = rows[0]
    assert first["series_id"] == "APU0000708111"
    assert first["year"] == "2026"
    assert first["period"] == "M05"
    assert first["value"] == "2.191"  # kept as the API's string, no casting
    assert first["latest"] == "true"
    assert json.loads(first["footnotes"]) == [{}]
    assert first["source_file"] == "bls_ap.json"


def test_bls_rows_no_results_key():
    assert loader.bls_rows_from_response({"status": "X"}, "bls.json", LOADED_AT) == []


def test_fmap_rows_use_header_as_keys():
    sheet_rows = [
        ("Year", "Month", "EFPG_name"),
        ("2012", "1", "Whole-grain breads"),
        ("2012", "2", "Whole-grain breads"),
    ]
    rows = loader.fmap_rows_from_sheet("Data", iter(sheet_rows), "fmap.xlsx", LOADED_AT)

    assert len(rows) == 2
    assert rows[0]["sheet_name"] == "Data"
    assert rows[0]["row_index"] == 1
    assert rows[1]["row_index"] == 2
    record = json.loads(rows[0]["raw_json"])
    assert record == {"Year": "2012", "Month": "1", "EFPG_name": "Whole-grain breads"}


def test_fmap_rows_header_only_sheet_yields_nothing():
    assert loader.fmap_rows_from_sheet("Data", iter([("A", "B")]), "f.xlsx", LOADED_AT) == []


def test_fmap_rows_empty_sheet_yields_nothing():
    assert loader.fmap_rows_from_sheet("ReadMe", iter([]), "f.xlsx", LOADED_AT) == []


def test_fmap_rows_handle_missing_and_extra_cells():
    # Header has 2 columns; one data row is short, one is long.
    sheet_rows = [("A", "B"), ("only-a",), ("a", "b", "c")]
    rows = loader.fmap_rows_from_sheet("Data", iter(sheet_rows), "f.xlsx", LOADED_AT)

    assert json.loads(rows[0]["raw_json"]) == {"A": "only-a"}
    # Extra cell beyond the header falls back to a positional key.
    assert json.loads(rows[1]["raw_json"]) == {"A": "a", "B": "b", "col_2": "c"}
