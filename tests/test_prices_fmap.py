"""Tests for the ERS F-MAP file ingestion (HTTP mocked, no parsing)."""

from __future__ import annotations

from unittest import mock

import pytest

from usda_food_price_pipeline.ingestion import prices_fmap


def test_source_basename_strips_query_string():
    url = "https://www.ers.usda.gov/media/5399/food-at-home-monthly-area-prices-2012-to-2018.xlsx?v=27903"
    assert prices_fmap.source_basename(url) == "food-at-home-monthly-area-prices-2012-to-2018.xlsx"


def test_source_basename_handles_local_path():
    assert prices_fmap.source_basename("/c/Users/mikep/Downloads/FMAP.xlsx") == "FMAP.xlsx"


def test_fmap_filename_prefixes_timestamp():
    name = prices_fmap.fmap_filename("https://x/y/data.xlsx?v=1", "20260620T000000Z")
    assert name == "20260620T000000Z_data.xlsx"


def test_download_file_streams_to_disk(tmp_path):
    resp = mock.Mock()
    resp.status_code = 200
    resp.iter_content.return_value = [b"abc", b"", b"def"]  # empty chunk skipped
    session = mock.Mock()
    session.get.return_value = resp

    dest = prices_fmap.download_file(session, "https://x/data.xlsx", tmp_path / "data.xlsx")

    assert dest.read_bytes() == b"abcdef"
    session.get.assert_called_once_with("https://x/data.xlsx", stream=True, timeout=120)
    resp.raise_for_status.assert_called_once()


def test_copy_local_file(tmp_path):
    src = tmp_path / "FMAP.xlsx"
    src.write_bytes(b"raw-xlsx-bytes")
    dest = prices_fmap.copy_local_file(src, tmp_path / "out" / "copy.xlsx")
    assert dest.read_bytes() == b"raw-xlsx-bytes"


def test_ingest_from_file_missing_source_raises():
    with pytest.raises(FileNotFoundError):
        prices_fmap.ingest_from_file("/no/such/file.xlsx")
