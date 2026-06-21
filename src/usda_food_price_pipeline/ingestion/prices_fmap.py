"""Phase 1 ingestion: USDA ERS Food-at-Home Monthly Area Prices (F-MAP).

F-MAP is a FILE DOWNLOAD, not an API (no key needed). This script saves the raw
data file(s) AS-IS to ``data/raw/prices/fmap/`` — it does NOT parse or transform
them (parsing is a later phase).

By default it downloads the official 2012-2018 XLSX from ers.usda.gov. If you
already have the file locally (or the download is blocked), pass
``--from-file PATH`` to copy it in instead.

Run:
    python -m usda_food_price_pipeline.ingestion.prices_fmap
    python -m usda_food_price_pipeline.ingestion.prices_fmap --from-file ~/Downloads/FMAP.xlsx
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from urllib.parse import unquote, urlsplit

from . import common

# Data-product landing page (for reference / docs).
FMAP_PAGE_URL = "https://www.ers.usda.gov/data-products/food-at-home-monthly-area-prices"

# Direct file URLs verified 2026-06-20 (HTTP 200, ~12.5 MB xlsx). The main
# dataset is first; supplemental indexes are included for completeness.
FMAP_DOWNLOAD_URLS = [
    "https://www.ers.usda.gov/media/5399/food-at-home-monthly-area-prices-2012-to-2018.xlsx?v=27903",
    "https://www.ers.usda.gov/media/5401/food-at-home-monthly-area-prices-supplemental-price-indexes-2016-to-2018.xlsx?v=84439",
]


def source_basename(source: str) -> str:
    """Filename portion of a URL or local path, query string stripped."""
    path_part = urlsplit(source).path if "://" in source else source
    name = Path(unquote(path_part)).name
    return name or "fmap.xlsx"


def fmap_filename(source: str, timestamp: str) -> str:
    """Timestamped destination filename derived from the source URL/path."""
    return f"{timestamp}_{source_basename(source)}"


def download_file(session, url: str, dest: Path) -> Path:
    """Stream a URL to ``dest`` raw (with retries). Returns ``dest``."""
    response = common.retry_request(lambda: session.get(url, stream=True, timeout=120))
    response.raise_for_status()
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("wb") as fh:
        for chunk in response.iter_content(chunk_size=1 << 16):
            if chunk:
                fh.write(chunk)
    return dest


def copy_local_file(src: Path, dest: Path) -> Path:
    """Copy an already-downloaded F-MAP file into the raw folder."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dest)
    return dest


def ingest_download(urls=FMAP_DOWNLOAD_URLS) -> list:
    """Download each F-MAP URL to the raw folder. Returns saved paths."""
    out_dir = common.raw_dir("prices", "fmap")
    session = common.make_session()
    saved = []
    for url in urls:
        timestamp = common.utc_timestamp()
        dest = out_dir / fmap_filename(url, timestamp)
        download_file(session, url, dest)
        saved.append(dest)
        print(f"  downloaded {dest.name}  ({dest.stat().st_size:,} bytes)")
    return saved


def ingest_from_file(src_path: str) -> list:
    """Copy a local F-MAP file into the raw folder. Returns saved paths."""
    out_dir = common.raw_dir("prices", "fmap")
    src = Path(src_path).expanduser()
    if not src.is_file():
        raise FileNotFoundError(f"F-MAP source file not found: {src}")
    dest = out_dir / fmap_filename(str(src), common.utc_timestamp())
    copy_local_file(src, dest)
    print(f"  copied {dest.name}  ({dest.stat().st_size:,} bytes)")
    return [dest]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest USDA ERS F-MAP price file(s) (raw, local).")
    parser.add_argument("--from-file", help="Copy this local file instead of downloading.")
    parser.add_argument("--url", nargs="+", help="Override the F-MAP download URL(s).")
    args = parser.parse_args(argv)

    out_dir = common.raw_dir("prices", "fmap")
    print(f"Ingesting F-MAP price file(s) -> {out_dir}")
    if args.from_file:
        saved = ingest_from_file(args.from_file)
    else:
        saved = ingest_download(args.url or FMAP_DOWNLOAD_URLS)
    print(f"Done. Wrote {len(saved)} raw file(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
