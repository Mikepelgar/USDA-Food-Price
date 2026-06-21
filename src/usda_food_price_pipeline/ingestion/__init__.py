"""Ingestion package: scripts that pull raw data to local files (Phase 1).

Modules:
  - ``common``        — shared HTTP/retry/rate-limit/path helpers.
  - ``nutrition_fdc`` — FoodData Central nutrition -> data/raw/nutrition/.
  - ``prices_fmap``   — ERS F-MAP file download -> data/raw/prices/fmap/.
  - ``prices_bls``    — BLS Average Price API -> data/raw/prices/bls/.
"""
