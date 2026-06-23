"""Phase 2: load Phase-1 raw files into BigQuery raw tables.

The only module here is :mod:`bigquery_loader`, which reads the raw files under
``data/raw/`` and batch-loads them essentially as-is into three raw tables.
No cleaning, joining, or transformation happens here — that is Phase 3 (dbt).
"""
