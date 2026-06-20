# CLAUDE.md

Guidance for Claude Code (and humans) working in this repository.

## Project goal

An automated data pipeline that pulls US food **price** (USDA ERS via api.data.gov) and
**nutrition** (USDA FoodData Central) data from public USDA APIs and loads it into a cloud
data warehouse. The warehouse data is transformed into analytics-ready tables that power a
**dashboard** and a **food-price forecast**.

## Planned architecture & tech stack

```
USDA APIs            Ingestion          Warehouse         Transform         Serve
─────────            ─────────          ─────────         ─────────         ─────
FoodData Central ─┐
(nutrition)       ├─► Python scripts ─► raw tables ────► cleaned/ ───────► Dashboard
ERS via           │   (requests)        (BigQuery)       modeled tables    + Price forecast
api.data.gov ─────┘                                      (SQL)
```

- **Language:** Python 3.11 (venv at `.venv/`).
- **Ingestion:** `requests` for HTTP; `python-dotenv` for config; `pytest` for tests.
  Only these three dependencies exist today — add more per phase, not preemptively.
- **Warehouse:** Google Cloud **BigQuery** (assumed; authenticated via a service-account
  JSON referenced by `GOOGLE_APPLICATION_CREDENTIALS`). Revisit if the user prefers
  another warehouse.
- **Transformation:** SQL over warehouse tables (raw → cleaned/modeled). Tooling (e.g. dbt)
  to be decided in Phase 2.
- **Serving:** a dashboard (tool TBD) plus a price-forecasting model (library TBD).

## Phase plan

1. **Phase 1 — Ingestion (next):** Python scripts that call the two USDA APIs and land raw
   responses in the warehouse. Add warehouse-client deps when this phase starts.
2. **Phase 2 — Transformation:** SQL models turning raw data into clean, analytics-ready
   tables.
3. **Phase 3 — Serving:** dashboard for trends + a food-price forecast model.

Build phases one at a time. Do not write ingestion, transformation, or dashboard code until
the corresponding phase is explicitly started.

## Conventions

- **Secrets:**
  - Real secrets live in `.env` (gitignored). `.env.example` is the committed template.
  - The Google Cloud service-account JSON lives in `secrets/` (gitignored except
    `.gitkeep`). Never commit credentials.
  - Required env vars: `FDC_API_KEY`, `ERS_API_KEY`, `GOOGLE_APPLICATION_CREDENTIALS`.
- **Project layout (src layout):**
  - `src/usda_food_price_pipeline/` — the importable Python package.
  - `src/usda_food_price_pipeline/ingestion/` — ingestion scripts.
  - `config/` — non-secret configuration. `tests/` — pytest tests. `docs/` — documentation.
- **Naming:** package/module/function names are `snake_case`; directories `snake_case`.
  Tests live in `tests/` as `test_*.py`. The distribution/repo name uses hyphens
  (`usda-food-price-pipeline`); the importable package uses underscores
  (`usda_food_price_pipeline`).
- **Dependencies:** keep `requirements.txt` minimal and phase-scoped; document why each is
  added.

## Current state

Only the **scaffold** exists: repository structure, virtual environment, ingestion-phase
`requirements.txt`, env templates, `.gitignore`, README, and this file. No ingestion,
transformation, or dashboard code has been written yet.

**Next:** Phase 1 — ingestion.
