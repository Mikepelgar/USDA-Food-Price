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

**Phase 0 (scaffold) is COMPLETE and fully provisioned** (as of 2026-06-20):

- Repository structure, virtual environment, ingestion-phase `requirements.txt`, env
  templates, `.gitignore`, README, and this file all exist.
- Git initialized and pushed to GitHub: https://github.com/Mikepelgar/USDA-Food-Price
  (branch `main`; repo-local commit email `Mikepelgar@users.noreply.github.com`).
- **All three credentials are filled in and verified to authenticate** (checked
  2026-06-20): `FDC_API_KEY` (FoodData Central, 200 OK), `ERS_API_KEY` (ERS ARMS API,
  200 OK), and the GCP service-account JSON at `secrets/gcp-service-account.json`
  (BigQuery token minted + dry-run query 200 OK; project `usda-food-prices`).
- No ingestion, transformation, or dashboard code has been written yet.

**Venv note:** `google-auth` was installed into `.venv` ad hoc for the credential check
but is intentionally NOT in `requirements.txt`. In Phase 1, add the real warehouse
client (likely `google-cloud-bigquery`) to `requirements.txt` properly.

**Billing note:** the user wants to stay on the **free tier only**. Confirm BigQuery
billing posture before heavy use (Sandbox = no billing account = cannot be charged;
otherwise rely on free tier + budget alerts). USDA APIs are free, capped at 1,000
requests/hour per key (HTTP 429 when exceeded).

**Next: Phase 1 — ingestion.** Build the scripts under
`src/usda_food_price_pipeline/ingestion/` that pull from the two USDA APIs and batch-load
raw data into BigQuery. (Batch loads are free; avoid streaming inserts, which cost money.)
