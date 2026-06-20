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
- **Warehouse:** Google Cloud **BigQuery** (permanent free tier; authenticated via the
  service-account JSON referenced by `GOOGLE_APPLICATION_CREDENTIALS`).
- **Transformation:** **dbt** on BigQuery.
- **Orchestration:** **Apache Airflow**, run locally via **Docker** (Docker Compose).
- **CI:** **GitHub Actions**.
- **Serving:** **Streamlit** dashboard + a price forecast using **scikit-learn** (or a
  simple statistical time-series model).

## Phase plan

0. **Phase 0 — Scaffold (DONE):** repo structure, venv, env templates, git + GitHub.
1. **Phase 1 — Ingestion → LOCAL FILES ONLY:** Python scripts pull from both USDA APIs and
   save raw, timestamped JSON to `data/raw/nutrition/` and `data/raw/prices/`. **No cloud,
   warehouse, orchestration, or dashboard code in this phase.**
2. **Phase 2 — Load to BigQuery:** a loader reads the raw JSON files and loads them as-is
   into raw/staging tables in BigQuery (idempotent re-runs). No transformation yet.
3. **Phase 3 — Transformation (dbt on BigQuery):** staging + analytics models, including the
   nutrition-per-dollar join; dbt tests and docs.
4. **Phase 4 — Orchestration (Airflow + Docker):** one DAG runs ingest → load → `dbt build`
   → `dbt test` on a daily schedule, with retries; containerized via Docker Compose.
5. **Phase 5 — Dashboard + forecast (Streamlit + scikit-learn):** dashboard over the
   analytics tables; next-month price forecast written back to BigQuery.
6. **Phase 6 — Polish, CI, portfolio:** GitHub Actions CI, portfolio-quality README, repo
   cleanup.

Build phases one at a time. Do not write code for a later phase until that phase is
explicitly started — keep each session focused on its single phase.

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

## API reference (endpoints verified working in Phase 0)

These exact calls returned HTTP 200 with the project's real keys on 2026-06-20.

- **USDA FoodData Central (nutrition)** — base `https://api.nal.usda.gov/fdc/v1`
  - Verified: `GET /foods/search?query=<term>&pageSize=<n>&api_key=$FDC_API_KEY`
  - Other documented endpoints: `/food/{fdcId}`, `/foods`, `/foods/list`.
  - Docs: https://fdc.nal.usda.gov/api-guide.html
- **USDA ERS (price data)** — intended dataset is **Food-at-Home Monthly Area Prices**,
  accessed via the api.data.gov service using `$ERS_API_KEY` (an api.data.gov key).
  - Key validated in Phase 0 against the ERS ARMS API base
    `https://api.ers.usda.gov/data/arms` (`GET /year` → 200), which only confirms the key
    works. In Phase 1, confirm the exact endpoint/dataset for **Food-at-Home Monthly Area
    Prices** (monthly food prices by category and region) — that, not ARMS, is the target.
  - Docs: https://www.ers.usda.gov/developer/data-apis/
- **BigQuery REST** — `https://bigquery.googleapis.com/bigquery/v2/projects/usda-food-prices/...`
  - Auth: mint an OAuth token from `secrets/gcp-service-account.json` (scope
    `https://www.googleapis.com/auth/bigquery`). A dry-run query job returned 200.
- Both USDA keys are api.data.gov keys: rate-limited to 1,000 requests/hour (HTTP 429).

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

**Next: Phase 1 — ingestion → LOCAL FILES ONLY.** Build Python scripts under
`src/usda_food_price_pipeline/ingestion/` that pull from both USDA APIs and save raw,
timestamped JSON to `data/raw/nutrition/` and `data/raw/prices/`. Handle pagination, the
1,000 req/hour rate limit, and retries with backoff; add unit tests for the non-network
logic (mock the HTTP calls). **Do NOT add BigQuery, cloud, orchestration, or dashboard code
in Phase 1 — BigQuery loading is Phase 2.**
