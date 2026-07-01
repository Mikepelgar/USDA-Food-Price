# Phase 6 — Polish, CI, Portfolio (+ one feature)

## Context

Phases 0–5 are complete and merged to `main`: the pipeline ingests USDA FDC nutrition, ERS F-MAP
historical prices, and BLS current prices → `data/raw/` → BigQuery `usda_raw` → dbt
(`usda_staging` / `usda_analytics`) → Airflow DAG → Streamlit dashboard + a BLS forecast
(`usda_forecast`). Everything runs on the BigQuery Sandbox (no billing; batch + query jobs only).

Phase 6 is the final phase: **harden, document, and present** what exists, plus exactly **one** new
feature — expand nutrition-per-dollar from 3 nutrients to all ~221. No other new features, no
forecast/orchestration logic changes, stay on the free tier / Sandbox. Commit email
`Mikepelgar@users.noreply.github.com`.

The raw data already holds every nutrient — `data/raw/nutrition` `foodNutrients` items carry
`nutrientNumber`, `nutrientName`, `unitName`, `value` (verified; `usda_raw.raw_nutrition` has ~221
distinct nutrients). No new ingestion/dataset needed.

## PR strategy

Recommended: **two PRs** for a smaller blast radius, since Task 1 is the only behavior/schema change
while Tasks 2–6 are pure polish:

- **PR A — `phase-6-nutrients`:** Task 1 only (dbt LONG redesign + dashboard tab 3). Merge once
  `dbt build` + tests are green.
- **PR B — `phase-6-polish`:** Tasks 2–6 (CI, README, repo review). Written *after* PR A merges, so
  the docs describe the final shape.

Acceptable alternative: a single `phase-6` PR containing everything (the tasks are sequenced
feature-first so this works too). The final docs-only CLAUDE.md update may be committed directly to
`main` per repo convention.

---

## Task 1 — Expand nutrition-per-dollar to every nutrient (WIDE → LONG)

### `transform/models/analytics/dim_nutrition.sql` — pivot to LONG
Replace the hardcoded 8-column `per_food` pivot + 8 `approx_quantiles` columns with a generic unnest
→ median over **every** nutrient. New shape, **grain `(food_category, nutrient_number, unit)`**:

- `nutrients` CTE: unnest `foodNutrients`, carry `nutrient_number`, `nutrient_name`, `unit_name`,
  `safe_cast(value as numeric)`. Keep the existing food filter (Foundation / SR Legacy / Survey;
  `food_category is not null`).
- `per_food_nutrient` CTE: `group by fdc_id, food_category, nutrient_number, unit_name`,
  `any_value(nutrient_name) as nutrient_name`, `max(value) as value` (collapses a food reporting the
  same nutrient under multiple derivations; `where value is not null`).
- final select: `group by food_category, nutrient_number, unit_name`, output
  `food_category, nutrient_number, nutrient_name, unit (= unit_name), n_foods = count(*),
   amount_per_100g = approx_quantiles(value, 2)[safe_offset(1)]`.

Header comment: energy appears as two nutrients (KCAL and kJ) because `unit` is part of the key —
intentional and honest (this is exactly why the old model hard-coded `and unit_name = 'KCAL'`).
Units flow through verbatim (`G`/`MG`/`UG`/`KCAL`/…).

### `transform/models/analytics/fct_nutrition_per_dollar.sql` — fan out per nutrient
Same three CTEs (prices ⋈ crosswalk ⋈ nutrition). Joining the LONG `dim_nutrition` fans each price
row out to one row per nutrient. **New grain `(efpg_code, region_code, month_date, nutrient_number,
unit)`**. Columns:

- carry through `efpg_code, efpg_name, region_code, region_name, year, month_num, month_date,
  mean_unit_value, fdc_food_category`
- `nutrient_number, nutrient_name, unit, amount_per_100g`
- `amount_per_dollar = safe_divide(amount_per_100g, mean_unit_value)`
- replace `protein_rank` with generic
  `nutrient_rank = rank() over (partition by region_code, month_date, nutrient_number, unit
   order by amount_per_dollar desc)`
- add `{{ config(materialized='table', cluster_by=['nutrient_number']) }}` so the dashboard's
  per-nutrient `WHERE nutrient_number = …` scans less. (Polish, not load-bearing — even a full scan
  of this table is well under the Sandbox cap; don't let anything block on it.)

Keep the CAVEAT comment (historical 2012–2018 F-MAP price × static nutrition; lossy crosswalk).

### `transform/models/analytics/_analytics.yml` — tests + docs
- **`dim_nutrition`**: drop the 8 per-nutrient column blocks. New columns/tests: `food_category`
  not_null; `nutrient_number` not_null; `unit` not_null; `nutrient_name` (doc); `n_foods`
  accepted_range min_value 1; `amount_per_100g` not_null + accepted_range min_value 0. Replace
  `unique` on `food_category` with
  `dbt_utils.unique_combination_of_columns: [food_category, nutrient_number, unit]`. Update the model
  description (per-nutrient LONG, one row per food_category × nutrient).
- **`fct_nutrition_per_dollar`**: keep `efpg_code` not_null + relationships→`category_crosswalk`,
  `region_code`/`month_date` not_null. Add `nutrient_number` not_null, `unit` not_null,
  `nutrient_name` doc, `amount_per_dollar` accepted_range min_value 0, `nutrient_rank` doc. Change the
  grain test to `[efpg_code, region_code, month_date, nutrient_number, unit]`. Update the description.
- Note in the model header: dropping the old per-nutrient `max_value` bounds is intentional — a
  single ceiling can't span g/mg/µg/kcal, so we keep only `>= 0` (accepted trade-off: looser sanity
  bounds for generality). The new grain test relies on the crosswalk being one-to-one on
  `fmap_efpg_code` — the old `[efpg, region, month]` unique test passing already proves this, and the
  new test will catch it if that ever changes.

### `dashboard/app.py` — radio → dropdown, filtered reads (tab 3 only)
The per-dollar table is now too large to read whole. Replace `load_nutrition_per_dollar()` with:

- `load_nutrition_menu()` — cached `SELECT DISTINCT nutrient_number, nutrient_name, unit FROM
  fct_nutrition_per_dollar ORDER BY nutrient_name` (small) → dropdown options.
- `load_nutrition_per_dollar(nutrient_number, unit)` — cached, filtered to the selected nutrient
  (≈ all regions × months for one nutrient, ~25k rows); coerce numerics; parse `month_date`. (Each
  nutrient selection is a separate small cached query — fine.)

Rewrite tab 3 (`tab_npd`, ~`dashboard/app.py:349`): swap the 3-way `st.radio` for a `st.selectbox`
over the menu (label = `f"{nutrient_name} ({unit}) / $"`; default to the Protein row —
`nutrient_number == '203'` and `unit == 'G'` — if present). Use generic columns `amount_per_dollar` /
`amount_per_100g`; label axis/tooltip with the carried `unit` so "per dollar" reads correctly (e.g.
"g of Protein / $", "mg of Calcium / $", "kcal of Energy / $"). Keep the region filter, month slider,
top-N bar, and the lossy-crosswalk caption. The other three tabs are unchanged.

### Re-materialize + re-validate
- From `transform/` (venv active): `dbt build --profiles-dir .` → all models build, all tests pass.
  **Record the new test count** (it changes with the grain/column edits).
- `python -m pytest` (mocked; stays green — no Python touches dbt models).
- `streamlit run dashboard/app.py` (or headless `AppTest`) against live BigQuery → tab 3 dropdown
  lists all nutrients, charts populate, other tabs unaffected.
- Airflow note: the Airflow image *bakes* the dbt project (`COPY transform/` + `dbt deps` at build).
  Local `dbt build` is enough to validate Phase 6; the running DAG would only pick up the new models
  after `docker compose up -d --build`. Re-running the stack is **out of scope** here — just note it
  in CLAUDE.md so it isn't forgotten.

---

## Task 2 — GitHub Actions CI (lightweight, credential-free)

New `.github/workflows/ci.yml`, triggers `on: [push, pull_request]`, two parallel jobs on
`ubuntu-latest`, Python 3.11, `actions/setup-python` with `cache: pip`:

1. **`python-tests`**: `pip install -r requirements.txt` → `python -m pytest` (suite is fully mocked;
   `pyproject.toml` already sets `pythonpath=["src"]`, so no install/creds needed).
2. **`dbt-validate`** (credential-free): `pip install "dbt-bigquery>=1.7,<2.0"`; then in
   `working-directory: transform`, `dbt deps` (downloads `dbt_utils`) and `dbt parse`. `parse` builds
   /validates the manifest **without opening a warehouse connection**. Point dbt at a committed dummy
   profile via `DBT_PROFILES_DIR` → new `.github/dbt/profiles.yml`:
   ```yaml
   usda_food_prices:
     target: ci
     outputs:
       ci: { type: bigquery, method: oauth, project: dummy, dataset: dummy, threads: 1, location: US }
   ```

Treat the CI run as the gate, not an assumption. `dbt parse` is the standard credential-free check
and *should* run without auth (BigQuery credential resolution is deferred to connection-open, which
`parse` never reaches). But `method: oauth` can occasionally trigger a `google.auth.default()`
lookup. So: push the branch and **confirm `dbt-validate` actually goes green before considering Task 2
done** (this is verification step 4 — make it blocking). Fallback if `parse` insists on credentials:
keep `dbt deps` (proves packages + project parse-load) and drop `dbt parse` to a
`python -c "from dbt.cli.main import dbtRunner"` import/sanity check rather than switching to
`dbt compile` (which genuinely connects and is *not* credential-free). Do **not** commit a real or
fake keyfile.

Add a CI status badge to the README top.

---

## Task 3 — Portfolio README

Enhance `README.md` in place (already accurate and detailed — preserve the correct per-phase run
docs, condense where wordy). Add/ensure:

- **Top:** one-paragraph project summary + CI badge.
- **Architecture diagram:** keep & polish the existing full-flow ASCII diagram (FDC API + ERS F-MAP
  file + BLS API → `data/raw` → BigQuery `usda_raw` → dbt `usda_staging`/`usda_analytics` → Airflow →
  Streamlit dashboard + forecast). Optionally add a Mermaid `flowchart` (renders on GitHub) alongside
  it.
- **Tech stack** table (Python 3.11, requests/dotenv, BigQuery, dbt, Airflow + Docker Compose,
  Streamlit + scikit-learn, GitHub Actions, pytest).
- **Setup** + **how to run each phase** (already present — keep, tighten).
- **Update the analytics-table description tables.** The existing README table describes
  `dim_nutrition` as "median nutrients per 100 g" and `fct_nutrition_per_dollar` as "ranked by
  protein/$". Rewrite both to the **LONG / per-nutrient** shape (one row per category × nutrient;
  per-dollar for any nutrient; new grains).
- **Data caveats** (consolidate, honest): F-MAP is historical 2012–2018; BLS is current but U.S.
  city-average (no regional breakdown); **BLS is not USDA**; the FDC↔F-MAP nutrition crosswalk is
  intentionally lossy.
- **Results** section (see Task 4).

---

## Task 4 — Results section with labeled placeholders

In README **Results**, a table with clearly-labeled fill-in placeholders (e.g. `_(fill in)_`) for
metrics the user supplies from real runs. **Do not invent numbers.**

- Total records processed
- # food categories · # regions
- # nutrients surfaced (≈221)
- Pipeline run time
- Pipeline success rate
- # data-quality (dbt) tests *(use the real count from Task 1's `dbt build`)*
- Forecast accuracy — MAPE vs. naive baseline *(dashboard/forecast already report ≈2.8% vs ≈2.1%;
  mark as a confirmable placeholder)*

---

## Task 5 — Repo review (harden, no new features)

- **Secrets/tracking:** confirm with `git ls-files` (not just `.gitignore`) that only `.env.example`
  + `secrets/.gitkeep` are tracked — no `.env`, service-account JSON, or keys. `.gitignore` covers
  `.env`, `secrets/*`, `data/`, `*service-account*.json`. Re-confirm after the changes.
- **TODO/FIXME:** none expected in source — confirm again after edits.
- **Widen the dead-code grep.** Confirm no references remain to the removed columns, in **dbt,
  dashboard, README, and CLAUDE.md**:
  - per-dollar: `protein_g_per_dollar`, `energy_kcal_per_dollar`, `fiber_g_per_dollar`, `protein_rank`
  - removed `dim_nutrition` wide columns: `protein_g_per_100g`, `energy_kcal_per_100g`,
    `total_fat_g_per_100g`, `carbs_g_per_100g`, `fiber_g_per_100g`, `calcium_mg_per_100g`,
    `iron_mg_per_100g`, `sodium_mg_per_100g`
- **Naming / dead code:** note anything unprofessional found; fix small items inline.

---

## Task 6 — Final CLAUDE.md update

Mark **all phases (0–6) complete** in "Current state". Tighten the long per-phase build-history into a
concise finished-project summary (what exists, how to run, the Sandbox/billing note, the data
caveats). Record: the Task-1 schema change (**LONG `dim_nutrition`**; per-nutrient
`fct_nutrition_per_dollar`; new grains; the real updated dbt test count); the new CI workflow; and the
**Airflow-rebuild note** from Task 1. Keep it accurate, not a changelog. (Docs-only CLAUDE.md update
may go directly to `main`; code changes go via the phase-6 PR(s).)

---

## Verification (end-to-end)

1. `dbt build --profiles-dir .` from `transform/` → all models build; **all tests pass** (record the
   new count).
2. `python -m pytest` → green (mocked).
3. Dashboard (live BigQuery): nutrient dropdown reachable for all nutrients, units labelled
   correctly, other tabs unchanged.
4. **Blocking:** push the branch and confirm **both** CI jobs (`python-tests`, `dbt-validate`) go
   green on the PR before calling Task 2 done.
5. `git ls-files` → still no secrets tracked; grep (full list above) → no stale wide-column /
   `protein_rank` refs anywhere.

## Files
- `transform/models/analytics/dim_nutrition.sql` (rewrite to LONG)
- `transform/models/analytics/fct_nutrition_per_dollar.sql` (fan out per nutrient + cluster_by)
- `transform/models/analytics/_analytics.yml` (tests + docs for both)
- `dashboard/app.py` (tab 3: menu + filtered read, radio → dropdown)
- `.github/workflows/ci.yml` (new) · `.github/dbt/profiles.yml` (new dummy CI profile)
- `README.md` (portfolio pass + analytics-table table updates + Results placeholders + CI badge)
- `CLAUDE.md` (final summary)

## Out of scope (explicitly not doing)
Phase-6 backlog items: loader `--latest-only`, wiring the forecast into the Airflow DAG, adding
`google-cloud-bigquery-storage`. No other new features. Re-running the Airflow stack
(`docker compose up --build`) to pick up the new models is noted but not performed here.
