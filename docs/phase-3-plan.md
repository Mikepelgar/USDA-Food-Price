# Phase 3 — Transformation with dbt on BigQuery (revised plan)

> Revised after review. Changes vs the first draft are called out inline as **[REV]**.
> Start the Phase 3 session from this document.

## Context

The USDA Food Price Pipeline (`C:\Users\mikep\usda-food-price-pipeline`) has finished
Phases 0–2: ingestion writes raw files locally, and the Phase-2 loader batch-loads them
**verbatim** into three raw tables in BigQuery dataset `usda_raw` (project `usda-food-prices`,
location `US`, running in free-tier **Sandbox** = cannot be billed):

| Raw table | Rows | Shape (all payloads are raw JSON strings — nothing typed/cleaned/joined) |
| --- | --- | --- |
| `raw_nutrition` | 1,500 | `source_file, fdc_id INT64, raw_json STRING, loaded_at`. `raw_json` = full FDC food object: `description, dataType, foodCategory, foodNutrients[]` (`nutrientName/nutrientNumber, value, unitName`; values are per **100 g**). |
| `raw_prices_bls` | 327 | `source_file, series_id, year, period, period_name, value, latest, footnotes, loaded_at` — **all STRING**. `series_id`→item name lives only in Python (`DEFAULT_SERIES`, 8 APU series). `period` `M01`–`M12` monthly, `M13` = annual avg. |
| `raw_prices_fmap` | 162,262 | `source_file, sheet_name, row_index, raw_json STRING, loaded_at`. `raw_json` = one `{header:cell}` record per worksheet row, across `ReadMe`+`Data` sheets of **2 different workbooks** (main 2012–2018 + supplemental price-indexes 2016–2018). |

Phase 3 is the missing **Transform** stage: turn those raw JSON blobs into typed, cleaned,
documented, tested analytics tables. By design Phase 2 did **no** cleaning/casting/joining —
it all belongs here. This session sets up the dbt project and writes the models/tests/docs so
the user can run `dbt build`/`dbt test` and inspect the output tables before we move on.

**Out of scope (do NOT build this session):** any change to ingestion or the loader; Airflow/
Docker orchestration (Phase 4); the Streamlit dashboard or forecast (Phase 5); anything that
leaves free-tier (batch/Sandbox only — dbt issues query jobs, free under the 1 TB/mo tier and
fine in Sandbox). No git commit/push — the user runs and inspects first.

**Confirmed choices:** dbt project lives at `transform/`; the category crosswalk is a dbt
**seed** at `transform/seeds/category_crosswalk.csv`.

---

## Implementation

### Step 0 — Verify raw shapes before writing SQL (cheap, read-only) — HARD GATE
CLAUDE.md's column lists are point-in-time notes; confirm the *actual* keys first. Run these
free preview queries (`LIMIT`/metadata, well under free tier) in the BigQuery console or `bq`.
**The F-MAP design below depends on the answers — make these decisions here, not while coding:**

- `SELECT DISTINCT sheet_name FROM usda_raw.raw_prices_fmap;`
- `SELECT DISTINCT source_file FROM usda_raw.raw_prices_fmap;` — **[REV]** confirm the two
  workbooks are distinguishable by `source_file` (main basename vs the one containing
  `supplemental`). This filter is how we separate them.
- Main workbook keys: `SELECT raw_json FROM usda_raw.raw_prices_fmap WHERE sheet_name='Data'
  AND source_file NOT LIKE '%supplemental%' LIMIT 5;`
- Supplemental keys: `SELECT raw_json FROM usda_raw.raw_prices_fmap WHERE
  source_file LIKE '%supplemental%' AND sheet_name = <its data sheet> LIMIT 5;` — **[REV]**
  the supplemental may name its data sheet differently and almost certainly has *different
  columns* (price indexes, not dollars/grams). Capture its exact key for the index value and
  its join keys (category/region/year/month).
- `SELECT DISTINCT series_id FROM usda_raw.raw_prices_bls;`
- **[REV]** Author the crosswalk from the **cleaned** category strings, so they match the
  staging output exactly. Run the distinct queries with the same normalization the staging
  models will apply, e.g.:
  - `SELECT DISTINCT LOWER(TRIM(JSON_VALUE(raw_json,'$.foodCategory'))) FROM usda_raw.raw_nutrition;`
  - `SELECT DISTINCT LOWER(TRIM(JSON_VALUE(raw_json,'$.EFPG_name'))) FROM usda_raw.raw_prices_fmap
     WHERE sheet_name='Data' AND source_file NOT LIKE '%supplemental%';`

Expected main-workbook keys (per CLAUDE.md, to be confirmed): `Year, Month, EFPG_name,
EFPG_code, Metroregion_name, Metroregion_code, Purchase_dollars_wtd, Purchase_grams_wtd`.
Adjust all SQL to whatever Step 0 actually shows.

### Step 1 — dbt project scaffold (`transform/`)
Create a standard dbt-BigQuery project. Files:
- `transform/dbt_project.yml` — **[REV]** `config-version: 2`, `name: usda_food_prices`,
  `profile: usda_food_prices`, `model-paths: [models]`, `seed-paths: [seeds]`. Model config:
  `staging` → `+schema: usda_staging` `+materialized: view`; `analytics` → `+schema:
  usda_analytics` `+materialized: table`. **[REV]** Seeds also pinned:
  `seeds: usda_food_prices: +schema: usda_analytics`. Optionally `require-dbt-version`.
- **[REV]** `transform/macros/generate_schema_name.sql` — the standard "use the custom schema
  **verbatim**" override. With this macro, `+schema: usda_staging` yields dataset
  `usda_staging` exactly (NOT `<target>_usda_staging`). **This is why the `+schema:` values
  above are the full dataset names**, not `staging`/`analytics` — the earlier draft's
  `+schema: staging` would have produced a dataset literally named `staging`.
- `transform/profiles.example.yml` — committed template (mirrors the repo's `.env.example`
  pattern). BigQuery, `method: service-account`, `keyfile: ../secrets/gcp-service-account.json`,
  `project: usda-food-prices`, `dataset: usda_analytics` (default target schema), `location:
  US`, `threads: 4`. The **real** `profiles.yml` is created by the user and is gitignored.
- `transform/packages.yml` — **[REV]** pin it: `dbt-labs/dbt_utils, version: [">=1.1.0",
  "<2.0.0"]` (provides `accepted_range`, `unique_combination_of_columns`, `relationships`
  helpers used below). `dbt deps` installs it.

**Dataset names:** profile `dataset: usda_analytics` is the default; the verbatim macro +
full-name `+schema:` configs route staging models to `usda_staging`, and analytics models +
seeds to `usda_analytics`. dbt creates both datasets on first run (they inherit Sandbox's
60-day expiry — re-run `dbt build` to recreate after expiry).

### Step 2 — Sources (`transform/models/staging/_sources.yml`)
Declare one dbt `source` `usda_raw` with the three raw tables, so staging models use
`source('usda_raw','raw_nutrition')` etc. (decouples models from hard-coded dataset names and
documents the lineage root).

### Step 3 — Staging models (`models/staging/`, materialized as views)
Each: parse JSON, enforce types with **`SAFE_CAST`** **[REV]** (raw values are strings that may
contain blanks/footnote markers — a hard `CAST` would fail the whole run), standardize
category/region names (`TRIM` + consistent case), handle nulls, de-duplicate. **[REV]** Dedup
is NOT merely defensive: re-running ingestion + the `WRITE_TRUNCATE` loader reloads *all* raw
files in a folder, so duplicate `source_file`s are real — order the window
`ROW_NUMBER() OVER (PARTITION BY <grain> ORDER BY loaded_at DESC, source_file DESC)` so the
newest wins.

- **`stg_nutrition.sql`** ← `source('usda_raw','raw_nutrition')`. One row per `fdc_id`:
  `fdc_id`, `description` (trimmed), `data_type`, `food_category`
  (`LOWER(TRIM(JSON_VALUE(raw_json,'$.foodCategory')))`), and the `food_nutrients` array kept
  as JSON (`JSON_QUERY(raw_json,'$.foodNutrients')`) for the analytics pivot. Dedup on `fdc_id`.
- **`stg_prices_bls.sql`** ← `raw_prices_bls`. `SAFE_CAST(value AS NUMERIC)` → `price_usd`,
  `SAFE_CAST(year AS INT64)`; derive `month_num` from `period` (`M01`→1) and
  `month_date = DATE(year, month_num, 1)`; **keep only `M01`–`M12`** (drop `M13` annual avg);
  join the **`bls_series_items`** seed for `item_label`/`unit`; `SAFE_CAST(latest AS BOOL)` →
  `is_latest`. Dedup on `(series_id, year, period)`.
- **`stg_prices_fmap.sql`** ← `raw_prices_fmap`, **[REV]** filtered to `sheet_name='Data' AND
  source_file NOT LIKE '%supplemental%'` (main workbook only — this is the dollars/grams
  source). Parse keys via `JSON_VALUE` + `SAFE_CAST`; build `month_date`; standardize
  `efpg_name`/`region_name` (LOWER/TRIM to match the crosswalk); compute
  `mean_unit_value = 100 * Purchase_dollars_wtd / NULLIF(Purchase_grams_wtd,0)` ($ per **100 g**,
  matching FDC's per-100 g basis). Drop rows with null/zero grams. Dedup on
  `(efpg_code, region_code, year, month)`.
- **[REV] NEW `stg_fmap_price_index.sql`** ← `raw_prices_fmap`, filtered to the supplemental
  workbook (`source_file LIKE '%supplemental%'`, its data sheet from Step 0). Parse only the
  join keys + the price-index column(s); build `month_date`; standardize keys. Dedup on the
  supplemental's grain. **Kept SEPARATE from `stg_prices_fmap` on purpose** — the two workbooks
  have different columns and overlapping years (2016–2018), so a union+dedup on a shared grain
  would clobber dollars/grams vs index. They are *joined* in Step 5, not unioned.

### Step 4 — Seeds (`transform/seeds/`)
- **`category_crosswalk.csv`** — columns `fmap_efpg_name, fmap_efpg_code, fdc_food_category,
  notes`. **[REV]** Dropped the ambiguous `canonical_category` from the first draft — the join
  uses `fdc_food_category`, so a separate canonical column was decorative. Keyed on the
  **F-MAP category** (granular, price-bearing, unique per row), mapping each to the
  best-matching broad FDC `foodCategory` that `dim_nutrition` is keyed on. **[REV]** Values
  authored from the Step-0 **cleaned** distinct lists (post LOWER/TRIM) so the joins hit.
  Small, curated, intentionally-lossy: covers only the overlap of the 15 FDC queries × F-MAP
  categories (eggs, milk, bread, flour, ground beef, chicken, bananas, potatoes, apple,
  broccoli, spinach, rice, beans, …). F-MAP categories with no FDC match are omitted (they
  drop out of the join). **Documented limitation:** FDC `foodCategory` is broad ("Dairy and Egg
  Products") while F-MAP is specific ("Eggs", "Whole milk"), so several priced categories can
  share one nutrition profile — a deliberate simplification, refinable later by mapping to
  representative foods. The header/`_seeds.yml` explains the crosswalk's purpose + the mismatch.
- **`bls_series_items.csv`** — `series_id, item_label, unit`, mirroring `DEFAULT_SERIES` in
  `src/usda_food_price_pipeline/ingestion/prices_bls.py` (8 rows). **[REV]** Use the
  **corrected** Phase-1 basket — eggs, milk, bread, flour, **ground beef** (`APU0000703112`),
  **chicken breast boneless** (`APU0000FF1101`), bananas, **white potatoes**
  (`APU0000712112`). Do NOT copy the pre-fix list (the old "potatoes" code `717311` was
  actually coffee). Brings series→food into dbt as a testable table instead of an inlined CASE.

### Step 5 — Analytics models (`models/analytics/`, materialized as tables)
- **`fct_fmap_prices.sql`** — clean monthly food price by category × region. **[REV]** From
  `stg_prices_fmap` **LEFT JOIN** `stg_fmap_price_index` on
  `(efpg_code, region_code, month_date)` so the 2016–2018 index rides alongside dollars/grams
  without clobbering it (index is null for 2012–2015). Grain **(efpg_code, region_code,
  month_date)**: `efpg_code, efpg_name, region_code, region_name, year, month_num, month_date,
  mean_unit_value, price_index`.
- **`fct_bls_prices.sql`** — clean current monthly price series by item (from
  `stg_prices_bls`) — **the series Phase 5's forecast will consume.** Grain
  **(series_id, month_date)**: `series_id, item_label, unit, year, month_num, month_date,
  price_usd, is_latest`.
- **`dim_nutrition.sql`** — clean nutrition keyed by FDC food category (from `stg_nutrition`).
  `UNNEST(JSON_QUERY_ARRAY(food_nutrients))`, filter to target nutrient numbers (203 protein,
  208 energy kcal, 204 fat, 205 carbs, 291 fiber; + a couple of micros e.g. 301 calcium,
  303 iron), pivot to columns, then aggregate across the foods in each category. **[REV]
  Quality:** filter to `data_type IN ('Foundation','SR Legacy','Survey')` (exclude noisy
  `Branded`); **dedup multiple energy rows** per food before pivoting (208 can co-exist with
  kJ/Atwater variants — pick one); consider median (`APPROX_QUANTILES(x,2)[OFFSET(1)]`) instead
  of `AVG` to resist outliers. Grain **(food_category)**: `food_category, n_foods,
  protein_g_per_100g, energy_kcal_per_100g, total_fat_g_per_100g, carbs_g_per_100g,
  fiber_g_per_100g, …`.
- **`fct_nutrition_per_dollar.sql`** — the combined model. Join `fct_fmap_prices` →
  `category_crosswalk` (on `efpg_name`/`efpg_code`) → `dim_nutrition` (on `fdc_food_category`).
  Both price and nutrition use a **per-100 g** basis, so the 100 g cancels:
  `nutrient_per_dollar = nutrient_g_per_100g / mean_unit_value` (mean_unit_value is $/100 g →
  result is grams of nutrient per dollar). Emit `protein_g_per_dollar` (headline), plus
  calories/fiber per dollar, and `protein_rank = RANK() OVER (PARTITION BY region_code,
  month_date ORDER BY protein_g_per_dollar DESC)`. Grain **(efpg_code, region_code,
  month_date)**. **[REV]** Model `description:` must state this uses **F-MAP 2012–2018**
  (historical) prices with static nutrition — it is NOT today's prices; `fct_bls_prices` is the
  current/forecastable feed.

### Step 6 — Tests & docs (`_models.yml` in staging/ and analytics/)
Tests (built-in + `dbt_utils`):
- **not_null + unique on keys:** single keys via built-ins; composite grains via
  `dbt_utils.unique_combination_of_columns`.
- **Uniqueness on the F-MAP category-month-region grain** (explicitly requested):
  `dbt_utils.unique_combination_of_columns(['efpg_code','region_code','month_date'])` on
  `fct_fmap_prices`.
- **accepted_range:** `dbt_utils.accepted_range(min_value:0)` on every price/value column
  (`mean_unit_value`, `price_usd`, `protein_g_per_dollar`); plausible upper bounds on
  `dim_nutrition` (protein 0–100 g/100 g, energy 0–900 kcal/100 g).
- **relationships (clean, total ones only):** `stg_prices_bls.series_id →
  bls_series_items.series_id` (every series is in the seed); `fct_nutrition_per_dollar.efpg_code
  → category_crosswalk.fmap_efpg_code` (the join only emits matched rows).
- **[REV] Do NOT** put a strict `relationships` test from `category_crosswalk` →
  `fct_fmap_prices` (or the reverse). The crosswalk is **intentionally lossy** — unmatched/
  null-grams-dropped categories would throw false failures. If you want coverage there, scope
  it with a `where:` filter or `config: {severity: warn}`.

Docs: model- and column-level `description:` on all analytics tables (and lighter descriptions
on staging + seeds), so `dbt docs generate` produces the catalog the task asks for.

### Step 7 — Wiring & docs updates
- **`requirements.txt`** — **[REV]** add under `# --- Phase 3: transformation (dbt on BigQuery)
  ---`: `dbt-bigquery>=1.7,<2.0` (pulls in `dbt-core`), matching the file's bare-name +
  comment style, but pinned for reproducibility.
- **`.gitignore`** — Phase-3 block: `transform/profiles.yml`, `transform/target/`,
  `transform/dbt_packages/`, `transform/logs/`, `transform/.user.yml` (keep secrets + build
  artifacts out of git; the committed `profiles.example.yml` stays tracked).
- **`README.md`** — new "Phase 3 — Transformation (dbt)" section: `pip install -r
  requirements.txt`; copy `transform/profiles.example.yml` → `~/.dbt/profiles.yml` (or
  `transform/profiles.yml` run with `--profiles-dir .`); `dbt deps`; `dbt debug`; `dbt build`
  (runs seeds → models → tests in dependency order); `dbt docs generate && dbt docs serve`.
  **[REV]** Note `dbt seed`/`dbt run`/`dbt test` exist for running stages individually, but
  `dbt build` already does all three in order — they're not prerequisites. Include the beginner
  explanation of how dbt connects to BigQuery (below).
- **`CLAUDE.md`** — in **What exists**, add a Phase-3 block: dbt project at `transform/`
  (profiles handling, `usda_staging`/`usda_analytics` datasets via the verbatim schema macro);
  the **four** staging models (incl. the separate `stg_fmap_price_index`); the four analytics
  models — call out `fct_nutrition_per_dollar`, its grain `(efpg_code, region_code,
  month_date)`, the per-100 g unit basis + **historical-price caveat**, and the
  `category_crosswalk` seed + its broad-FDC-category limitation; the `bls_series_items` seed;
  the test suite. Flip **Current state / Next** to **Phase 4 — Orchestration (Airflow +
  Docker)**. Convert dates to absolute. Per repo convention CLAUDE.md is committed to `main` at
  session end — **deferred** here until after the user validates (they run/inspect first), then
  shipped on the Phase-3 branch + PR per the established workflow.

### Beginner explanation to include (README + my summary)
- **profiles.yml = dbt's database connection file.** `dbt_project.yml` (in the repo) names a
  `profile`; dbt looks it up in `profiles.yml` to learn *which warehouse, which project/dataset,
  and which credentials*. Keeping it separate lets the same SQL run against different targets
  without code changes.
- **How it connects to BigQuery:** `type: bigquery`, `method: service-account`, and `keyfile:`
  pointing at `secrets/gcp-service-account.json` — the **same** key Phase 2 used via
  `GOOGLE_APPLICATION_CREDENTIALS`. dbt mints a token from that key and runs query jobs in
  project `usda-food-prices`.
- **Keeping secrets out of git:** the keyfile is already gitignored. dbt's default home for
  `profiles.yml` is `~/.dbt/profiles.yml` — *outside* the repo, never committed; we commit only
  `profiles.example.yml`. (If kept inside `transform/`, it's gitignored.) profiles stores only a
  *path* to the key, not the key's contents.

---

## Verification (user-run, after I write the files)
From `transform/` in the activated `.venv`:
1. `pip install -r ../requirements.txt` then `dbt deps` → installs `dbt-bigquery` + `dbt_utils`.
2. `dbt debug` → confirms profiles.yml + BigQuery auth ("Connection test: OK").
3. `dbt build` → seeds, builds 4 staging views + 4 analytics tables, runs all tests in
   dependency order; expect all PASS.
4. Inspect output in BigQuery console (datasets `usda_staging`, `usda_analytics`):
   - `SELECT * FROM usda_analytics.fct_fmap_prices WHERE price_index IS NOT NULL LIMIT 20;`
     **[REV]** (confirms the supplemental index joined onto 2016–2018 rows)
   - `SELECT * FROM usda_analytics.fct_bls_prices ORDER BY month_date DESC LIMIT 20;`
   - `SELECT food_category, n_foods, protein_g_per_100g FROM usda_analytics.dim_nutrition;`
   - `SELECT region_name, month_date, efpg_name, protein_g_per_dollar, protein_rank
      FROM usda_analytics.fct_nutrition_per_dollar
      WHERE protein_rank <= 5 ORDER BY region_name, month_date, protein_rank LIMIT 30;`
5. `dbt docs generate && dbt docs serve` → browse model/column docs + lineage graph.
6. Iterate with targeted re-runs: `dbt build --select fct_nutrition_per_dollar+`.

## Deferred to later (explicitly not now)
Git commit/PR for Phase 3 (after the user validates → branch + PR per the established
workflow); Phase 4 Airflow/Docker; Phase 5 dashboard/forecast.

---

## Summary of revisions from review
1. **Schema names** — use full `+schema: usda_staging`/`usda_analytics` (the verbatim macro
   makes `+schema:` literal); seeds pinned to `usda_analytics`.
2. **Two F-MAP workbooks** — split into `stg_prices_fmap` (main, dollars/grams) +
   `stg_fmap_price_index` (supplemental, indexes), joined via LEFT JOIN in `fct_fmap_prices`
   instead of union+dedup (which would clobber overlapping 2016–2018 rows).
3. **Crosswalk** authored from **cleaned** distinct values; dropped the unused
   `canonical_category` column.
4. **Relationship tests** — keep only the total/safe ones; drop strict tests across the lossy
   crosswalk join (or set `severity: warn`).
5. **dim_nutrition quality** — exclude `Branded`, dedup energy rows, prefer median over mean.
6. **SAFE_CAST** everywhere + deterministic dedup (`ORDER BY loaded_at DESC`).
7. **bls_series_items** uses the corrected Phase-1 basket (white potatoes, not coffee).
8. **Pin versions** — `dbt_utils` in packages.yml, `dbt-bigquery` in requirements.txt.
9. **Historical-price caveat** documented on `fct_nutrition_per_dollar`.
