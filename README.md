# USDA Food Price & Nutrition Pipeline

## Goal

An automated data pipeline that pulls US food price and nutrition data from public
USDA APIs and loads it into a cloud data warehouse. The warehouse data is transformed
into analytics-ready tables that power a dashboard and a food-price forecast.

## Planned Architecture (high level)

```
USDA APIs            Ingestion          Warehouse         Transform         Serve
─────────            ─────────          ─────────         ─────────         ─────
FoodData Central ─┐
(nutrition)       ├─► Python scripts ─► raw tables ────► cleaned/ ───────► Dashboard
ERS via           │   (requests)        (BigQuery)       modeled tables    + Price forecast
api.data.gov ─────┘                                      (SQL)
(prices)
```

1. **Ingestion** — Python scripts call the USDA FoodData Central and ERS APIs and land
   raw responses in the cloud warehouse.
2. **Warehouse** — Google Cloud (BigQuery) holds raw and transformed data.
3. **Transformation** — SQL transforms raw data into clean, modeled tables.
4. **Serving** — a dashboard visualizes trends; a forecasting model projects food prices.

> Status: project skeleton only. Ingestion, transformation, and dashboard code come in
> later phases.

## Setup

```bash
# Create and activate the virtual environment (Windows PowerShell)
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Install ingestion-phase dependencies
pip install -r requirements.txt

# Configure secrets
copy .env.example .env   # then fill in your keys
```

## Repository Layout

| Path                              | Purpose                                              |
| --------------------------------- | ---------------------------------------------------- |
| `src/usda_food_price_pipeline/`   | Python package (importable source code)              |
| `src/.../ingestion/`              | Ingestion scripts that pull from the USDA APIs       |
| `config/`                         | Non-secret configuration files                       |
| `tests/`                          | Automated tests (pytest)                             |
| `docs/`                           | Project documentation                                |
| `secrets/`                        | Local-only credentials (gitignored)                  |
| `.env.example` / `.env`           | Environment-variable template / your real values     |
| `requirements.txt`                | Python dependencies                                  |
