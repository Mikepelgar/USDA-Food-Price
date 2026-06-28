"""Phase 5: Streamlit dashboard over the BigQuery analytics tables.

Read-only serving layer. It queries the dbt-built tables in ``usda_analytics`` (kept
fresh by the Phase-4 Airflow pipeline) plus the Python-written ``usda_forecast`` table,
and shows four views:

  1. F-MAP price trends — by category x region (historical 2012-2018, USD per 100 g).
  2. BLS month-over-month inflation — current/ongoing monthly retail prices.
  3. Nutrition per dollar — most nutrient per dollar (F-MAP price x FDC nutrition).
  4. Forecast vs actuals — next-month BLS price forecast + held-out accuracy (MAPE).

It changes nothing in the warehouse. BigQuery reads are wrapped in ``st.cache_data``
with a TTL so widget interactions filter cached pandas frames instead of re-scanning
BigQuery; the analytics tables are small, so each cached read scans only a few MB.

Run it (from the repo root, in the activated .venv):
    streamlit run dashboard/app.py

Needs GOOGLE_APPLICATION_CREDENTIALS (.env) — the same service-account key the loader,
dbt, and the forecast script use.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

# Make the src/-layout package importable so we reuse the project's .env loader,
# regardless of the cwd `streamlit run` is launched from.
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))
from usda_food_price_pipeline.ingestion import common  # noqa: E402

common.load_environment()

ANALYTICS_DATASET = os.environ.get("BIGQUERY_ANALYTICS_DATASET", "usda_analytics")
FORECAST_DATASET = os.environ.get("BIGQUERY_FORECAST_DATASET", "usda_forecast")
FORECAST_TABLE = "fct_bls_forecast"
CACHE_TTL_SECONDS = 60 * 60  # 1 hour: re-query at most once an hour per table.

st.set_page_config(page_title="USDA Food Price & Nutrition", page_icon="🥕", layout="wide")


# --------------------------------------------------------------------------- #
# BigQuery access — one shared client, small cached table reads.
# --------------------------------------------------------------------------- #
@st.cache_resource(show_spinner=False)
def get_client():
    from google.cloud import bigquery

    project = os.environ.get("BIGQUERY_PROJECT")  # else inferred from the credentials
    return bigquery.Client(project=project) if project else bigquery.Client()


def _query_df(sql: str) -> pd.DataFrame:
    return get_client().query(sql).to_dataframe()


def _to_floats(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """BigQuery NUMERIC comes back as Decimal objects over the REST path; coerce to float."""
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner="Loading F-MAP prices…")
def load_fmap_prices() -> pd.DataFrame:
    project = get_client().project
    df = _query_df(
        f"""
        select efpg_code, efpg_name, region_code, region_name,
               month_date, mean_unit_value, price_index_geks
        from `{project}.{ANALYTICS_DATASET}.fct_fmap_prices`
        """
    )
    df["month_date"] = pd.to_datetime(df["month_date"])
    return _to_floats(df, ["mean_unit_value", "price_index_geks"])


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner="Loading BLS prices…")
def load_bls_prices() -> pd.DataFrame:
    project = get_client().project
    df = _query_df(
        f"""
        select series_id, item_label, unit, month_date, price_usd, is_latest
        from `{project}.{ANALYTICS_DATASET}.fct_bls_prices`
        order by series_id, month_date
        """
    )
    df["month_date"] = pd.to_datetime(df["month_date"])
    df = _to_floats(df, ["price_usd"])
    # Month-over-month and year-over-year % change within each series.
    df["mom_pct"] = df.groupby("series_id")["price_usd"].pct_change() * 100
    df["yoy_pct"] = df.groupby("series_id")["price_usd"].pct_change(periods=12) * 100
    return df


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner="Loading nutrient menu…")
def load_nutrition_menu() -> pd.DataFrame:
    """Distinct (nutrient_number, nutrient_name, unit) for the nutrient dropdown — a tiny query
    over the now-LONG, all-nutrients per-dollar table."""
    project = get_client().project
    return _query_df(
        f"""
        select distinct nutrient_number, nutrient_name, unit
        from `{project}.{ANALYTICS_DATASET}.fct_nutrition_per_dollar`
        order by nutrient_name
        """
    )


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner="Loading nutrition-per-dollar…")
def load_nutrition_per_dollar(nutrient_number: str, unit: str) -> pd.DataFrame:
    """One nutrient's slice (all regions × months) — the per-dollar table is now LONG over ~221
    nutrients, so we read just the selected nutrient. Cached per (nutrient_number, unit)."""
    project = get_client().project
    df = _query_df(
        f"""
        select efpg_code, efpg_name, region_code, region_name, month_date,
               fdc_food_category, nutrient_name, unit, mean_unit_value,
               amount_per_100g, amount_per_dollar, nutrient_rank
        from `{project}.{ANALYTICS_DATASET}.fct_nutrition_per_dollar`
        where nutrient_number = '{nutrient_number}' and unit = '{unit}'
        """
    )
    df["month_date"] = pd.to_datetime(df["month_date"])
    return _to_floats(df, ["mean_unit_value", "amount_per_100g", "amount_per_dollar"])


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner="Loading forecasts…")
def load_forecast() -> pd.DataFrame:
    """Forecast table is written by the Python script, not dbt. Returns an empty
    frame (rather than raising) if it hasn't been generated yet."""
    project = get_client().project
    try:
        df = _query_df(
            f"""
            select series_id, item_label, unit, forecast_month, forecast_price_usd,
                   last_actual_month, last_actual_price_usd, pct_change_vs_last,
                   model, mape_backtest, naive_mape_backtest, n_backtest_points,
                   n_train_months, generated_at
            from `{project}.{FORECAST_DATASET}.{FORECAST_TABLE}`
            """
        )
    except Exception:  # noqa: BLE001 — most likely the table/dataset doesn't exist yet.
        return pd.DataFrame()
    for col in ("forecast_month", "last_actual_month"):
        df[col] = pd.to_datetime(df[col])
    return _to_floats(
        df,
        ["forecast_price_usd", "last_actual_price_usd", "pct_change_vs_last",
         "mape_backtest", "naive_mape_backtest"],
    )


def _date_range_caption(df: pd.DataFrame, col: str = "month_date") -> str:
    lo, hi = df[col].min(), df[col].max()
    return f"{lo:%b %Y} – {hi:%b %Y}"


def _default_region(regions: list[str]) -> int:
    """Index of a sensible default region (prefer a national/U.S. one)."""
    for i, name in enumerate(regions):
        if "nation" in name.lower() or name.strip().lower() in {"us", "u.s.", "united states"}:
            return i
    return 0


# --------------------------------------------------------------------------- #
# Load everything once (cached), then build the UI.
# --------------------------------------------------------------------------- #
st.title("🥕 USDA Food Price & Nutrition Dashboard")
st.caption(
    "Read-only views over BigQuery analytics tables built by dbt and refreshed daily by "
    "the Airflow pipeline. **Sources:** USDA ERS F-MAP (historical prices, file download), "
    "BLS Average Price Data (current prices, API), USDA FoodData Central (nutrition, API)."
)

try:
    fmap = load_fmap_prices()
    bls = load_bls_prices()
    npd_menu = load_nutrition_menu()  # per-nutrient slices are loaded lazily inside the tab
    forecast = load_forecast()
except Exception as exc:  # noqa: BLE001
    st.error(
        "Could not read from BigQuery. Check that GOOGLE_APPLICATION_CREDENTIALS is set in "
        f".env and the analytics tables exist.\n\n{type(exc).__name__}: {exc}"
    )
    st.stop()

# Sidebar filters (drive the F-MAP and nutrition-per-dollar views).
st.sidebar.header("Filters")
regions = sorted(fmap["region_name"].dropna().unique().tolist())
categories = sorted(fmap["efpg_name"].dropna().unique().tolist())

region = st.sidebar.selectbox("Region (F-MAP / nutrition views)", regions, index=_default_region(regions))
sel_categories = st.sidebar.multiselect(
    "Categories (F-MAP trend)", categories, default=categories[: min(5, len(categories))]
)
st.sidebar.caption(
    "Region applies to the F-MAP price-trend and nutrition-per-dollar views. The BLS view "
    "is U.S. city-average only (no regional breakdown) and has its own item picker."
)
st.sidebar.divider()
st.sidebar.caption("🔄 BigQuery reads are cached for 1 hour, so filtering doesn't re-scan.")

tab_fmap, tab_bls, tab_npd, tab_fcst = st.tabs(
    ["📈 F-MAP price trends", "📊 BLS inflation", "🥗 Nutrition per dollar", "🔮 Forecast"]
)

# --------------------------------------------------------------------------- #
# Tab 1 — F-MAP price trends (category x region).
# --------------------------------------------------------------------------- #
with tab_fmap:
    st.subheader("Food-at-home price trends")
    st.caption(
        f"**Source:** USDA ERS Food-at-Home Monthly Area Prices (F-MAP) · **historical, "
        f"{_date_range_caption(fmap)}** · price = weighted mean unit value (USD per 100 g)."
    )

    st.markdown(f"**Categories within _{region}_**")
    if not sel_categories:
        st.info("Pick one or more categories in the sidebar to see their price trends.")
    else:
        sub = fmap[(fmap["region_name"] == region) & (fmap["efpg_name"].isin(sel_categories))]
        if sub.empty:
            st.warning("No F-MAP rows for that region/category combination.")
        else:
            chart = (
                alt.Chart(sub)
                .mark_line()
                .encode(
                    x=alt.X("month_date:T", title="Month"),
                    y=alt.Y("mean_unit_value:Q", title="USD per 100 g"),
                    color=alt.Color("efpg_name:N", title="Category"),
                    tooltip=[
                        alt.Tooltip("efpg_name:N", title="Category"),
                        alt.Tooltip("month_date:T", title="Month"),
                        alt.Tooltip("mean_unit_value:Q", title="USD/100g", format=".3f"),
                    ],
                )
                .properties(height=380)
            )
            st.altair_chart(chart, width="stretch")

    st.divider()
    st.markdown("**One category across regions**")
    cat_default = sel_categories[0] if sel_categories else categories[0]
    cat_one = st.selectbox(
        "Category", categories, index=categories.index(cat_default), key="fmap_cat_regions"
    )
    cat_df = fmap[fmap["efpg_name"] == cat_one]
    region_opts = sorted(cat_df["region_name"].dropna().unique().tolist())
    region_default = region_opts[: min(5, len(region_opts))]
    if region in region_opts and region not in region_default:
        region_default = [region] + region_default[:-1]
    sel_regions = st.multiselect(
        "Regions to compare", region_opts, default=region_default, key="fmap_regions"
    )
    reg_sub = cat_df[cat_df["region_name"].isin(sel_regions)]
    if reg_sub.empty:
        st.info("Pick one or more regions to compare for this category.")
    else:
        chart2 = (
            alt.Chart(reg_sub)
            .mark_line()
            .encode(
                x=alt.X("month_date:T", title="Month"),
                y=alt.Y("mean_unit_value:Q", title="USD per 100 g"),
                color=alt.Color("region_name:N", title="Region"),
                tooltip=[
                    alt.Tooltip("region_name:N", title="Region"),
                    alt.Tooltip("month_date:T", title="Month"),
                    alt.Tooltip("mean_unit_value:Q", title="USD/100g", format=".3f"),
                ],
            )
            .properties(height=380)
        )
        st.altair_chart(chart2, width="stretch")

# --------------------------------------------------------------------------- #
# Tab 2 — BLS month-over-month inflation (current).
# --------------------------------------------------------------------------- #
with tab_bls:
    st.subheader("Retail price inflation (month-over-month)")
    st.caption(
        f"**Source:** BLS Average Price Data (APU) · **current/ongoing, "
        f"{_date_range_caption(bls)}** · U.S. city average (no regional breakdown)."
    )

    items = sorted(bls["item_label"].dropna().unique().tolist())
    sel_items = st.multiselect("Food items", items, default=items, key="bls_items")
    bsub = bls[bls["item_label"].isin(sel_items)] if sel_items else bls.iloc[0:0]

    if bsub.empty:
        st.info("Pick one or more food items to see their prices and inflation.")
    else:
        # Latest month-over-month change per item, as metric tiles.
        latest_month = bsub["month_date"].max()
        latest = bsub[bsub["month_date"] == latest_month].sort_values("item_label")
        st.markdown(f"**Latest month-over-month change** — {latest_month:%b %Y}")
        cols = st.columns(min(4, len(latest)) or 1)
        for i, (_, row) in enumerate(latest.iterrows()):
            mom = row["mom_pct"]
            cols[i % len(cols)].metric(
                label=f"{row['item_label']} ({row['unit']})",
                value=f"${row['price_usd']:.2f}",
                delta=(f"{mom:+.1f}% MoM" if pd.notna(mom) else "n/a"),
                delta_color="inverse",  # rising prices = bad, show red
            )

        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Price over time** (USD)")
            price_chart = (
                alt.Chart(bsub)
                .mark_line()
                .encode(
                    x=alt.X("month_date:T", title="Month"),
                    y=alt.Y("price_usd:Q", title="Price (USD)"),
                    color=alt.Color("item_label:N", title="Item"),
                    tooltip=["item_label", alt.Tooltip("month_date:T"), alt.Tooltip("price_usd:Q", format=".2f")],
                )
                .properties(height=340)
            )
            st.altair_chart(price_chart, width="stretch")
        with c2:
            st.markdown("**Month-over-month inflation** (%)")
            mom_chart = (
                alt.Chart(bsub.dropna(subset=["mom_pct"]))
                .mark_line()
                .encode(
                    x=alt.X("month_date:T", title="Month"),
                    y=alt.Y("mom_pct:Q", title="MoM change (%)"),
                    color=alt.Color("item_label:N", title="Item"),
                    tooltip=["item_label", alt.Tooltip("month_date:T"), alt.Tooltip("mom_pct:Q", format="+.2f")],
                )
                .properties(height=340)
            )
            zero = alt.Chart(pd.DataFrame({"y": [0]})).mark_rule(strokeDash=[4, 4], color="gray").encode(y="y:Q")
            st.altair_chart(mom_chart + zero, width="stretch")

        st.markdown("**Latest snapshot**")
        table = latest[["item_label", "unit", "price_usd", "mom_pct", "yoy_pct"]].rename(
            columns={
                "item_label": "Item", "unit": "Unit", "price_usd": "Price (USD)",
                "mom_pct": "MoM %", "yoy_pct": "YoY %",
            }
        )
        st.dataframe(
            table.style.format({"Price (USD)": "${:.2f}", "MoM %": "{:+.2f}", "YoY %": "{:+.2f}"}),
            width="stretch", hide_index=True,
        )

# --------------------------------------------------------------------------- #
# Tab 3 — Nutrition per dollar (most nutrient per dollar).
# --------------------------------------------------------------------------- #
with tab_npd:
    st.subheader("Most nutrition per dollar")
    st.caption(
        "**Source:** F-MAP price (**historical**) joined to FoodData Central nutrition (static) "
        "via a category crosswalk. Pick any of the ~221 reported nutrients; bars show the amount "
        "**per dollar** in that nutrient's own unit. Note: the crosswalk is intentionally lossy, "
        "so several priced categories share one broad nutrition profile."
    )

    if npd_menu.empty:
        st.warning("No nutrition-per-dollar data found. Has `dbt build` been run?")
    else:
        # Nutrient dropdown over every (nutrient_name, unit) in the LONG table; default to protein.
        menu = npd_menu.copy()
        menu["label"] = menu["nutrient_name"].fillna(menu["nutrient_number"]) + " (" + menu["unit"] + ") / $"
        menu = menu.sort_values("label").reset_index(drop=True)
        default_idx = menu.index[
            (menu["nutrient_number"] == "203") & (menu["unit"] == "G")
        ].tolist()
        nutrient_label = st.selectbox(
            "Nutrient",
            menu["label"].tolist(),
            index=(default_idx[0] if default_idx else 0),
            key="npd_nutrient",
        )
        sel = menu[menu["label"] == nutrient_label].iloc[0]
        unit = sel["unit"]

        npd = load_nutrition_per_dollar(sel["nutrient_number"], unit)
        reg_npd = npd[npd["region_name"] == region]
        if reg_npd.empty:
            st.warning(f"No nutrition-per-dollar rows for region '{region}'.")
        else:
            months = sorted(reg_npd["month_date"].dropna().unique())
            month_labels = [pd.Timestamp(m).strftime("%b %Y") for m in months]
            pick = st.select_slider(
                "Month", options=month_labels, value=month_labels[-1], key="npd_month"
            )
            chosen_month = months[month_labels.index(pick)]
            top_n = st.slider("Show top N categories", 5, 20, 10, key="npd_topn")

            snap = (
                reg_npd[reg_npd["month_date"] == chosen_month]
                .dropna(subset=["amount_per_dollar"])
                .sort_values("amount_per_dollar", ascending=False)
                .head(top_n)
            )
            axis_title = f"{nutrient_label}"
            st.markdown(f"**{nutrient_label}** — {region}, {pick}")
            if snap.empty:
                st.info("No data for that month/region.")
            else:
                bar = (
                    alt.Chart(snap)
                    .mark_bar()
                    .encode(
                        x=alt.X("amount_per_dollar:Q", title=axis_title),
                        y=alt.Y("efpg_name:N", title="Category", sort="-x"),
                        color=alt.Color(
                            "amount_per_dollar:Q", legend=None, scale=alt.Scale(scheme="greens")
                        ),
                        tooltip=[
                            alt.Tooltip("efpg_name:N", title="Category"),
                            alt.Tooltip("fdc_food_category:N", title="Nutrition profile"),
                            alt.Tooltip("mean_unit_value:Q", title="Price USD/100g", format=".3f"),
                            alt.Tooltip("amount_per_100g:Q", title=f"Amount/100g ({unit})", format=".2f"),
                            alt.Tooltip("amount_per_dollar:Q", title=axis_title, format=".2f"),
                        ],
                    )
                    .properties(height=max(300, 28 * len(snap)))
                )
                st.altair_chart(bar, width="stretch")

# --------------------------------------------------------------------------- #
# Tab 4 — Forecast vs actuals (next-month BLS price).
# --------------------------------------------------------------------------- #
with tab_fcst:
    st.subheader("Next-month price forecast (BLS)")
    st.caption(
        "**Source:** forecast written by `usda_food_price_pipeline.forecast.bls_forecast` to "
        f"`{FORECAST_DATASET}.{FORECAST_TABLE}`. Model: per-series linear trend + month "
        "seasonality (scikit-learn Ridge). **Accuracy** = MAPE of an expanding one-step-ahead "
        "backtest over the most recent held-out months; a last-value naive baseline is shown "
        "for context. Small data (~48 monthly points/series) → expect noisy accuracy."
    )

    if forecast.empty:
        st.info(
            "No forecast table found yet. Generate it with:\n\n"
            "```\npython -m usda_food_price_pipeline.forecast.bls_forecast\n```"
        )
    else:
        valid_mape = forecast["mape_backtest"].dropna()
        valid_naive = forecast["naive_mape_backtest"].dropna()
        m1, m2, m3 = st.columns(3)
        m1.metric("Items forecast", f"{len(forecast)}")
        m2.metric(
            "Mean MAPE (held-out)",
            f"{valid_mape.mean():.1f}%" if not valid_mape.empty else "n/a",
        )
        m3.metric(
            "Naive baseline MAPE",
            f"{valid_naive.mean():.1f}%" if not valid_naive.empty else "n/a",
        )

        gen = pd.to_datetime(forecast["generated_at"]).max()
        st.caption(f"Forecast generated {gen:%Y-%m-%d %H:%M UTC}. Lower MAPE is better.")

        ftable = forecast.sort_values("mape_backtest")[
            ["item_label", "unit", "forecast_month", "forecast_price_usd",
             "last_actual_price_usd", "pct_change_vs_last", "mape_backtest", "naive_mape_backtest"]
        ].rename(
            columns={
                "item_label": "Item", "unit": "Unit", "forecast_month": "Forecast month",
                "forecast_price_usd": "Forecast (USD)", "last_actual_price_usd": "Last actual (USD)",
                "pct_change_vs_last": "Δ vs last %", "mape_backtest": "MAPE %",
                "naive_mape_backtest": "Naive MAPE %",
            }
        )
        st.dataframe(
            ftable.style.format(
                {
                    "Forecast month": "{:%b %Y}", "Forecast (USD)": "${:.2f}",
                    "Last actual (USD)": "${:.2f}", "Δ vs last %": "{:+.1f}",
                    "MAPE %": "{:.1f}", "Naive MAPE %": "{:.1f}",
                }
            ),
            width="stretch", hide_index=True,
        )

        st.divider()
        fc_items = forecast.sort_values("item_label")["item_label"].tolist()
        pick_item = st.selectbox("Show actuals + forecast for", fc_items, key="fcst_item")
        frow = forecast[forecast["item_label"] == pick_item].iloc[0]

        hist = bls[bls["series_id"] == frow["series_id"]][["month_date", "price_usd"]]
        fc_point = pd.DataFrame(
            {"month_date": [frow["forecast_month"]], "price_usd": [frow["forecast_price_usd"]]}
        )
        connector = pd.DataFrame(
            {
                "month_date": [frow["last_actual_month"], frow["forecast_month"]],
                "price_usd": [frow["last_actual_price_usd"], frow["forecast_price_usd"]],
            }
        )

        actual_line = alt.Chart(hist).mark_line(color="#4c78a8").encode(
            x=alt.X("month_date:T", title="Month"),
            y=alt.Y("price_usd:Q", title="Price (USD)"),
            tooltip=[alt.Tooltip("month_date:T"), alt.Tooltip("price_usd:Q", format=".2f")],
        )
        fc_line = alt.Chart(connector).mark_line(strokeDash=[5, 4], color="#e45756")
        fc_line = fc_line.encode(x="month_date:T", y="price_usd:Q")
        fc_dot = alt.Chart(fc_point).mark_point(size=120, color="#e45756", filled=True).encode(
            x="month_date:T", y="price_usd:Q",
            tooltip=[alt.Tooltip("month_date:T", title="Forecast month"),
                     alt.Tooltip("price_usd:Q", title="Forecast", format=".2f")],
        )
        st.altair_chart((actual_line + fc_line + fc_dot).properties(height=360), width="stretch")

        d1, d2, d3 = st.columns(3)
        d1.metric(f"Forecast — {frow['forecast_month']:%b %Y}", f"${frow['forecast_price_usd']:.2f}",
                  delta=f"{frow['pct_change_vs_last']:+.1f}% vs last" if pd.notna(frow["pct_change_vs_last"]) else None,
                  delta_color="inverse")
        d2.metric("This item's MAPE", f"{frow['mape_backtest']:.1f}%" if pd.notna(frow["mape_backtest"]) else "n/a")
        d3.metric("Naive baseline", f"{frow['naive_mape_backtest']:.1f}%" if pd.notna(frow["naive_mape_backtest"]) else "n/a")
