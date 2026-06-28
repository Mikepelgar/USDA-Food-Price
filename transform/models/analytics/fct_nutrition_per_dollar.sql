{{ config(materialized='table', cluster_by=['nutrient_number']) }}

-- Combined model: F-MAP price joined (via category_crosswalk) to the LONG dim_nutrition, giving
-- every nutrient per dollar by category x region x month, ranked per nutrient within each
-- region/month. Joining the LONG nutrition dim fans each price row out to one row per nutrient.
--
-- Units: both sides are per 100 g, so the 100 g cancels —
--   amount_per_dollar = amount_per_100g / mean_unit_value   (mean_unit_value is USD per 100 g)
--                     = amount of nutrient (in its own unit: g / mg / ug / kcal) per dollar.
--
-- CAVEAT: uses HISTORICAL F-MAP prices (2012-2018) with static nutrition — NOT current prices.
-- fct_bls_prices is the current/forecastable feed. Crosswalk is intentionally lossy and several
-- priced categories share one broad FDC nutrition profile (see category_crosswalk).
-- Grain: (efpg_code, region_code, month_date, nutrient_number, unit).

with prices as (

    select * from {{ ref('fct_fmap_prices') }}

),

crosswalk as (

    select * from {{ ref('category_crosswalk') }}

),

nutrition as (

    select * from {{ ref('dim_nutrition') }}

),

joined as (

    select
        prices.efpg_code,
        prices.efpg_name,
        prices.region_code,
        prices.region_name,
        prices.year,
        prices.month_num,
        prices.month_date,
        prices.mean_unit_value,
        crosswalk.fdc_food_category,
        nutrition.nutrient_number,
        nutrition.nutrient_name,
        nutrition.unit,
        nutrition.amount_per_100g,
        safe_divide(nutrition.amount_per_100g, prices.mean_unit_value) as amount_per_dollar
    from prices
    inner join crosswalk
        on prices.efpg_code = crosswalk.fmap_efpg_code
    inner join nutrition
        on crosswalk.fdc_food_category = nutrition.food_category

)

select
    *,
    rank() over (
        partition by region_code, month_date, nutrient_number, unit
        order by amount_per_dollar desc
    ) as nutrient_rank
from joined
