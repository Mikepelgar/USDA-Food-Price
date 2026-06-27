-- Combined model: F-MAP price joined (via category_crosswalk) to dim_nutrition, giving nutrients
-- per dollar by category x region x month, ranked by protein per dollar within each region/month.
--
-- Units: both sides are per 100 g, so the 100 g cancels —
--   nutrient_per_dollar = nutrient_g_per_100g / mean_unit_value   (mean_unit_value is USD per 100 g)
--                       = grams of nutrient per dollar.
--
-- CAVEAT: uses HISTORICAL F-MAP prices (2012-2018) with static nutrition — NOT current prices.
-- fct_bls_prices is the current/forecastable feed. Crosswalk is intentionally lossy and several
-- priced categories share one broad FDC nutrition profile (see category_crosswalk).
-- Grain: (efpg_code, region_code, month_date).

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
        nutrition.protein_g_per_100g,
        nutrition.energy_kcal_per_100g,
        nutrition.fiber_g_per_100g,
        safe_divide(nutrition.protein_g_per_100g,   prices.mean_unit_value) as protein_g_per_dollar,
        safe_divide(nutrition.energy_kcal_per_100g, prices.mean_unit_value) as energy_kcal_per_dollar,
        safe_divide(nutrition.fiber_g_per_100g,     prices.mean_unit_value) as fiber_g_per_dollar
    from prices
    inner join crosswalk
        on prices.efpg_code = crosswalk.fmap_efpg_code
    inner join nutrition
        on crosswalk.fdc_food_category = nutrition.food_category

)

select
    *,
    rank() over (
        partition by region_code, month_date
        order by protein_g_per_dollar desc
    ) as protein_rank
from joined
