-- Clean nutrition profile per FDC food_category (per 100 g).
-- Explodes foodNutrients, pivots the target nutrients per food, then takes the MEDIAN across the
-- foods in each category (median resists outliers / mislabeled foods). Branded + Experimental
-- foods are excluded (noisy grocery-aisle categories); energy is restricted to the kcal row.
-- Grain: (food_category).

with foods as (

    select
        fdc_id,
        food_category,
        food_nutrients
    from {{ ref('stg_nutrition') }}
    where food_category is not null
      and (data_type in ('Foundation', 'SR Legacy') or data_type like 'Survey%')

),

nutrients as (

    select
        f.fdc_id,
        f.food_category,
        json_value(n, '$.nutrientNumber')               as nutrient_number,
        json_value(n, '$.unitName')                     as unit_name,
        safe_cast(json_value(n, '$.value') as numeric)  as value
    from foods f,
    unnest(json_query_array(f.food_nutrients)) as n

),

per_food as (

    select
        fdc_id,
        food_category,
        max(case when nutrient_number = '203' then value end)                        as protein_g,
        max(case when nutrient_number = '208' and unit_name = 'KCAL' then value end) as energy_kcal,
        max(case when nutrient_number = '204' then value end)                        as total_fat_g,
        max(case when nutrient_number = '205' then value end)                        as carbs_g,
        max(case when nutrient_number = '291' then value end)                        as fiber_g,
        max(case when nutrient_number = '301' then value end)                        as calcium_mg,
        max(case when nutrient_number = '303' then value end)                        as iron_mg,
        max(case when nutrient_number = '307' then value end)                        as sodium_mg
    from nutrients
    group by fdc_id, food_category

)

select
    food_category,
    count(*)                                              as n_foods,
    -- APPROX_QUANTILES(x, 2) -> [min, median, max]; SAFE_OFFSET(1) is the median (NULL if empty)
    approx_quantiles(protein_g, 2)[safe_offset(1)]        as protein_g_per_100g,
    approx_quantiles(energy_kcal, 2)[safe_offset(1)]      as energy_kcal_per_100g,
    approx_quantiles(total_fat_g, 2)[safe_offset(1)]      as total_fat_g_per_100g,
    approx_quantiles(carbs_g, 2)[safe_offset(1)]          as carbs_g_per_100g,
    approx_quantiles(fiber_g, 2)[safe_offset(1)]          as fiber_g_per_100g,
    approx_quantiles(calcium_mg, 2)[safe_offset(1)]       as calcium_mg_per_100g,
    approx_quantiles(iron_mg, 2)[safe_offset(1)]          as iron_mg_per_100g,
    approx_quantiles(sodium_mg, 2)[safe_offset(1)]        as sodium_mg_per_100g
from per_food
group by food_category
