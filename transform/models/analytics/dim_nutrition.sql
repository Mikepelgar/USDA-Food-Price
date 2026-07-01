-- Clean nutrition profile per FDC food_category, in LONG form: one row per
-- (food_category, nutrient_number, unit). Explodes foodNutrients, collapses each food to one value
-- per nutrient, then takes the MEDIAN across the foods in each category (median resists outliers /
-- mislabeled foods). Branded + Experimental foods are excluded (noisy grocery-aisle categories).
--
-- "unit" is part of the key on purpose: energy shows up as two nutrients (KCAL and kJ), which is why
-- the old WIDE model had to hard-code `and unit_name = 'KCAL'`. Units (G / MG / UG / KCAL / …) flow
-- through verbatim so downstream "per dollar" can be labelled correctly.
-- Grain: (food_category, nutrient_number, unit).

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
        json_value(n, '$.nutrientName')                 as nutrient_name,
        json_value(n, '$.unitName')                     as unit_name,
        safe_cast(json_value(n, '$.value') as numeric)  as value
    from foods f,
    unnest(json_query_array(f.food_nutrients)) as n

),

per_food_nutrient as (

    -- one value per food per nutrient; a food can list the same nutrient under several
    -- derivations, so collapse to its max reported value.
    select
        fdc_id,
        food_category,
        nutrient_number,
        unit_name,
        any_value(nutrient_name) as nutrient_name,
        max(value)               as value
    from nutrients
    where nutrient_number is not null
      and value is not null
    group by fdc_id, food_category, nutrient_number, unit_name

)

select
    food_category,
    nutrient_number,
    any_value(nutrient_name)                        as nutrient_name,
    unit_name                                       as unit,
    count(*)                                        as n_foods,
    -- APPROX_QUANTILES(x, 2) -> [min, median, max]; SAFE_OFFSET(1) is the median (NULL if empty)
    approx_quantiles(value, 2)[safe_offset(1)]      as amount_per_100g
from per_food_nutrient
group by food_category, nutrient_number, unit_name
