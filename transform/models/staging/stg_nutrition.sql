-- Cleaned FDC foods: one row per fdc_id. Standardizes the food_category key, keeps the
-- foodNutrients array as JSON for the per-category pivot in dim_nutrition. Source nutrient
-- values are per 100 g of edible portion.

with src as (

    select
        fdc_id,
        source_file,
        loaded_at,
        raw_json
    from {{ source('usda_raw', 'raw_nutrition') }}

),

parsed as (

    select
        fdc_id,
        nullif(trim(json_value(raw_json, '$.description')), '')              as description,
        nullif(trim(json_value(raw_json, '$.dataType')), '')                as data_type,
        -- standardize: lower + trim + collapse internal whitespace
        nullif(
            regexp_replace(lower(trim(json_value(raw_json, '$.foodCategory'))), r'\s+', ' '),
            ''
        )                                                                    as food_category_raw,
        json_query(raw_json, '$.foodNutrients')                             as food_nutrients,
        source_file,
        loaded_at
    from src

),

cleaned as (

    select
        fdc_id,
        description,
        data_type,
        -- NULL out the "no category" sentinels so they don't form a junk key
        case
            when food_category_raw in ('not included in a food category', 'not in a food category')
                then null
            else food_category_raw
        end                                                                  as food_category,
        food_nutrients,
        source_file,
        loaded_at,
        -- deterministic dedup: same fdc_id can recur across overlapping query pages / reloads
        row_number() over (
            partition by fdc_id
            order by loaded_at desc, source_file desc
        )                                                                    as rn
    from parsed

)

select
    fdc_id,
    description,
    data_type,
    food_category,
    food_nutrients
from cleaned
where rn = 1
