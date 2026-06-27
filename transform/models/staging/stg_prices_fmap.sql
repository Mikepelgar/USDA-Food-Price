-- Cleaned ERS F-MAP MAIN workbook (2012-2018): one row per (efpg_code, region_code, month).
-- Exposes the official weighted mean unit value (Unit_value_mean_wtd, USD per 100 g) AND the
-- GEKS price index — both present in the main workbook for ALL years. The supplemental
-- index-only workbook is handled separately in stg_fmap_price_index.

with src as (

    select
        raw_json,
        source_file,
        loaded_at
    from {{ source('usda_raw', 'raw_prices_fmap') }}
    where sheet_name = 'Data'
      and source_file not like '%supplemental%'   -- main workbook only

),

parsed as (

    select
        safe_cast(json_value(raw_json, '$.EFPG_code') as int64)              as efpg_code,
        regexp_replace(lower(trim(json_value(raw_json, '$.EFPG_name'))), r'\s+', ' ')          as efpg_name,
        safe_cast(json_value(raw_json, '$.Metroregion_code') as int64)       as region_code,
        regexp_replace(lower(trim(json_value(raw_json, '$.Metroregion_name'))), r'\s+', ' ')   as region_name,
        safe_cast(json_value(raw_json, '$.Year') as int64)                   as year,
        safe_cast(json_value(raw_json, '$.Month') as int64)                  as month_num,
        safe_cast(json_value(raw_json, '$.Unit_value_mean_wtd') as numeric)  as mean_unit_value,
        safe_cast(json_value(raw_json, '$.Purchase_dollars_wtd') as numeric) as purchase_dollars_wtd,
        safe_cast(json_value(raw_json, '$.Purchase_grams_wtd') as numeric)   as purchase_grams_wtd,
        safe_cast(json_value(raw_json, '$.Price_index_GEKS') as numeric)     as price_index_geks,
        safe_cast(json_value(raw_json, '$.Number_stores') as int64)          as number_stores,
        source_file,
        loaded_at
    from src

),

cleaned as (

    select
        efpg_code,
        efpg_name,
        region_code,
        region_name,
        year,
        month_num,
        date(year, month_num, 1)                                             as month_date,
        mean_unit_value,
        -- documented cross-check: 100 * dollars/grams should ≈ mean_unit_value (USD/100 g)
        round(100 * safe_divide(purchase_dollars_wtd, purchase_grams_wtd), 4) as unit_value_check,
        price_index_geks,
        number_stores,
        source_file,
        loaded_at
    from parsed
    where mean_unit_value is not null
      and year is not null
      and month_num is not null

),

deduped as (

    select
        *,
        row_number() over (
            partition by efpg_code, region_code, year, month_num
            order by loaded_at desc, source_file desc
        ) as rn
    from cleaned

)

select
    efpg_code,
    efpg_name,
    region_code,
    region_name,
    year,
    month_num,
    month_date,
    mean_unit_value,
    unit_value_check,
    price_index_geks,
    number_stores
from deduped
where rn = 1
