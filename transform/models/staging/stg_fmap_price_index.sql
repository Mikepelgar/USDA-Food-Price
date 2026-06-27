-- Cleaned ERS F-MAP SUPPLEMENTAL workbook (2016-2018): alternative price-index methodologies
-- only (no dollars/grams price). Kept SEPARATE from stg_prices_fmap because the two workbooks
-- have different columns and overlapping years — unioning them would clobber the price. These
-- extra indexes are LEFT JOINed onto fct_fmap_prices. Grain: (efpg_code, region_code, month).

with src as (

    select
        raw_json,
        source_file,
        loaded_at
    from {{ source('usda_raw', 'raw_prices_fmap') }}
    where sheet_name = 'Data'
      and source_file like '%supplemental%'   -- supplemental workbook only

),

parsed as (

    select
        safe_cast(json_value(raw_json, '$.EFPG_code') as int64)                  as efpg_code,
        safe_cast(json_value(raw_json, '$.Metroregion_code') as int64)           as region_code,
        safe_cast(json_value(raw_json, '$.Year') as int64)                       as year,
        safe_cast(json_value(raw_json, '$.Month') as int64)                      as month_num,
        safe_cast(json_value(raw_json, '$.Price_index_Laspeyres') as numeric)    as price_index_laspeyres,
        safe_cast(json_value(raw_json, '$.Price_index_Paasche') as numeric)      as price_index_paasche,
        safe_cast(json_value(raw_json, '$.Price_index_Tornqvist') as numeric)    as price_index_tornqvist,
        safe_cast(json_value(raw_json, '$.Price_index_Fisher_Ideal') as numeric) as price_index_fisher_ideal,
        safe_cast(json_value(raw_json, '$.Price_index_GEKS') as numeric)         as price_index_geks_supp,
        safe_cast(json_value(raw_json, '$.Price_index_CCD') as numeric)          as price_index_ccd,
        source_file,
        loaded_at
    from src

),

deduped as (

    select
        *,
        date(year, month_num, 1) as month_date,
        row_number() over (
            partition by efpg_code, region_code, year, month_num
            order by loaded_at desc, source_file desc
        ) as rn
    from parsed
    where year is not null
      and month_num is not null

)

select
    efpg_code,
    region_code,
    year,
    month_num,
    month_date,
    price_index_laspeyres,
    price_index_paasche,
    price_index_tornqvist,
    price_index_fisher_ideal,
    price_index_geks_supp,
    price_index_ccd
from deduped
where rn = 1
