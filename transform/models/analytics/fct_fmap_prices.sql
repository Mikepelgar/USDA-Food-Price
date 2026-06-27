-- Clean monthly F-MAP food price by category x region (2012-2018).
-- Grain: (efpg_code, region_code, month_date). Price = ERS weighted mean unit value (USD/100 g)
-- with the GEKS index (both from the main workbook, all years). The supplemental workbook's
-- alternative index methods are LEFT JOINed on (2016-2018 only; NULL for 2012-2015).

with main as (

    select * from {{ ref('stg_prices_fmap') }}

),

idx as (

    select * from {{ ref('stg_fmap_price_index') }}

)

select
    main.efpg_code,
    main.efpg_name,
    main.region_code,
    main.region_name,
    main.year,
    main.month_num,
    main.month_date,
    main.mean_unit_value,
    main.unit_value_check,
    main.price_index_geks,
    main.number_stores,
    -- supplementary alternative index methodologies (2016-2018 only)
    idx.price_index_laspeyres,
    idx.price_index_paasche,
    idx.price_index_tornqvist,
    idx.price_index_fisher_ideal,
    idx.price_index_ccd
from main
left join idx
    on  main.efpg_code   = idx.efpg_code
    and main.region_code = idx.region_code
    and main.month_date  = idx.month_date
