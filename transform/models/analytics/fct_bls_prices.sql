-- Clean current monthly BLS retail price series by item.
-- This is the live, forecastable price feed that the Phase 5 model will consume.
-- Grain: (series_id, month_date).

select
    series_id,
    item_label,
    unit,
    year,
    month_num,
    month_date,
    price_usd,
    is_latest
from {{ ref('stg_prices_bls') }}
