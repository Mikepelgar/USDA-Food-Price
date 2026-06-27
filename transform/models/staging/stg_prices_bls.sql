-- Cleaned BLS APU monthly retail prices: one row per (series_id, month_date).
-- Monthly observations only (M01-M12; M13 annual average dropped). Item label comes from the
-- bls_series_items seed. Values are SAFE_CAST so stray blanks/footnote markers never fail the run.

with src as (

    select
        series_id,
        year,
        period,
        value,
        latest,
        source_file,
        loaded_at
    from {{ source('usda_raw', 'raw_prices_bls') }}
    where regexp_contains(period, r'^M(0[1-9]|1[0-2])$')   -- monthly periods only

),

cleaned as (

    select
        series_id,
        safe_cast(year as int64)                                              as year,
        cast(substr(period, 2) as int64)                                      as month_num,
        date(safe_cast(year as int64), cast(substr(period, 2) as int64), 1)   as month_date,
        safe_cast(value as numeric)                                           as price_usd,
        coalesce(safe_cast(latest as bool), false)                            as is_latest,
        source_file,
        loaded_at
    from src

),

deduped as (

    select
        *,
        row_number() over (
            partition by series_id, year, month_num
            order by loaded_at desc, source_file desc
        ) as rn
    from cleaned
    where price_usd is not null

)

select
    d.series_id,
    i.item_label,
    i.unit,
    d.year,
    d.month_num,
    d.month_date,
    d.price_usd,
    d.is_latest
from deduped d
left join {{ ref('bls_series_items') }} i
    on d.series_id = i.series_id
where d.rn = 1
