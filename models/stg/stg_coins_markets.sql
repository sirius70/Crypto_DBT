{{ config(
    materialized='incremental',
    unique_key=['coin_id', 'fetched_at']   -- ensures no duplicates / safe upsert
) }}

with raw as (
  select 
    data as v,
    meta:fetched_at::timestamp_ntz as fetched_at
  from {{ source('raw', 'raw_coins_markets') }}
)

select
    v:id::string                          as coin_id,
    v:symbol::string                      as symbol,
    v:name::string                        as name,
    v:image::string                       as image_url,
    v:current_price::float                as current_price,
    v:market_cap::float                   as market_cap,
    v:market_cap_rank::int                as market_cap_rank,
    v:fully_diluted_valuation::float      as fully_diluted_valuation,
    v:total_volume::float                 as total_volume,
    v:high_24h::float                     as high_24h,
    v:low_24h::float                      as low_24h,
    v:price_change_24h::float             as price_change_24h,
    v:price_change_percentage_24h::float  as price_change_pct_24h,
    v:market_cap_change_24h::float        as market_cap_change_24h,
    v:market_cap_change_percentage_24h::float as market_cap_change_pct_24h,
    v:circulating_supply::float           as circulating_supply,
    v:total_supply::float                 as total_supply,
    v:max_supply::float                   as max_supply,
    v:ath::float                          as all_time_high,
    v:ath_change_percentage::float        as ath_change_pct,
    v:ath_date::timestamp_ntz             as ath_date,
    v:atl::float                          as all_time_low,
    v:atl_change_percentage::float        as atl_change_pct,
    v:atl_date::timestamp_ntz             as atl_date,
    v:roi.currency::string                as roi_currency,
    v:roi.percentage::float               as roi_pct,
    v:roi.times::float                    as roi_times,
    v:last_updated::timestamp_ntz         as last_updated,
    fetched_at,
    current_timestamp() as ingested_at
from raw
