{{ config(materialized='incremental') }}

with latest as (
    select coalesce(max(fetched_at), '1970-01-01'::timestamp_ntz) as max_fetched
    from {{ this }}
)

, nfts as (
    select
        nft_id,
        name,
        symbol,
        native_currency,
        floor_price_native,
        floor_price_pct_24h,
        regexp_replace(floor_price_str, '[^0-9.]', '')::float as floor_price_usd,
        regexp_replace(h24_volume, '[^0-9.]', '')::float as h24_volume_usd,
        regexp_replace(h24_avg_sale_price, '[^0-9.]', '')::float as h24_avg_sale_usd,
        fetched_at,
        current_timestamp() as ingested_at
    from {{ ref('stg_trending_nfts') }}
    {% if is_incremental() %}
    where fetched_at > (select max_fetched from latest)
    {% endif %}
)

select * from nfts
