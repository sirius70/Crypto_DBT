{{ config(materialized='incremental') }}

with nfts as (
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
        ingested_at
    from {{ ref('stg_trending_nfts') }}
)

select *
from nfts

{% if is_incremental() %}
where ingested_at > (
    select coalesce(max(ingested_at), '1970-01-01'::timestamp_ntz)
    from {{ this }}
)
{% endif %}
