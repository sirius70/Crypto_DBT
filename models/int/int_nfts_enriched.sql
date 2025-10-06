{{ config(
    materialized='incremental',
    unique_key=['nft_id', 'fetched_at', 'ingested_at']
) }}

with nfts as (
    select
        nft_id,
        name,
        symbol,
        native_currency,
        floor_price_native,
        floor_price_pct_24h,
        TO_NUMBER(REGEXP_REPLACE(floor_price_str, '[^0-9.]', '')) AS floor_price_usd,
        TO_NUMBER(REGEXP_REPLACE(h24_volume, '[^0-9.]', '')) AS h24_volume_usd,
        TO_NUMBER(REGEXP_REPLACE(h24_avg_sale_price, '[^0-9.]', '')) AS h24_avg_sale_usd,
        fetched_at,
        current_timestamp() as ingested_at
    from {{ ref('stg_trending_nfts') }}
    {% if is_incremental() %}
        where fetched_at::timestamp_ntz > (
      select coalesce(max(fetched_at), '1970-01-01'::timestamp_ntz) from {{ this }}
  )
  {% endif %}
)

select * from nfts
