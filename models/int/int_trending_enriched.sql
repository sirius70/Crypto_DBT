{{ config(
    materialized='incremental',
    unique_key=['coin_id', 'fetched_at', 'ingested_at']
) }}

with trending as (
    select
        coin_id,
        coalesce(name, '') as name,
        coalesce(symbol, '') as symbol,
        coalesce(market_cap_rank, 0) as market_cap_rank,
        coalesce(image_thumb, '') as image_thumb,
        coalesce(image_small, '') as image_small,
        coalesce(image_large, '') as image_large,
        coalesce(slug, '') as slug,

        -- Convert nulls and add +1 to score
        coalesce(score, 0) + 1 as rank_position,

        coalesce(current_price_usd, 0) as current_price_usd,
        coalesce(current_price_btc, 0) as current_price_btc,

        -- Clean string fields (remove symbols, cast to numeric)
        try_to_number(regexp_replace(market_cap_str, '[^0-9.]', '')) as market_cap_usd,
        try_to_number(regexp_replace(total_volume_str, '[^0-9.]', '')) as total_volume_usd,

        coalesce(market_cap_btc, 0) as market_cap_btc,
        coalesce(total_volume_btc, 0) as total_volume_btc,
        coalesce(sparkline_url, '') as sparkline_url,
        coalesce(content_title, '') as content_title,
        coalesce(content_description, '') as content_description,

        fetched_at,
        current_timestamp() as ingested_at

    from {{ ref('stg_coins_trending') }}

    {% if is_incremental() %}
        where fetched_at::timestamp_ntz > (
      select coalesce(max(fetched_at), '1970-01-01'::timestamp_ntz) from {{ this }}
  )
  {% endif %}
)

select *
from trending