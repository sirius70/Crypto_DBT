{{ config(
    materialized='incremental',
    unique_key=['coin_id', 'fetched_at', 'ingested_at']
) }}

with raw as (
    select 
        data as v, 
        meta:fetched_at::timestamp_ntz as fetched_at
    from {{ source('raw', 'raw_coins_trending') }}
),
coins as (
    select
        coin.value:item:id::string              as coin_id,
        coin.value:item:coin_id::int            as coin_numeric_id,
        coin.value:item:name::string            as name,
        coin.value:item:symbol::string          as symbol,
        coin.value:item:market_cap_rank::int    as market_cap_rank,
        coin.value:item:thumb::string           as image_thumb,
        coin.value:item:small::string           as image_small,
        coin.value:item:large::string           as image_large,
        coin.value:item:slug::string            as slug,
        coin.value:item:price_btc::float        as price_btc,
        coin.value:item:score::int              as score,
        coin.value:item:data:price::float       as current_price_usd,
        coin.value:item:data:price_btc::float   as current_price_btc,
        coin.value:item:data:market_cap::string as market_cap_str, 
        coin.value:item:data:market_cap_btc::float as market_cap_btc,
        coin.value:item:data:total_volume::string as total_volume_str,
        coin.value:item:data:total_volume_btc::float as total_volume_btc,
        coin.value:item:data:sparkline::string  as sparkline_url,
        coin.value:item:data:content:title::string       as content_title,
        coin.value:item:data:content:description::string as content_description,
        fetched_at,
        current_timestamp() as ingested_at
    from raw,
    lateral flatten(input => v:coins) coin
)

select * from coins
