with nfts as (
    select *
    from {{ ref('stg_trending_nfts') }}
),

cleaned as (
    select
        nft_id,
        name,
        symbol,
        native_currency,
        floor_price_native,
        floor_price_pct_24h,
        regexp_replace(floor_price_str, '[^0-9.]', '')::float as floor_price_usd,
        regexp_replace(h24_volume, '[^0-9.]', '')::float as h24_volume_usd,
        regexp_replace(h24_avg_sale_price, '[^0-9.]', '')::float as h24_avg_sale_usd
    from nfts
)

select * from cleaned
