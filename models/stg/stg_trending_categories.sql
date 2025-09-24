with raw as (
    select data as v
    from {{ source('raw', 'raw_coins_trending') }}
),
cats as (
    select
        cat.value:id::int                     as category_id,
        cat.value:name::string                as name,
        cat.value:slug::string                as slug,
        cat.value:coins_count::int            as coins_count,
        cat.value:market_cap_1h_change::float as market_cap_1h_change,
        cat.value:data:market_cap::float      as market_cap_usd,
        cat.value:data:market_cap_btc::float  as market_cap_btc,
        cat.value:data:total_volume::float    as total_volume_usd,
        cat.value:data:total_volume_btc::float as total_volume_btc,
        cat.value:data:sparkline::string      as sparkline_url
    from raw,
    lateral flatten(input => v:categories) cat
)

select * from cats
