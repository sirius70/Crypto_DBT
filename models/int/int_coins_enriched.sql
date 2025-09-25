{{ config(materialized='incremental') }}

with latest as (
    select coalesce(max(fetched_at), '1970-01-01'::timestamp_ntz) as max_fetched
    from {{ this }}
)

, markets as (
    select *
    from {{ ref('stg_coins_markets') }}
    {% if is_incremental() %}
    where fetched_at > (select max_fetched from latest)
    {% endif %}
)

, trending as (
    select coin_id, score, fetched_at
    from {{ ref('stg_coins_trending') }}
)

, categories as (
    select 
        category_id, 
        name as category_name, 
        market_cap_usd, 
        total_volume_usd, 
        fetched_at
    from {{ ref('stg_trending_categories') }}
)

, joined as (
    select
        m.coin_id,
        m.symbol,
        m.name,
        m.market_cap_rank,
        m.current_price,
        m.market_cap,
        m.total_volume,
        m.price_change_pct_24h,
        m.circulating_supply,
        m.all_time_high,
        m.all_time_low,
        m.last_updated,

        -- trending flag
        case when t.coin_id is not null then true else false end as is_trending,
        t.score as trending_score,

        -- category enrich (for later analysis)
        c.category_name,
        c.market_cap_usd as category_market_cap,
        c.total_volume_usd as category_volume,

        -- audit columns
        m.fetched_at,                        -- use from markets
        current_timestamp() as ingested_at   -- when this int table was built
    from markets m
    left join trending t on m.coin_id = t.coin_id
    left join categories c on 1=1   -- categories are overall, not per coin
)

select * from joined
