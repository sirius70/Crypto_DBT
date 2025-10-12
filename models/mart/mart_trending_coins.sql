{{ config(
    materialized='incremental',
    unique_key=['coin_id', 'fetched_at', 'ingested_at'],
    on_schema_change='sync_all_columns'
) }}

-- Pulls the latest clean trending data from your intermediate model
with base as (
    select
        coin_id,
        name,
        symbol,
        market_cap_rank,
        image_thumb,
        image_small,
        image_large,
        slug,
        rank_position,
        current_price_usd,
        current_price_btc,
        market_cap_usd,
        total_volume_usd,
        market_cap_btc,
        total_volume_btc,
        sparkline_url,
        content_title,
        content_description,
        fetched_at,
        ingested_at
    from {{ ref('int_trending_enriched') }}
    {% if is_incremental() %}
        where fetched_at > (select coalesce(max(fetched_at), '1970-01-01'::timestamp_ntz) from {{ this }})
    {% endif %}
),

-- Adds analytic metrics: liquidity ratio, rank tiers, and trend categories
metrics as (
    select
        *,
        -- Volume-to-market-cap ratio: shows liquidity
        case 
            when market_cap_usd > 0 then total_volume_usd / market_cap_usd
            else 0 
        end as volume_to_marketcap_ratio,

        -- Market tier based on rank
        case
            when market_cap_rank between 1 and 10 then 'Top 10'
            when market_cap_rank between 11 and 50 then 'Top 50'
            when market_cap_rank between 51 and 100 then 'Top 100'
            else 'Others'
        end as market_tier,

        -- Trending category based on rank_position (from trending endpoint)
        case
            when rank_position between 1 and 5 then 'Hot Movers'
            when rank_position between 6 and 10 then 'Trending'
            else 'Lower Trend'
        end as trend_category
    from base
),

-- Aggregates for Power BI KPI cards (total coins, total market cap, average rank, etc.)
summary as (
    select
        fetched_at::date as trend_date,
        count(distinct coin_id) as total_trending_coins,
        avg(rank_position) as avg_rank_position,
        avg(current_price_usd) as avg_trending_price,
        sum(market_cap_usd) as total_trending_marketcap,
        sum(total_volume_usd) as total_trending_volume,
        current_timestamp() as ingested_at
    from metrics
    group by 1
)

-- Merges both detailed and summary data for rich dashboard visuals
select
    m.*,
    s.total_trending_coins,
    s.avg_rank_position,
    s.avg_trending_price,
    s.total_trending_marketcap,
    s.total_trending_volume
from metrics m
left join summary s on m.fetched_at::date = s.trend_date
