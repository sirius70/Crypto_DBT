{{ config(
    materialized='incremental',
    unique_key=['category_name', 'ingested_at', 'ingested_at']  
) }}

with base as (
    select *
    from {{ ref('int_coins_enriched') }}
),

aggregated as (
    select
        category_name,
        current_timestamp() as ingested_at,
        count(distinct coin_id) as num_coins,
        avg(price_change_pct_24h) as avg_daily_change,
        sum(market_cap) as total_market_cap,
        sum(total_volume) as total_volume
    from base
    group by category_name
)

select * from aggregated
