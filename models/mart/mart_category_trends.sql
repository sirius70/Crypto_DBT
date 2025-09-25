{{ config(materialized='incremental') }}

with base as (
    select
        category_name,
        coin_id,
        price_change_pct_24h,
        market_cap,
        total_volume,
        ingested_at
    from {{ ref('int_coins_enriched') }}
)

select
    category_name,
    ingested_at,
    count(distinct coin_id) as num_coins,
    avg(price_change_pct_24h) as avg_daily_change,
    sum(market_cap) as total_market_cap,
    sum(total_volume) as total_volume
from base
group by category_name, ingested_at

{% if is_incremental() %}
having ingested_at > (
    select coalesce(max(ingested_at), '1970-01-01'::timestamp_ntz)
    from {{ this }}
)
{% endif %}
