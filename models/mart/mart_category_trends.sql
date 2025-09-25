with base as (
    select *
    from {{ ref('int_coins_enriched') }}
)

select
    category_name,
    count(distinct coin_id) as num_coins,
    avg(price_change_pct_24h) as avg_daily_change,
    sum(market_cap) as total_market_cap,
    sum(total_volume) as total_volume
from base
group by category_name
