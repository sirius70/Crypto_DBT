-- show top 20 coins by 24h % change for the latest ingested_at

{{ config(
    materialized = 'incremental',
    unique_key = ['coin_id', 'ingested_at'],
    on_schema_change = 'sync_all_columns'
) }}

with latest_batch as (
    -- Get latest ingested_at timestamp
    select max(ingested_at) as latest_ingested_at
    from {{ ref('int_coins_enriched') }}
),

filtered as (
    select
        c.coin_id,
        c.symbol,
        c.name,
        c.current_price,
        c.market_cap,
        c.total_volume,
        c.price_change_pct_24h,
        c.price_diff,
        c.price_change_pct_since_prev,
        c.market_cap_rank,
        c.fetched_at,
        c.ingested_at
    from {{ ref('int_coins_enriched') }} c
    where c.ingested_at = (select latest_ingested_at from latest_batch)
),

ranked as (
    select
        *,
        row_number() over (order by price_change_pct_24h desc) as mover_rank
    from filtered
)

select *
from ranked
where mover_rank <= 20
