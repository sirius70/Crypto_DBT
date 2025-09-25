{{ config(
    materialized='incremental',
    unique_key=['coin_id', 'ingested_at']  -- safe upsert
) }}

with base as (
    select *
    from {{ ref('int_coins_enriched') }}
),

ranked as (
    select
        coin_id,
        name,
        symbol,
        current_price,
        price_change_pct_24h,
        market_cap_rank,
        current_timestamp() as ingested_at,
        row_number() over (order by abs(price_change_pct_24h) desc) as mover_rank
    from base
)

select *
from ranked
where mover_rank <= 20
