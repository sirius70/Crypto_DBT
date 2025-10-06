{{ config(
    materialized='incremental',
    unique_key=['coin_id', 'fetched_at', 'ingested_at']  
) }}

with base as (
    select *
    from {{ ref('int_coins_enriched') }}

    {% if is_incremental() %}
      where fetched_at > (
          select coalesce(max(fetched_at), '1970-01-01'::timestamp_ntz) from {{ this }}
      )
    {% endif %}

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
