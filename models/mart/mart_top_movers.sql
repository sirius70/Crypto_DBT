{{ config(
    materialized='incremental',
    unique_key=['coin_id', 'ingested_at'],
    on_schema_change='sync_all_columns'
) }}


-- Filter new data from intermediate layer
with base as (
    select *
    from {{ ref('int_coins_enriched') }}
    {% if is_incremental() %}
        where ingested_at::timestamp_ntz > (
      select coalesce(max(ingested_at), '1970-01-01'::timestamp_ntz) from {{ this }}
  )
  {% endif %}
),

-- Rank movers and add ingestion timestamp
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

-- Final selection (top 20 movers)
select *
from ranked
where mover_rank <= 20
