{{ config(
    materialized='incremental',
    unique_key=['category_name', 'fetched_at', 'ingested_at'],
    on_schema_change='sync_all_columns'
) }}


-- Pull only new records from intermediate model
with base as (
    select *
    from {{ ref('int_coins_enriched') }}
    {% if is_incremental() %}
        where fetched_at::timestamp_ntz > (
      select coalesce(max(fetched_at), '1970-01-01'::timestamp_ntz) from {{ this }}
  )
  {% endif %}
),

-- Aggregate by category
aggregated as (
    select
        category_name,
        max(fetched_at) as fetched_at,  -- keep a reference to latest batch
        current_timestamp() as ingested_at,
        count(distinct coin_id) as num_coins,
        avg(price_change_pct_24h) as avg_daily_change,
        sum(market_cap) as total_market_cap,
        sum(total_volume) as total_volume
    from base
    group by category_name
)

-- Final result
select *
from aggregated
