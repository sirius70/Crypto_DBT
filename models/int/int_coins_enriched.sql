{{ config(
    materialized='incremental'
) }}

with markets as (
  select * from {{ ref('stg_coins_markets') }}
),

trending as (
  select coin_id, score, ingested_at as trending_ingested_at
  from {{ ref('stg_coins_trending') }}
)

select
  m.coin_id,
  m.symbol,
  m.name,
  m.current_price,
  m.market_cap,
  m.total_volume,
  m.price_change_pct_24h,
  m.market_cap_rank,
  m.all_time_high,
  m.all_time_low,
  -- trending enrichment: if there's a trending record at same ingest time (or nearest)
  case when t.coin_id is not null then true else false end as is_trending,
  t.score as trending_score,
  greatest(m.ingested_at, t.trending_ingested_at) as ingested_at
from markets m
left join trending t
  on m.coin_id = t.coin_id
  and t.trending_ingested_at = m.ingested_at  -- simple alignment by time

{% if is_incremental() %}
where ingested_at > (select coalesce(max(ingested_at), '1970-01-01'::timestamp_ntz) from {{ this }})
{% endif %}
