{{ config(materialized='incremental') }}

with base as (
  select
    coin_id,
    name,
    symbol,
    current_price,
    price_change_pct_24h,
    market_cap,
    ingested_at
  from {{ ref('int_coins_enriched') }}
)

, ranked AS (
  select
    *,
    row_number() over (partition by ingested_at order by abs(price_change_pct_24h) desc) as mover_rank
  from base
)

select
  coin_id,
  name,
  symbol,
  current_price,
  price_change_pct_24h,
  market_cap,
  ingested_at,
  mover_rank
from ranked
where mover_rank <= 50

{% if is_incremental() %}
and ingested_at > (select coalesce(max(ingested_at), '1970-01-01'::timestamp_ntz) from {{ this }})
{% endif %}
