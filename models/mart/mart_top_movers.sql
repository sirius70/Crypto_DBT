{{ config(materialized='incremental') }}

{% if is_incremental() %}
with latest as (
    select coalesce(max(fetched_at), '1970-01-01'::timestamp_ntz) as max_fetched
    from {{ this }}
),
{% else %}
with
{% endif %}
base as (
    select *
    from {{ ref('int_coins_enriched') }}
    {% if is_incremental() %}
    where fetched_at > (select max_fetched from latest)
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
