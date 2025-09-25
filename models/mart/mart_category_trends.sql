{{ config(materialized='incremental') }}

{% if is_incremental() %}
with latest as (
    select coalesce(max(ingested_at), '1970-01-01'::timestamp_ntz) as max_ingested
    from {{ this }}
),
{% else %}
with
{% endif %}
base as (
    select *
    from {{ ref('int_coins_enriched') }}
    {% if is_incremental() %}
    where ingested_at > (select max_ingested from latest)
    {% endif %}
),
aggregated as (
    select
        category_name,
        current_timestamp() as ingested_at,
        count(distinct coin_id) as num_coins,
        avg(price_change_pct_24h) as avg_daily_change,
        sum(market_cap) as total_market_cap,
        sum(total_volume) as total_volume
    from base
    group by category_name
)

select * from aggregated
