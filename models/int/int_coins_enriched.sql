{{ config(materialized='incremental') }}

with latest as (
    select coalesce(max(ingested_at), '1970-01-01'::timestamp_ntz) as max_ingested
    from {{ this }}
)

, coins as (
    select
        coin_id,
        name,
        symbol,
        category_name,
        current_price,
        price_change_pct_24h,
        market_cap,
        total_volume,
        ingested_at
    from {{ ref('stg_coins_markets') }}
    {% if is_incremental() %}
    where ingested_at > (select max_ingested from latest)
    {% endif %}
)

select * from coins
