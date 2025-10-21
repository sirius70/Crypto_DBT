{{ config(
    materialized='incremental',
    unique_key=['coin_id', 'fetched_at', 'ingested_at'],
    on_schema_change='sync_all_columns'
) }}

with base as (
    select
        coin_id,
        coalesce(symbol, '') as symbol,
        coalesce(name, '') as name,
        coalesce(current_price, 0.00) as current_price,
        coalesce(market_cap, 0.00) as market_cap,
        coalesce(market_cap_rank, -1) as market_cap_rank,
        coalesce(total_volume, 0.00) as total_volume,
        coalesce(price_change_pct_24h, 0.00) as price_change_pct_24h,
        coalesce(circulating_supply, 0.00) as circulating_supply,
        coalesce(all_time_high, 0.00) as all_time_high,
        coalesce(all_time_low, 0.00) as all_time_low,
        last_updated,
        fetched_at,
        current_timestamp() as ingested_at
    from {{ ref('stg_coins_markets') }}

    {% if is_incremental() %}
        where fetched_at::timestamp_ntz > (
            select coalesce(max(fetched_at), '1970-01-01'::timestamp_ntz)
            from {{ this }}
        )
    {% endif %}
),

-- include past data to allow window functions to work properly
combined as (
    select * from base
    {% if is_incremental() %}
    union all
    select * from {{ this }}
    {% endif %}
),

with_prev as (
    select
        coin_id,
        symbol,
        name,
        current_price,
        coalesce(
            lag(current_price) over (
                partition by coin_id order by fetched_at
            ),
            0.00
        ) as prev_price,
        market_cap,
        market_cap_rank,
        total_volume,
        price_change_pct_24h,
        circulating_supply,
        all_time_high,
        all_time_low,
        fetched_at,
        last_updated,
        ingested_at
    from combined
)

select
    *,
    case
        when prev_price = 0.00 then 0.00
        else current_price - prev_price
    end as price_diff,
    case
        when prev_price = 0.00 then 0.00
        else ((current_price - prev_price) / prev_price) * 100
    end as price_change_pct_since_prev
from with_prev
