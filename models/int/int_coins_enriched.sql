{{ config(
    materialized='incremental',
    unique_key=['coin_id', 'fetched_at', 'ingested_at']
) }}

with markets as (
    select *
    from {{ ref('stg_coins_markets') }}
    {% if is_incremental() %}
        where fetched_at::timestamp_ntz > (
      select coalesce(max(fetched_at), '1970-01-01'::timestamp_ntz) from {{ this }}
  )
  {% endif %}
),
trending as (
    select coin_id, score, fetched_at
    from {{ ref('stg_coins_trending') }}
    {% if is_incremental() %}
        where fetched_at::timestamp_ntz > (
      select coalesce(max(fetched_at), '1970-01-01'::timestamp_ntz) from {{ this }}
  )
  {% endif %}
),
categories as (
    select 
        category_id, 
        name as category_name, 
        market_cap_usd, 
        total_volume_usd, 
        fetched_at
    from {{ ref('stg_trending_categories') }}
    {% if is_incremental() %}
        where fetched_at::timestamp_ntz > (
      select coalesce(max(fetched_at), '1970-01-01'::timestamp_ntz) from {{ this }}
  )
  {% endif %}
),
joined as (
    select
        m.coin_id,
        m.symbol,
        m.name,
        m.market_cap_rank,
        m.current_price,
        m.market_cap,
        m.total_volume,
        m.price_change_pct_24h,
        m.circulating_supply,
        m.all_time_high,
        m.all_time_low,
        m.last_updated,

        -- trending flag
        case when t.coin_id is not null then true else false end as is_trending,
        t.score as trending_score,

        -- category enrich
        c.category_name,
        c.market_cap_usd as category_market_cap,
        c.total_volume_usd as category_volume,

        -- audit columns
        m.fetched_at,
        current_timestamp() as ingested_at
    from markets m
    left join trending t on m.coin_id = t.coin_id
    left join categories c on 1=1
)

select * from joined
