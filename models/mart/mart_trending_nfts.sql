{{ config(
    materialized='incremental',
    unique_key=['nft_id', 'fetched_at', 'ingested_at'],
    on_schema_change='sync_all_columns'
) }}

-- 1. Compute last fetched in mart table
with last_fetched as (
    select coalesce(max(fetched_at), '1970-01-01'::timestamp_ntz) as max_fetched
    from {{ this }}
),

-- 2. Pull base rows from enriched table
base as (
    select n.*
    from {{ ref('int_nfts_enriched') }} n
    cross join last_fetched l
    {% if is_incremental() %}
        where n."FETCHED_AT" > l.max_fetched
    {% endif %}
),

-- 3. Rank NFTs
ranked as (
    select
        nft_id,
        name,
        symbol,
        floor_price_usd,
        floor_price_pct_24h,
        h24_volume_usd,
        current_timestamp() as ingested_at,
        row_number() over (order by floor_price_pct_24h desc) as nft_rank
    from base
)

-- 4. Only top 20
select *
from ranked
where nft_rank <= 20
