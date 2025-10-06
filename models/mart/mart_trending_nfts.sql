{{ config(
    materialized='incremental',
    unique_key=['nft_id', 'fetched_at', 'ingested_at'],
    on_schema_change='sync_all_columns'
) }}


with base as (
    select *
    from {{ ref('int_nfts_enriched') }}
    {% if is_incremental() %}
        where fetched_at > (
            select coalesce(max(fetched_at), '1970-01-01'::timestamp_ntz) 
            from {{ this }}
        )
    {% endif %}
),

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

select *
from ranked
where nft_rank <= 20
