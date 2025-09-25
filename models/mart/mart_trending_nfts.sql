{{ config(materialized='incremental') }}

with base as (
    select
        nft_id,
        name,
        symbol,
        floor_price_usd,
        floor_price_pct_24h,
        h24_volume_usd,
        ingested_at
    from {{ ref('int_nfts_enriched') }}
)

, ranked as (
    select
        *,
        row_number() over (
            partition by ingested_at
            order by floor_price_pct_24h desc
        ) as nft_rank
    from base
)

select
    nft_id,
    name,
    symbol,
    floor_price_usd,
    floor_price_pct_24h,
    h24_volume_usd,
    ingested_at,
    nft_rank
from ranked
where nft_rank <= 20

{% if is_incremental() %}
and ingested_at > (
    select coalesce(max(ingested_at), '1970-01-01'::timestamp_ntz)
    from {{ this }}
)
{% endif %}
