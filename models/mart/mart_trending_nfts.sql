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
    from {{ ref('int_nfts_enriched') }}
    {% if is_incremental() %}
    where ingested_at > (select max_ingested from latest)
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
