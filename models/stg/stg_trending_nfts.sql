with raw as (
    select data as v
    from {{ source('raw', 'raw_coins_trending') }}
),
nfts as (
    select
        nft.value:id::string                  as nft_id,
        nft.value:name::string                as name,
        nft.value:symbol::string              as symbol,
        nft.value:nft_contract_id::int        as nft_contract_id,
        nft.value:thumb::string               as image_thumb,
        nft.value:native_currency_symbol::string as native_currency,
        nft.value:floor_price_in_native_currency::float as floor_price_native,
        nft.value:floor_price_24h_percentage_change::float as floor_price_pct_24h,
        nft.value:data:floor_price::string    as floor_price_str,
        nft.value:data:h24_volume::string     as h24_volume,
        nft.value:data:h24_average_sale_price::string as h24_avg_sale_price,
        nft.value:data:sparkline::string      as sparkline_url
    from raw,
    lateral flatten(input => v:nfts) nft
)

select * from nfts