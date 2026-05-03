with raw as (
    select * from {{ source('public', 'raw_prices') }}
)
select
    event_id,
    to_timestamp(ingested_at / 1000.0) as ingested_at_ts,
    coin_id,
    symbol,
    name,
    current_price_usd,
    market_cap_usd,
    total_volume_usd,
    price_change_24h,
    price_change_pct_24h,
    market_cap_rank,
    ath_usd,
    circulating_supply,
    source
from raw
