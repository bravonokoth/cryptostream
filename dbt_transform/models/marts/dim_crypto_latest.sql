with staged as (
    select * from {{ ref('stg_crypto_prices') }}
),
ranked as (
    select
        *,
        row_number() over (partition by coin_id order by ingested_at_ts desc) as rn
    from staged
)
select
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
    source,
    ingested_at_ts as last_updated_at
from ranked
where rn = 1
