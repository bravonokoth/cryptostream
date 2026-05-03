with staged as (
    select * from {{ ref('stg_crypto_prices') }}
),
trends as (
    select
        coin_id,
        date_trunc('hour', ingested_at_ts) as trend_hour,
        avg(current_price_usd) as avg_price_usd,
        max(current_price_usd) as max_price_usd,
        min(current_price_usd) as min_price_usd,
        count(event_id) as event_count
    from staged
    where ingested_at_ts >= current_timestamp - interval '24 hours'
    group by 1, 2
)
select * from trends
