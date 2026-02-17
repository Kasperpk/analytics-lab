{{ config(materialized='table') }}

with base as (
    select *
    from {{ ref('stg_finance_prices') }}
),

returns as (
    select
        ticker,
        date,
        close,
        close / lag(close) over (partition by ticker order by date) - 1 as daily_return
    from base
),

moving_avg as (
    select
        *,
        avg(close) over (
            partition by ticker
            order by date
            rows between 19 preceding and current row
        ) as ma_20,

        avg(close) over (
            partition by ticker
            order by date
            rows between 49 preceding and current row
        ) as ma_50
    from returns
),

volatility as (
    select
        *,
        stddev(daily_return) over (
            partition by ticker
            order by date
            rows between 29 preceding and current row
        ) * sqrt(252) as rolling_vol_30d
    from moving_avg
)

select *
from volatility
