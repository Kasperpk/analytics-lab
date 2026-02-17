{{ config(materialized='table') }}

with base_dates as (
    select distinct cast(order_ts as date) as date_day
    from {{ ref('silver_orders') }}
    where order_ts is not null
)

select
    cast(strftime(date_day, '%Y%m%d') as integer) as date_key,
    date_day,
    cast(strftime(date_day, '%Y') as integer) as year_number,
    cast(strftime(date_day, '%m') as integer) as month_number,
    strftime(date_day, '%B') as month_name,
    cast(strftime(date_day, '%d') as integer) as day_of_month,
    cast(strftime(date_day, '%u') as integer) as day_of_week_iso,
    strftime(date_day, '%A') as day_name,
    cast(strftime(date_day, '%V') as integer) as iso_week_number,
    cast(((cast(strftime(date_day, '%m') as integer) - 1) / 3) + 1 as integer) as quarter_number,
    case when cast(strftime(date_day, '%u') as integer) in (6, 7) then true else false end as is_weekend
from base_dates
