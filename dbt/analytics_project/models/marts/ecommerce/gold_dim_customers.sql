{{ config(materialized='table') }}

select
    customer_id,
    min(order_ts) as first_order_ts,
    max(order_ts) as last_order_ts,
    count(distinct order_id) as lifetime_order_count,
    min(country) as primary_country,
    min(currency) as primary_currency
from {{ ref('silver_orders') }}
where customer_id is not null
group by 1
