{{ config(materialized='table') }}

select
    order_id,
    customer_id,
    order_ts,
    cast(order_ts as date) as order_date,
    country,
    currency,
    payment_method,
    order_status,
    source_file_date
from {{ ref('silver_orders') }}
where order_id is not null
