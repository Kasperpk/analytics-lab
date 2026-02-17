-- Silver is persisted as incremental merge so daily loads perform upserts:
-- new order_ids are inserted, existing order_ids are updated when attributes change.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    schema='silver'
) }}

with typed as (
    select
        order_id,
        customer_id,
        try_cast(order_ts as timestamp) as order_ts,
        country,
        currency,
        payment_method,
        try_cast(shipping_amount as double) as shipping_amount,
        try_cast(discount_amount as double) as discount_amount,
        order_status,
        try_cast(source_file_date as date) as source_file_date,
        _file_date,
        _source_file
    from {{ ref('bronze_orders') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by order_id
            order by _file_date desc, order_ts desc nulls last
        ) as _rn
    from typed
    where order_id is not null
)

select
    order_id,
    customer_id,
    order_ts,
    country,
    currency,
    payment_method,
    shipping_amount,
    discount_amount,
    order_status,
    source_file_date,
    _file_date,
    _source_file
from ranked
where _rn = 1
