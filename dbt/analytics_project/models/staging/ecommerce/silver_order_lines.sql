-- Silver is persisted as incremental merge so daily loads perform upserts:
-- new order_line_ids are inserted, existing order_line_ids are updated when values change.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_line_id',
    schema='silver'
) }}

with typed as (
    select
        order_line_id,
        order_id,
        product_id,
        sku,
        try_cast(quantity as bigint) as quantity,
        try_cast(unit_price as double) as unit_price,
        try_cast(line_discount_amount as double) as line_discount_amount,
        try_cast(tax_rate as double) as tax_rate,
        try_cast(line_ts as timestamp) as line_ts,
        _file_date,
        _source_file
    from {{ ref('bronze_order_lines') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by order_line_id
            order by _file_date desc, line_ts desc nulls last
        ) as _rn
    from typed
    where order_line_id is not null
)

select
    order_line_id,
    order_id,
    product_id,
    sku,
    quantity,
    unit_price,
    line_discount_amount,
    tax_rate,
    line_ts,
    _file_date,
    _source_file
from ranked
where _rn = 1
    and order_line_id like 'L20%'
    and order_id like 'O20%'
    and product_id like 'P%'
