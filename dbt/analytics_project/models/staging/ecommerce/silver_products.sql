{{ config(materialized='table', schema='silver') }}

with typed as (
    select
        product_id,
        sku,
        product_name,
        product_type,
        category,
        brand,
        try_cast(cost_price as double) as cost_price,
        try_cast(list_price as double) as list_price,
        case lower(is_active)
            when 'true' then true
            when 'false' then false
            else null
        end as is_active,
        currency,
        try_cast(created_at as timestamp) as created_at,
        _file_date,
        _source_file
    from {{ ref('bronze_products') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by coalesce(product_id, sku)
            order by _file_date desc, created_at desc nulls last
        ) as _rn
    from typed
    where coalesce(product_id, sku) is not null
)

select
    product_id,
    sku,
    product_name,
    product_type,
    category,
    brand,
    cost_price,
    list_price,
    is_active,
    currency,
    created_at,
    _file_date,
    _source_file
from ranked
where _rn = 1
    and product_id is not null
    and product_id like 'P%'
