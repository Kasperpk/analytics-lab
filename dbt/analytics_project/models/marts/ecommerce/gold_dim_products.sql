{{ config(materialized='table') }}

select
    product_id,
    sku,
    product_name,
    product_type,
    category,
    brand,
    cost_price,
    list_price,
    currency,
    is_active,
    created_at
from {{ ref('silver_products') }}
