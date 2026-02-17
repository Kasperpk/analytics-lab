{{ config(materialized='table') }}

with refunds_by_line as (
    select
        order_line_id,
        sum(coalesce(refund_amount, 0)) as refund_amount
    from {{ ref('silver_refunds') }}
    group by 1
),

line_enriched as (
    select
        cast(o.order_ts as date) as order_date,
        l.order_line_id,
        l.order_id,
        o.customer_id,
        l.product_id,
        l.sku,
        p.product_type,
        coalesce(l.quantity, 0) as gross_quantity,
        coalesce(l.unit_price, 0) as unit_price,
        coalesce(r.refund_amount, 0) as refund_amount
    from {{ ref('silver_order_lines') }} l
    inner join {{ ref('silver_orders') }} o
        on l.order_id = o.order_id
    left join {{ ref('silver_products') }} p
        on l.product_id = p.product_id
    left join refunds_by_line r
        on l.order_line_id = r.order_line_id
)

select
    order_date,
    order_line_id,
    order_id,
    customer_id,
    product_id,
    sku,
    product_type,
    gross_quantity,
    least(gross_quantity, coalesce(try_cast(round(refund_amount / nullif(unit_price, 0)) as bigint), 0)) as refunded_quantity,
    gross_quantity - least(gross_quantity, coalesce(try_cast(round(refund_amount / nullif(unit_price, 0)) as bigint), 0)) as net_quantity,
    gross_quantity * unit_price as gross_sales_amount,
    refund_amount,
    (gross_quantity * unit_price) - refund_amount as net_sales_amount
from line_enriched
