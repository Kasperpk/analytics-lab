with expected as (
    select
        l.order_id,
        sum(coalesce(l.quantity, 0) * coalesce(l.unit_price, 0)) as expected_gross_amount
    from {{ ref('silver_order_lines') }} l
    inner join {{ ref('silver_orders') }} o
        on l.order_id = o.order_id
    group by 1
),

actual as (
    select
        order_id,
        sum(coalesce(gross_sales_amount, 0)) as actual_gross_amount
    from {{ ref('gold_fct_sales') }}
    group by 1
),

recon as (
    select
        coalesce(e.order_id, a.order_id) as order_id,
        coalesce(e.expected_gross_amount, 0) as expected_gross_amount,
        coalesce(a.actual_gross_amount, 0) as actual_gross_amount
    from expected e
    full outer join actual a
        on e.order_id = a.order_id
)

select
    order_id,
    expected_gross_amount,
    actual_gross_amount,
    abs(expected_gross_amount - actual_gross_amount) as diff
from recon
where abs(expected_gross_amount - actual_gross_amount) > 0.01
