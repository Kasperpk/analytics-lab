with silver_orders as (
    select count(distinct order_id) as cnt
    from {{ ref('silver_orders') }}
),

gold_orders as (
    select count(distinct order_id) as cnt
    from {{ ref('gold_dim_orders') }}
)

select
    s.cnt as silver_order_count,
    g.cnt as gold_order_count
from silver_orders s
cross join gold_orders g
where s.cnt <> g.cnt
