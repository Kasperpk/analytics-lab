-- Reconciliation test: number of business orders should be consistent
-- between silver (operationally cleaned) and gold (BI-serving dimension).
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
-- Singular dbt tests fail when rows are returned.
-- This returns a row only when there is a mismatch.
where s.cnt <> g.cnt
