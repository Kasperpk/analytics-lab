-- Data quality rule: quantities in curated silver order lines
-- must not be negative after cleansing and key filtering.
select
  order_line_id,
  quantity
from {{ ref('silver_order_lines') }}
-- Singular dbt test fails when any violating rows exist.
where quantity < 0
