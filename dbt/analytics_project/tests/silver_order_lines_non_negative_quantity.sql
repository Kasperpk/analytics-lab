select
  order_line_id,
  quantity
from {{ ref('silver_order_lines') }}
where quantity < 0
