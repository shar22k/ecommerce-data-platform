select *
from {{ ref('fact_orders') }}
where total_items < 0
