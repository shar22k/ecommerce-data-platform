select *
from {{ ref('fact_orders') }}
where
    total_item_value < 0
    or total_freight_value < 0
    or total_order_value < 0
