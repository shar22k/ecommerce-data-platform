with order_items as (

    select *
    from {{ ref('stg_order_items') }}

),

aggregated as (

    select
        order_id,

        count(*) as total_items,

        count(distinct product_id) as distinct_products,

        count(distinct seller_id) as distinct_sellers,

        sum(price) as total_item_value,

        sum(freight_value) as total_freight_value,

        sum(price + freight_value) as total_order_value,

        min(price) as minimum_item_price,

        max(price) as maximum_item_price,

        avg(price) as average_item_price,

        min(shipping_limit_date) as first_shipping_limit_date,

        max(shipping_limit_date) as last_shipping_limit_date

    from order_items

    group by order_id

)

select *
from aggregated
