with orders as (

    select *
    from {{ ref('stg_orders') }}

),

order_items as (

    select *
    from {{ ref('int_order_items_aggregated') }}

),

final as (

    select
        o.order_id,
        o.customer_id,

        to_number(
            to_char(
                o.order_purchase_timestamp::date,
                'YYYYMMDD'
            )
        ) as order_date_key,

        o.order_status,

        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,

        oi.total_items,
        oi.distinct_products,
        oi.distinct_sellers,

        oi.total_item_value,
        oi.total_freight_value,
        oi.total_order_value,

        oi.minimum_item_price,
        oi.maximum_item_price,
        oi.average_item_price,

        oi.first_shipping_limit_date,
        oi.last_shipping_limit_date,

        datediff(
            day,
            o.order_purchase_timestamp,
            o.order_delivered_customer_date
        ) as delivery_days,

        datediff(
            day,
            o.order_purchase_timestamp,
            o.order_estimated_delivery_date
        ) as estimated_delivery_days,

        case
            when
                o.order_delivered_customer_date is not null
                and o.order_estimated_delivery_date is not null
                and o.order_delivered_customer_date
                    <= o.order_estimated_delivery_date
            then true
            else false
        end as delivered_on_time,

        o._ingested_at,
        o._batch_id,
        o._source_system,
        o._silver_processed_at

    from orders o

    left join order_items oi
        on o.order_id = oi.order_id

)

select *
from final
