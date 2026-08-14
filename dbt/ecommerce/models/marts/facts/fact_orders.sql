{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

with orders as (

    select
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date
    from {{ ref('stg_orders') }}

    {% if is_incremental() %}

    where order_purchase_timestamp >= (
        select coalesce(
            max(order_purchase_timestamp),
            '1900-01-01'::timestamp_ntz
        )
        from {{ this }}
    )

    {% endif %}

),

order_items as (

    select
        order_id,
        total_items,
        total_item_value,
        total_freight_value,
        total_order_value
    from {{ ref('int_order_items_aggregated') }}

),

final as (

    select
        o.order_id,
        o.customer_id,

        to_number(
            to_char(
                cast(o.order_purchase_timestamp as date),
                'YYYYMMDD'
            )
        ) as order_date_key,

        o.order_status,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,

        coalesce(i.total_items, 0) as total_items,
        coalesce(i.total_item_value, 0) as total_item_value,
        coalesce(i.total_freight_value, 0) as total_freight_value,
        coalesce(i.total_order_value, 0) as total_order_value

    from orders o

    left join order_items i
        on o.order_id = i.order_id

)

select *
from final
