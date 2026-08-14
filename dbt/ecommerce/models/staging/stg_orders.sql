with source as (

    select *
    from {{ source('ecommerce_raw', 'orders') }}

),

renamed as (

    select
        order_id,
        customer_id,

        lower(trim(order_status)) as order_status,

        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,

        _ingested_at,
        _batch_id,
        _source_system,
        _source_table,
        _silver_processed_at

    from source

)

select *
from renamed
