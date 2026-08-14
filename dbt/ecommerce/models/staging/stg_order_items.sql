with source as (

    select *
    from {{ source('ecommerce_raw', 'order_items') }}

),

renamed as (

    select
        order_id,
        order_item_id,
        product_id,
        seller_id,

        shipping_limit_date,

        price,
        freight_value,

        _ingested_at,
        _batch_id,
        _source_system,
        _source_table,
        _silver_processed_at

    from source

)

select *
from renamed
