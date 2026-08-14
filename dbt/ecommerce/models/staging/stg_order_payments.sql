with source as (

    select *
    from {{ source('ecommerce_raw', 'order_payments') }}

),

renamed as (

    select
        order_id,
        payment_sequential,

        lower(trim(payment_type)) as payment_type,

        payment_installments,
        payment_value,

        _ingested_at,
        _batch_id,
        _source_system,
        _source_table,
        _silver_processed_at

    from source

)

select *
from renamed
