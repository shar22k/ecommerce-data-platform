with source as (

    select *
    from {{ source('ecommerce_raw', 'customers') }}

),

renamed as (

    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        trim(customer_city) as customer_city,
        upper(trim(customer_state)) as customer_state,

        _ingested_at,
        _batch_id,
        _source_system,
        _source_table,
        _silver_processed_at

    from source

)

select *
from renamed
