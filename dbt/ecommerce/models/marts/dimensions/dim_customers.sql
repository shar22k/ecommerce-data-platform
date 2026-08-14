with customers as (

    select *
    from {{ ref('stg_customers') }}

),

final as (

    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,

        _ingested_at,
        _batch_id,
        _source_system,
        _silver_processed_at

    from customers

)

select *
from final
