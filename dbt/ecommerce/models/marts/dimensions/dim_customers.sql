with customer_history as (

    select *
    from {{ ref('customers_snapshot') }}

),

current_customers as (

    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        dbt_valid_from,
        dbt_valid_to,
        _ingested_at,
        _batch_id,
        _source_system,
        _silver_processed_at
    from customer_history
    where dbt_valid_to is null

)

select *
from current_customers
