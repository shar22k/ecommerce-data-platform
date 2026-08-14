with payments as (

    select *
    from {{ ref('stg_order_payments') }}

),

orders as (

    select
        order_id,
        customer_id,
        order_purchase_timestamp

    from {{ ref('stg_orders') }}

),

final as (

    select
        p.order_id,
        p.payment_sequential,

        o.customer_id,

        to_number(
            to_char(
                o.order_purchase_timestamp::date,
                'YYYYMMDD'
            )
        ) as order_date_key,

        p.payment_type,
        p.payment_installments,
        p.payment_value,

        p._ingested_at,
        p._batch_id,
        p._source_system,
        p._silver_processed_at

    from payments p

    inner join orders o
        on p.order_id = o.order_id

)

select *
from final
