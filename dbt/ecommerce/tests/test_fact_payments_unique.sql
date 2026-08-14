select
    order_id,
    payment_sequential,
    count(*) as record_count
from {{ ref('fact_payments') }}
group by
    order_id,
    payment_sequential
having count(*) > 1
