select *
from {{ ref('fact_payments') }}
where payment_value < 0
