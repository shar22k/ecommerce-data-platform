with date_spine as (

    select
        dateadd(
            day,
            seq4(),
            '2016-01-01'::date
        ) as date_day

    from table(generator(rowcount => 3653))

),

final as (

    select
        to_number(to_char(date_day, 'YYYYMMDD')) as date_key,

        date_day,

        year(date_day) as year,
        quarter(date_day) as quarter,
        month(date_day) as month,
        monthname(date_day) as month_name,

        weekofyear(date_day) as week_of_year,

        day(date_day) as day_of_month,
        dayofweekiso(date_day) as day_of_week,
        dayname(date_day) as day_name,

        case
            when dayofweekiso(date_day) in (6, 7)
                then true
            else false
        end as is_weekend

    from date_spine

)

select *
from final
