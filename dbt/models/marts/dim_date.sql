with bounds as (
    select min(pickup_date) as lo, max(pickup_date) as hi from {{ ref('fct_trips') }}
),
days as (
    select cast(unnest(generate_series(lo, hi, interval 1 day)) as date) as date_day from bounds
)
select
    date_day,
    year(date_day)                    as year,
    month(date_day)                   as month,
    dayname(date_day)                 as day_name,
    isodow(date_day)                  as iso_day_of_week,
    isodow(date_day) in (6, 7)        as is_weekend
from days
