-- Grain: pickup zone x pickup date x hour. Refunds excluded from revenue averages.
select
    f.pickup_date,
    f.pickup_hour,
    f.pu_location_id,
    z.borough,
    z.zone_name,
    count(*)                                                            as trips,
    round(avg(f.trip_miles) filter (where not f.is_refund_or_dispute), 2)    as avg_miles,
    round(avg(f.duration_min) filter (where not f.is_refund_or_dispute), 1)  as avg_duration_min,
    round(sum(f.total_amount) filter (where not f.is_refund_or_dispute), 2)  as revenue,
    round(avg(f.tip_amount / nullif(f.fare_amount, 0))
          filter (where f.payment_type = 1 and f.fare_amount > 0), 3)   as avg_card_tip_ratio
from {{ ref('fct_trips') }} f
join {{ ref('dim_zone') }} z on z.location_id = f.pu_location_id
group by all
