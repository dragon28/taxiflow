-- Grain: one valid trip (one raw TLC row that passed DQ checks).
select
    trip_key,
    source_file,
    cast(pickup_at as date)         as pickup_date,
    hour(pickup_at)                 as pickup_hour,
    pickup_at,
    dropoff_at,
    pu_location_id,
    do_location_id,
    vendor_id,
    payment_type,
    passenger_count,
    trip_miles,
    duration_min,
    fare_amount,
    tip_amount,
    total_amount,
    is_refund_or_dispute
from {{ ref('stg_yellow_trips') }}
where dq_reason is null
