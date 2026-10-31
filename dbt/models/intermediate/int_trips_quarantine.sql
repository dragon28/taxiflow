-- Rows excluded from marts, with the reason. Inspect, don't silently drop.
select trip_key, source_file, source_row, dq_reason, pickup_at, dropoff_at,
       pu_location_id, do_location_id, trip_miles, total_amount, loaded_at
from {{ ref('stg_yellow_trips') }}
where dq_reason is not null
