-- 5-minute tumbling event-time windows from the stream (deduplicated, on-time events only).
-- Late events are reported separately in stream_late_events count.
with w as (
    select
        time_bucket(interval '5 minutes', event_time) as window_start,
        count(*)                                     as trips,
        round(sum(total_amount), 2)                  as revenue,
        count(distinct pu_location_id)               as active_pickup_zones
    from {{ ref('stg_stream_events') }}
    group by 1
)
select window_start, window_start + interval '5 minutes' as window_end, trips, revenue,
       active_pickup_zones
from w
order by window_start
