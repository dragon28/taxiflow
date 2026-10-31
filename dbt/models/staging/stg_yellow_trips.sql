-- Typed, renamed, and DQ-classified trips. Nothing is dropped here: every raw row
-- gets dq_reason (NULL = valid). Refunds/disputes are unusual but legitimate -> flagged, kept.
with src as (
    select * from {{ source('raw', 'yellow_trips') }}
),

zones as (
    select location_id from {{ ref('stg_zones') }}
),

typed as (
    select
        source_file || ':' || source_row                    as trip_key,
        source_file,
        source_row,
        -- file name carries the reporting month: yellow_tripdata_YYYY-MM.parquet
        cast(regexp_extract(source_file, '(\d{4}-\d{2})', 1) || '-01' as date) as file_month,
        "VendorID"                                           as vendor_id,
        tpep_pickup_datetime                                 as pickup_at,
        tpep_dropoff_datetime                                as dropoff_at,
        passenger_count,
        trip_distance                                        as trip_miles,
        "RatecodeID"                                         as rate_code_id,
        "PULocationID"                                       as pu_location_id,
        "DOLocationID"                                       as do_location_id,
        payment_type,
        fare_amount,
        tip_amount,
        tolls_amount,
        total_amount,
        coalesce(congestion_surcharge, 0) + coalesce("Airport_fee", 0) as surcharges_amount,
        date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0 as duration_min,
        payment_type in (3, 4) or total_amount < 0           as is_refund_or_dispute,
        loaded_at
    from src
)

select
    t.*,
    case
        when t.pickup_at is null or t.dropoff_at is null              then 'missing_timestamp'
        when date_trunc('month', t.pickup_at) <> t.file_month          then 'pickup_outside_file_month'
        when t.dropoff_at < t.pickup_at                                then 'dropoff_before_pickup'
        when t.duration_min > {{ var('max_trip_hours') }} * 60         then 'duration_too_long'
        when t.trip_miles < 0 or t.trip_miles > {{ var('max_trip_miles') }} then 'implausible_distance'
        when pu.location_id is null or dz.location_id is null          then 'unknown_zone'
        when t.total_amount < 0 and not t.is_refund_or_dispute         then 'negative_amount'
    end as dq_reason
from typed t
left join zones pu on pu.location_id = t.pu_location_id
left join zones dz on dz.location_id = t.do_location_id
