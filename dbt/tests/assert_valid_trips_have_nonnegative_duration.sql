select trip_key from {{ ref('fct_trips') }} where duration_min < 0
