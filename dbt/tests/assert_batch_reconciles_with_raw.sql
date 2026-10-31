-- Every raw row must land in exactly one of: fct_trips or the quarantine. Returns failures.
select 'raw' as side, count(*) as n from {{ source('raw', 'yellow_trips') }}
having count(*) <> (select count(*) from {{ ref('fct_trips') }})
                   + (select count(*) from {{ ref('int_trips_quarantine') }})
