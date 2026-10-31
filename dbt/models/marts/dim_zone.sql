select location_id, borough, zone_name, service_zone,
       zone_name in ('JFK Airport', 'LaGuardia Airport', 'Newark Airport') as is_airport
from {{ ref('stg_zones') }}
