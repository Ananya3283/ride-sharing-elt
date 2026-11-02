select
    location_id as zone_key,
    borough,
    zone_name,
    service_zone,
    zone_name in ('JFK Airport', 'LaGuardia Airport', 'Newark Airport') as is_airport
from {{ ref('stg_taxi_zones') }}
