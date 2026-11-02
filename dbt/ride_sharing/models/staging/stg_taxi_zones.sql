select
    location_id,
    borough,
    zone as zone_name,
    service_zone
from {{ source('raw', 'taxi_zones') }}
