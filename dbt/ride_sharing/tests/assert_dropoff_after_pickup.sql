-- A trip must end after it starts.
select trip_id, pickup_datetime, dropoff_datetime
from {{ ref('fct_trips') }}
where dropoff_datetime <= pickup_datetime
