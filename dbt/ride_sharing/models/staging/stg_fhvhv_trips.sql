with source as (

    select * from {{ source('raw', 'fhvhv_trips') }}

)

select
    trip_id,
    hvfhs_license_num,
    dispatching_base_num,
    request_datetime,
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    trip_miles,
    trip_seconds,
    case
        when request_datetime is not null and request_datetime <= pickup_datetime
            then extract(epoch from pickup_datetime - request_datetime)::integer
    end as wait_seconds,
    base_passenger_fare,
    tolls,
    black_car_fund,
    sales_tax,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    base_passenger_fare + tolls + black_car_fund + sales_tax
        + congestion_surcharge + airport_fee + cbd_congestion_fee as rider_fare_before_tips,
    tips,
    driver_pay,
    is_shared_request,
    is_shared_match,
    is_wav_request,
    is_wav_match,
    pickup_date,
    source_month,
    _loaded_at as loaded_at
from source
