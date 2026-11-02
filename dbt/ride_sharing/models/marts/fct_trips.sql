-- Grain: one row per trip.
-- Incremental by month: delete+insert on source_month replaces every month present in the
-- new batch, which matches how load.py replaces a month in the raw layer.
{{
    config(
        materialized='incremental',
        unique_key='source_month',
        incremental_strategy='delete+insert',
        indexes=[
            {'columns': ['pickup_date_key']},
            {'columns': ['company_key']},
        ],
    )
}}

with trips as (

    select * from {{ ref('stg_fhvhv_trips') }}
    {% if is_incremental() %}
    where loaded_at > (select coalesce(max(loaded_at), '1900-01-01') from {{ this }})
    {% endif %}

)

select
    trip_id,
    hvfhs_license_num as company_key,
    pickup_location_id as pickup_zone_key,
    dropoff_location_id as dropoff_zone_key,
    to_char(pickup_date, 'YYYYMMDD')::integer as pickup_date_key,
    extract(hour from pickup_datetime)::integer as pickup_hour,
    request_datetime,
    pickup_datetime,
    dropoff_datetime,
    trip_miles,
    trip_seconds,
    wait_seconds,
    base_passenger_fare,
    rider_fare_before_tips,
    tips,
    driver_pay,
    is_shared_request,
    is_shared_match,
    is_wav_request,
    is_wav_match,
    source_month,
    loaded_at
from trips
