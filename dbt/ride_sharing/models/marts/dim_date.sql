-- One row per calendar day covered by the trips loaded so far.
with bounds as (

    select
        min(pickup_date) as first_day,
        max(pickup_date) as last_day
    from {{ ref('stg_fhvhv_trips') }}

),

days as (

    select generate_series(first_day, last_day, interval '1 day')::date as date_day
    from bounds

)

select
    to_char(date_day, 'YYYYMMDD')::integer as date_key,
    date_day,
    extract(year from date_day)::integer as year,
    extract(quarter from date_day)::integer as quarter,
    extract(month from date_day)::integer as month,
    trim(to_char(date_day, 'Month')) as month_name,
    extract(isodow from date_day)::integer as day_of_week,
    trim(to_char(date_day, 'Day')) as day_name,
    extract(isodow from date_day) in (6, 7) as is_weekend
from days
