-- Grain: one row per day per company. Ready for a BI dashboard.
select
    d.date_key,
    d.date_day,
    d.is_weekend,
    c.company_name,
    count(*) as trips,
    round(sum(f.trip_miles)::numeric, 1) as total_miles,
    round(avg(f.trip_miles)::numeric, 2) as avg_trip_miles,
    round(avg(f.wait_seconds)::numeric, 0) as avg_wait_seconds,
    round(sum(f.rider_fare_before_tips)::numeric, 2) as rider_fare_total,
    round(sum(f.tips)::numeric, 2) as tips_total,
    round(sum(f.driver_pay)::numeric, 2) as driver_pay_total,
    round(
        (sum(f.driver_pay) / nullif(sum(f.base_passenger_fare), 0))::numeric, 4
    ) as driver_pay_share_of_base_fare
from {{ ref('fct_trips') }} as f
inner join {{ ref('dim_date') }} as d on f.pickup_date_key = d.date_key
inner join {{ ref('dim_company') }} as c on f.company_key = c.company_key
group by 1, 2, 3, 4
