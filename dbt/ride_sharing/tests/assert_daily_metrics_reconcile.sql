-- The daily mart must add up to the fact table: no trips lost or double-counted in the joins.
select
    (select sum(trips) from {{ ref('mart_daily_company_metrics') }}) as mart_trips,
    (select count(*) from {{ ref('fct_trips') }}) as fct_trips
where (select sum(trips) from {{ ref('mart_daily_company_metrics') }})
    is distinct from (select count(*) from {{ ref('fct_trips') }})
