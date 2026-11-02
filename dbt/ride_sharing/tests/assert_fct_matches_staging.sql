-- The incremental fact table must hold exactly the trips in staging, month by month.
with stg as (
    select source_month, count(*) as n from {{ ref('stg_fhvhv_trips') }} group by 1
),

fct as (
    select source_month, count(*) as n from {{ ref('fct_trips') }} group by 1
)

select coalesce(stg.source_month, fct.source_month) as source_month, stg.n as stg_rows, fct.n as fct_rows
from stg
full outer join fct on stg.source_month = fct.source_month
where stg.n is distinct from fct.n
