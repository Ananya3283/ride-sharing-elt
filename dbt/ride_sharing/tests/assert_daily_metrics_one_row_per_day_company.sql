select date_key, company_name, count(*) as n
from {{ ref('mart_daily_company_metrics') }}
group by 1, 2
having count(*) > 1
