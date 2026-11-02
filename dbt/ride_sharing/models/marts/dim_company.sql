select
    license_num as company_key,
    company_name
from {{ ref('hvfhs_companies') }}
