-- Fails if any (transaction_date, category) pair appears more than once.
select transaction_date, category, count(*) as n
from {{ ref('fct_daily_sales') }}
group by 1, 2
having count(*) > 1
