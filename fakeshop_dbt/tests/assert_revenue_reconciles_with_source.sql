-- Both marts must account for exactly the revenue in the raw layer.
with source_total as (
    select round(sum(total_amount), 2) as revenue from {{ source('raw_data', 'transactions') }}
),
fact_total as (
    select round(sum(daily_revenue), 2) as revenue from {{ ref('fct_daily_sales') }}
),
dim_total as (
    select round(sum(lifetime_value), 2) as revenue from {{ ref('dim_customers') }}
)
select *
from source_total, fact_total, dim_total
where abs(source_total.revenue - fact_total.revenue) > 0.01
   or abs(source_total.revenue - dim_total.revenue) > 0.01
