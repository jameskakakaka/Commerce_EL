-- Grain: one row per (transaction_date, category).

with transactions as (
    select * from {{ source('raw_data', 'transactions') }}
)

select
    transaction_date,
    category,
    count(distinct cart_id)         as daily_orders,
    count(*)                        as daily_line_items,
    sum(quantity)                   as items_sold,
    round(sum(total_amount), 2)     as daily_revenue
from transactions
group by transaction_date, category
