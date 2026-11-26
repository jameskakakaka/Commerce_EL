-- One row per customer: order history, lifetime value and a value segment.

with transactions as (
    select * from {{ source('raw_data', 'transactions') }}
),

per_customer as (
    select
        user_id,
        max(customer_name)             as customer_name,
        count(distinct cart_id)        as total_orders,
        count(*)                       as total_line_items,
        sum(quantity)                  as total_items,
        round(sum(total_amount), 2)    as lifetime_value,
        min(transaction_date)          as first_order_date,
        max(transaction_date)          as last_order_date
    from transactions
    group by user_id
)

select
    *,
    case
        when lifetime_value > 500 then 'VIP'
        when lifetime_value > 100 then 'Regular'
        else 'Casual'
    end as customer_segment
from per_customer
