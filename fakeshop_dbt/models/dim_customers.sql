

-- Calculating how much each customer has spent and categorize them
{{ config(materialized='table')}}

WITH raw_transactions AS (
    SELECT * FROM {{ source('raw_data', 'transactions') }}
)

SELECT
    user_id,
    customer_name,
    COUNT(DISTINCT transaction_id) AS total_orders,
    SUM(total_amount) AS lifetime_value,
    CASE
        WHEN SUM(total_amount) > 500 THEN 'VIP'
        WHEN SUM(total_amount) > 100 THEN 'Regular'
        ELSE 'Casual'
        END AS customer_segment

FROM raw_transactions
GROUP BY 1, 2