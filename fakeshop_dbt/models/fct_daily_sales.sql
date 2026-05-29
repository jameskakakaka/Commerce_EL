

-- Aggergate the sales by day and category for the dashboard 

{{config(materialized='table')}}

WITH raw_transactions AS (
    SELECT * FROM {{ source('raw_data', 'transactions') }}
)

SELECT 
    transaction_date,
    category,
    COUNT(DISTINCT transaction_id) AS daily_transactions,
    SUM(quantity) AS items_sold,
    SUM(total_amount) AS daily_revenue
FROM raw_transactions 
GROUP BY 1, 2 