

-- Setting up the foundational for the tables of Transaction  
-- Use the correct database and schemas   

-- Create the tables  
CREATE OR REPLACE TABLE transactions (
    -- Primary key and identifiers   
    transaction_id VARCHAR(255) PRIMARY KEY,
    cart_id INT, 

    -- Time and Customer Data 
    transaction_date DATE,
    user_id INT,
    customer_name VARCHAR(255),

    -- Product Details
    product_id INT,                           -- The specific item ID
    product_title VARCHAR(500),               -- The name of the item
    category VARCHAR(255),                    -- The uppercase category (e.g., ELECTRONICS)
    
    -- Financial Metrics
    unit_price FLOAT,                         -- Price of a single item
    quantity INT,                             -- How many they bought
    total_amount FLOAT,                       -- unit_price * quantity
    sale_tier VARCHAR(50),                    -- 'High Value' or 'Standard'
    
    -- Metadata
    updated_at TIMESTAMP_NTZ
)