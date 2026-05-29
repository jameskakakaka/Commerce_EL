# Database + Schema 
resource "snowflake_database" "fakeshop" {
  name = "FAKESHOP_DB"
}

resource "snowflake_schema" "raw" {
  database = snowflake_database.fakeshop.name
  name     = "RAW_DATA"
}

# Compute the Warehouse 
resource "snowflake_warehouse" "fakeshop_wh" {
  name           = "FAKESHOP_WH"
  warehouse_size = "X-SMALL"
  auto_suspend   = 60 
  auto_resume    = "true"
}

# The table 
resource "snowflake_table" "transactions" {
  database = snowflake_database.fakeshop.name
  schema   = snowflake_schema.raw.name
  name     = "FAKESHOP_TRANSACTIONS"

  column {
    name = "TRANSACTION_ID"
    type = "VARCHAR(255)"
  }
  column {
    name = "CART_ID"
    type = "NUMBER(38,0)"
  }
  column {
    name = "TRANSACTION_DATE"
    type = "DATE"
  }
  column {
    name = "USER_ID"
    type = "NUMBER(38,0)"
  }
  column {
    name = "CUSTOMER_NAME"
    type = "VARCHAR(255)"
  }
  column {
    name = "PRODUCT_ID"
    type = "NUMBER(38,0)"
  }
  column {
    name = "PRODUCT_TITLE"
    type = "VARCHAR(500)"
  }
  column {
    name = "CATEGORY"
    type = "VARCHAR(255)"
  }
  column {
    name = "UNIT_PRICE"
    type = "FLOAT"
  }
  column {
    name = "QUANTITY"
    type = "NUMBER(38,0)"
  }
  column {
    name = "TOTAL_AMOUNT"
    type = "FLOAT"
  }
  column {
    name = "SALE_TIER"
    type = "VARCHAR(50)"
  }
  column {
    name = "UPDATED_AT"
    type = "TIMESTAMP_NTZ"
  }
}