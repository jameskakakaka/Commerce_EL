

"""
DuckDB Data Loader 
Reads processed CSV files and load them into a local DuckDB database
"""

# Importing the necessary libraries 
import logging 
import os 
import duckdb
from datetime import datetime 
from pathlib import Path 

# Setting up the log
logger = logging.getLogger(__name__)

# Setting up the project root for duckDB 
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
LANDING_DIR = Path(os.getenv("LANDING_DIR", _PROJECT_ROOT / "landing"))

# Setting up the new Datawarehouse
DB_PATH = _PROJECT_ROOT / "fakeshop_warehouse.db"

# Setting up the loading data method 
def load_processed_data(date_str:str) -> str:
    """
    Loads a specific day's processed CSV into the DuckDB 
    """
    #Setting up the date of run format and the path to load CSV
    run_date = datetime.strptime(date_str, "%Y%m%d").date()
    csv_path = (
        LANDING_DIR
        / "processed"
        / run_date.strftime("%Y")
        / run_date.strftime("%m")
        / f"clean_data_{run_date.strftime('%Y%m%d')}.csv"
    )

    if not csv_path.exists():
        logger.warning(f"No processed file found for {date_str} at {csv_path}")
        return "Skipped - No File"

    # Connecting to the duckDB
    conn = duckdb.connect(str(DB_PATH))

    # Ensuring the table exists
    try: 
        conn.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id VARCHAR,
                cart_id INTEGER,
                transaction_date DATE,
                user_id INTEGER,
                customer_name VARCHAR,
                product_id INTEGER,
                product_title VARCHAR,
                category VARCHAR,
                unit_price DOUBLE,
                quantity INTEGER,
                total_amount DOUBLE,
                sale_tier VARCHAR,
                updated_at TIMESTAMP
            )
        """)

        # Make it idempotent: Delete any existing data for this date so we don't duplicate
        # Extract the date from the CSV filename to know what to delete
        conn.execute(f"DELETE FROM transactions WHERE CAST(transaction_date AS VARCHAR) LIKE '{run_date}%'")

        # Load the new data directly from the CSV 
        conn.execute(f"""
            INSERT INTO transactions 
            SELECT * FROM read_csv_auto('{str(csv_path)}')
        """)
    
        # Get a count of rows just loaded for our logs
        result = conn.execute(f"SELECT COUNT(*) FROM transactions WHERE CAST(transaction_date AS VARCHAR) LIKE '{run_date}%'").fetchone()
        logger.info(f"Successfully loaded {result[0]} rows into DuckDB for {date_str}")
        
    finally:
        # Always close the connection
        conn.close()

    return f"Loaded {date_str}"