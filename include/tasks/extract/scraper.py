

'''
Output raw CSV to: 
raw/YYYY/MM/raw_data_YYYYMMDD.csv
'''

# Installing the necessary libraries 
import logging
import requests # Send HTTP request to the website or APIs
import pandas as pd # Organize data and save them into tables 
import os # Create folders and manage file pathways 

from pathlib import Path 
from bs4 import BeautifulSoup # Extract and parse data from the website 
from datetime import date, datetime # To use the date of the files 
from typing import Dict, List 

# Setting up the logging to monitor what happens in the terminal
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Find the project roots (3 levels above this file) to set the landing zone of the files 
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
LANDING_DIR = Path(os.getenv("LANDING_DIR", _PROJECT_ROOT /"landing"))

# Set up the return structured landing path for a given file 
def get_output_path(run_date: date) -> Path:
    """
    Return the structured landing path for a given file
    """
    return(
        LANDING_DIR 
        / "raw" 
        # Setting up the date for the output
        / run_date.strftime("%Y")
        / run_date.strftime("%m")
        # CSV format raw data
        / f"raw_data_{run_date.strftime('%Y%m%d')}.csv" 
    )


# Defining a class for the transaction
class FakeStoreTransactionScraper:
    """
    Extract and store the data from the API end points 
    """
    BASE_URL = "https://fakestoreapi.com"

    # Using a session to keep the underlying TCP connection open
    # Making mutiple requests for efficiency
    def __init__(self):
        self._session = requests.Session()

    # Fetching the API endpoints 
    def _fetch_api_endpoint(self, endpoint: str) -> List[Dict]:
        """
        A method to fetch data from API endpoint  
        """
        # Setting the source
        url = f"{self.BASE_URL}/{endpoint}"
        # Monitor the logs
        logger.info(f"Fetching data from{url}")

        # Setting up the extracting from source
        try:
            response = self._session.get(url, timeout=10)
            # Check for errors
            response.raise_for_status() 
            return response.json() # return in json format

        except requests.RequestException as e:
            logger.error(f"Failed to fetch {endpoint}: {e}")
            return 

    # Defining the pipeline
    def extract_transactions(self) -> List[Path]:
        """
        The main pipeline methods, fetches all the data, join them and saves it
        """

        # Fetch the users and create a lookup dictionary: {user_id: "First Last"}
        users_raw = self._fetch_api_endpoint("users")
        user_lookup = {}
        for u in users_raw:
            # Combine the first and last name
            full_name = f"{u['name']['firstname']} {u['name']['lastname']}".title()
            user_lookup[u['id']] = full_name

        # Fetch the products and create a lookup dictionary: {product_id: {"title": X, "price": Y}}
        products_raw = self._fetch_api_endpoint("products")
        product_lookup = {
            p['id']: {"title": p['title'], "price": p['price'], "category": p['category']} 
            for p in products_raw
        }

        # Fetch the base transactions (Carts)
        carts_raw = self._fetch_api_endpoint("carts")

        # Flatten and join the data 
        flat_transactions = []

        for cart in carts_raw:
            cart_id = cart['id']
            cart_date = cart['date']
            user_id = cart['userId']
            user_name = user_lookup.get(user_id, "Unknown User")

        # Loop in case the cart have mutiple products 
            for item in cart['products']:
                product_id = item['productId']
                quantity = item['quantity']

                # Look up product details
                prod_details = product_lookup.get(product_id,{})
                price = prod_details.get('price', 0.0)
                title = prod_details.get('title', 'Unknown Product')
                category = prod_details.get('category', 'Unknown')

                # Calculate the total amount the specific item in the cart 
                total_amount = price * quantity 

                # Build the flat row
                transaction_row = {
                     "transaction_id": f"CART-{cart_id}-PROD-{product_id}", # Unique ID per row
                    "cart_id": cart_id,
                    "transaction_date": cart_date,
                    "user_id": user_id,
                    "customer_name": user_name,
                    "product_id": product_id,
                    "product_title": title,
                    "category": category,
                    "unit_price": price,
                    "quantity": quantity,
                    "total_amount": round(total_amount, 2)
                }
                # Apeend the flat row to file_transactions
                flat_transactions.append(transaction_row)

        # Check for the errors
        if not flat_transactions:
            raise RuntimeError("No transactions generated. Check API connections")

        # Conver to Pandas DataFrame for saving and date partitioning
        df = pd.DataFrame(flat_transactions)

        # Ensure the data column is parsed as a true datetime object
        # We use .str[:10] to grab just the YYYY-MM-DD part of the timestamp
        df["_parsed_date"] = pd.to_datetime(df["transaction_date"].str[:10]).dt.date

        # Group the data by the date and save to S3-like folder structure
        saved_paths = []
        for txn_date, group in df.groupby("_parsed_date"):
            output_path = get_output_path(txn_date)
            # Create the deeply nested folders if they don't exist
            output_path.parent.mkdir(parents=True, exist_ok=True)
            # Drop our temporary date column and save to CSV
            group.drop(columns=["_parsed_date"]).to_csv(output_path, index=False)
            saved_paths.append(output_path)
            logger.info(f"Saved {len(group)} transaction items → {output_path}")

        return saved_paths

# This allows us to test the script by running it directly in the terminal
if __name__ == "__main__":
    logger.info("Starting Fake Store API Extraction...")
    scraper = FakeStoreTransactionScraper()
    written_files = scraper.extract_transactions()
    logger.info(f"Success! {len(written_files)} daily transaction files were written.")







 

