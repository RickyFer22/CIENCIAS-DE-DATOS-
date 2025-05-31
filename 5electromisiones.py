import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry # Corrected import from 'requests.packages.urllib3.util.retry'
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(filename)s - [%(lineno)d] - %(message)s')

# Global script configurations
DB_FILENAME = 'precios_competencia.db'
TABLE_NAME = 'electromisiones'
REQUEST_TIMEOUT = 15 # Seconds for request timeout

def get_script_directory():
    """Returns the absolute path to the directory where the script is located."""
    return os.path.dirname(os.path.abspath(__file__))

def create_session_with_retries():
    """Creates and returns a requests.Session configured with retries."""
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=0.3, 
        status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    # Default User-Agent for the session
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    })
    return session

def main():
    logging.info("Script started: 5electromisiones.py")
    script_dir = get_script_directory()
    db_path = os.path.join(script_dir, DB_FILENAME)

    conn = None
    all_products_to_insert = [] # Collect all product data

    base_urls = [
        "https://www.electromisiones.com.ar/3-materiales_electricos",
        "https://www.electromisiones.com.ar/4-iluminacion?order=product.date_add.desc",
        "https://www.electromisiones.com.ar/11-cables",
        "https://www.electromisiones.com.ar/471-cajas?categoria=derivacion,tapas",
        "https://www.electromisiones.com.ar/34-termicas_y_disyuntores"
    ]
    
    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        logging.info(f"Successfully connected to database: {db_path}")

        c.execute(f'''CREATE TABLE IF NOT EXISTS {TABLE_NAME}
                        (Fecha TEXT, Descripcion TEXT, Precio TEXT)''')
        conn.commit() # Commit DDL (table creation)
        logging.info(f"Table '{TABLE_NAME}' ensured to exist or already exists.")

        session = create_session_with_retries()

        for base_url in base_urls:
            logging.info(f"Processing URL: {base_url}")
            try:
                # Headers are now set on the session object in create_session_with_retries()
                response = session.get(base_url, timeout=REQUEST_TIMEOUT) 
                response.raise_for_status()

                soup = BeautifulSoup(response.content, 'html.parser')
                
                product_divs = soup.select("#category-description article, #js-product-list article")

                if not product_divs:
                    logging.info(f"No products found on: {base_url} with selector '#category-description article, #js-product-list article'")
                    continue

                logging.info(f"Found {len(product_divs)} products on {base_url}.")

                for product_div in product_divs:
                    fecha_actual = datetime.now().strftime("%d/%m/%Y")
                    product_name = "No disponible"
                    product_price = "No disponible"

                    try:
                        # More robust product name extraction
                        name_element = product_div.select_one("h3.product-title a, h6.product-title a, .product-title a, .product_title a")
                        if name_element:
                            product_name = name_element.text.strip()
                        else:
                            logging.warning(f"Product name not found for an item on {base_url}. Item HTML snippet: {str(product_div)[:150]}")

                        # More robust product price extraction
                        price_element = product_div.select_one("span.price, div.tv-product-price span, .product-price-and-shipping .price, .product_price .price")
                        if price_element:
                            # Iterate through potential price texts if multiple spans are nested
                            price_text_parts = [s.strip() for s in price_element.find_all(string=True, recursive=True) if s.strip()]
                            product_price = ' '.join(price_text_parts) if price_text_parts else "No disponible"
                        else:
                            logging.warning(f"Product price not found for '{product_name}' on {base_url}. Item HTML snippet: {str(product_div)[:150]}")
                        
                        all_products_to_insert.append((fecha_actual, product_name, product_price))

                    except Exception as e_item:
                        logging.error(f"Error parsing a product item on {base_url}: {e_item}. Product div snippet: {str(product_div)[:200]}", exc_info=True)
                        all_products_to_insert.append((fecha_actual, f"Error en parseo - {product_name}", str(e_item)[:50]))


            except requests.exceptions.RequestException as e_req:
                logging.error(f"Request error accessing {base_url}: {e_req}", exc_info=True)
            except Exception as e_url: 
                logging.error(f"Unexpected error processing {base_url}: {e_url}", exc_info=True)
        
        if all_products_to_insert:
            logging.info(f"Preparing to insert {len(all_products_to_insert)} products in total.")
            try:
                c.executemany(f"INSERT INTO {TABLE_NAME} (Fecha, Descripcion, Precio) VALUES (?, ?, ?)", all_products_to_insert)
                logging.info(f"Successfully executed insert for {len(all_products_to_insert)} products.")
                conn.commit() 
                logging.info("All data committed to the database.")
            except sqlite3.Error as e_db_insert:
                logging.error(f"Database error during executemany or commit: {e_db_insert}", exc_info=True)
                if conn:
                    conn.rollback()
                    logging.info("Rolled back database changes due to error during bulk insert/commit.")
        else:
            logging.info("No products collected from any URL to insert.")

    except sqlite3.Error as e_db_main:
        logging.error(f"Main database operation error: {e_db_main}", exc_info=True)
        if conn: 
            conn.rollback()
    except Exception as e_main:
        logging.error(f"An unexpected critical error occurred in main execution: {e_main}", exc_info=True)
        if conn:
            try:
                conn.rollback()
                logging.info("Rolled back database changes due to unexpected critical error.")
            except sqlite3.Error as rb_err:
                logging.error(f"Error during rollback attempt: {rb_err}", exc_info=True)
    finally:
        if conn:
            try:
                conn.close()
                logging.info("Database connection closed.")
            except sqlite3.Error as e_db_close:
                logging.error(f"Error while closing database connection: {e_db_close}", exc_info=True)
    
    logging.info("Script finished: 5electromisiones.py")

if __name__ == "__main__":
    main()
```
