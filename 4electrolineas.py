import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry # Corrected import from prompt
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(filename)s - [%(lineno)d] - %(message)s')

# Global script configurations
DB_FILENAME = 'precios_competencia.db'
TABLE_NAME = 'electrolineas'
REQUEST_TIMEOUT = 15 # Seconds for request timeout

def get_script_directory():
    """Returns the absolute path to the directory where the script is located."""
    return os.path.dirname(os.path.abspath(__file__))

def create_session_with_retries():
    """Creates and returns a requests.Session configured with retries."""
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=0.1, 
        status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    # Default User-Agent can be set here if desired for all session requests
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    })
    return session

def main():
    logging.info("Script started.")
    script_dir = get_script_directory()
    db_path = os.path.join(script_dir, DB_FILENAME)

    conn = None
    all_products_to_insert = [] # Collect all product data here for one final commit

    base_urls = [
        "https://electrolineas.com.ar/categoria-producto/cables-categorias/?v=c838c18b91bc",
        "https://electrolineas.com.ar/categoria-producto/proteccion-electrica-categorias/?v=c838c18b91bc",
        "https://electrolineas.com.ar/categoria-producto/canos-y-accesorios-categorias/?v=c838c18b91bc",
        "https://electrolineas.com.ar/categoria-producto/iluminacion-categorias/?v=c838c18b91bc"
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
            logging.info(f"Processing base URL: {base_url}")
            page_urls_to_scrape = set()
            page_urls_to_scrape.add(base_url)

            try:
                # Initial request to get pagination links from the base_url
                logging.debug(f"Fetching initial page to find pagination: {base_url}")
                response = session.get(base_url, timeout=REQUEST_TIMEOUT)
                response.raise_for_status() 
                soup = BeautifulSoup(response.content, 'html.parser')

                # Try common WooCommerce pagination selectors
                pagination_nav = soup.select_one("nav.woocommerce-pagination")
                if not pagination_nav: # Fallback to a more generic nav if the first fails
                    pagination_nav = soup.select_one(".products-footer nav") # Common in some themes
                
                if pagination_nav:
                    links = pagination_nav.select('a.page-numbers') # WooCommerce specific
                    for link_tag in links:
                        if link_tag and 'href' in link_tag.attrs:
                            page_urls_to_scrape.add(link_tag['href'])
                    logging.info(f"Found {len(page_urls_to_scrape)} unique page URLs (including base) for {base_url} after checking pagination.")
                else:
                    logging.info(f"No pagination navigation found for {base_url} using selectors 'nav.woocommerce-pagination' or '.products-footer nav'. Processing base URL only.")

            except requests.exceptions.RequestException as e:
                logging.error(f"Error fetching initial page or finding pagination for {base_url}: {e}", exc_info=True)
                continue # Skip to the next base_url
            except Exception as e_parse:
                 logging.error(f"Error parsing initial page for pagination {base_url}: {e_parse}", exc_info=True)
                 continue


            for page_url in sorted(list(page_urls_to_scrape)): 
                logging.info(f"Scraping page: {page_url}")
                try:
                    response = session.get(page_url, timeout=REQUEST_TIMEOUT)
                    response.raise_for_status()
                    page_soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Primary selector for product divs
                    product_divs = page_soup.select("div.product-grid-item.product") 
                    if not product_divs:
                        # Fallback selector (often <li> elements in WooCommerce)
                        product_divs = page_soup.select("li.product.type-product")
                        if not product_divs:
                            logging.info(f"No product divs found on {page_url} using primary or fallback selectors.")
                            continue 
                        else:
                             logging.debug(f"Used fallback product selector 'li.product.type-product' for {page_url}.")
                    
                    logging.info(f"Found {len(product_divs)} products on {page_url}.")
                    
                    for product_div in product_divs:
                        fecha_actual = datetime.now().strftime("%d/%m/%Y")
                        product_name = "No disponible"
                        product_price = "No disponible"

                        try:
                            # Try a common product title selector first
                            name_element = product_div.select_one(".wd-entities-title a") 
                            if not name_element: # Fallback to original if not found
                                name_element = product_div.select_one("div.product-element-bottom > h3 > a")
                            
                            if name_element:
                                product_name = name_element.text.strip()
                            else:
                                logging.warning(f"Product name element not found on {page_url}. Title container: {product_div.select_one('h3, .wd-entities-title')}")

                            # Try common price selectors
                            price_selectors = [
                                "span.price ins span.woocommerce-Price-amount.amount bdi", # Sale price
                                "span.price span.woocommerce-Price-amount.amount bdi",   # Regular price
                                "div.wrap-price span.price span.woocommerce-Price-amount.amount bdi", # Wrapped price
                                "div.product-element-bottom div.wrap-price span.price span.woocommerce-Price-amount bdi" # Variation of original
                            ]
                            price_element = None
                            for selector in price_selectors:
                                price_element = product_div.select_one(selector)
                                if price_element:
                                    break
                            
                            if price_element:
                                product_price = price_element.text.strip()
                            else:
                                logging.warning(f"Price element not found for product '{product_name}' on {page_url}. Price container: {product_div.select_one('span.price, div.wrap-price')}")

                            all_products_to_insert.append((fecha_actual, product_name, product_price))
                        
                        except Exception as e_prod_parse:
                            logging.error(f"Error parsing an individual product item on {page_url}: {e_prod_parse}. Product HTML snippet: {str(product_div)[:250]}", exc_info=True)
                            all_products_to_insert.append((fecha_actual, f"Error en parseo - {product_name}", str(e_prod_parse)[:50]))


                except requests.exceptions.RequestException as e_page_req:
                    logging.error(f"Error fetching page {page_url}: {e_page_req}", exc_info=True)
                except Exception as e_page_parse: # Catch other errors like BeautifulSoup parsing issues
                    logging.error(f"Unexpected error processing page {page_url}: {e_page_parse}", exc_info=True)


        # After processing all base_urls and their pages, insert all collected products
        if all_products_to_insert:
            logging.info(f"Preparing to insert {len(all_products_to_insert)} products in total.")
            try:
                c.executemany(f"INSERT INTO {TABLE_NAME} (Fecha, Descripcion, Precio) VALUES (?, ?, ?)", all_products_to_insert)
                logging.info(f"Successfully executed insert for {len(all_products_to_insert)} products.")
                # Single commit after all insertions are prepared
            except sqlite3.Error as e_db_insert:
                logging.error(f"Database error during executemany: {e_db_insert}", exc_info=True)
                if conn:
                    conn.rollback() # Rollback if executemany fails
                    logging.info("Rolled back database changes due to error during bulk insert.")
        else:
            logging.info("No products collected from any URL to insert.")

        # Commit here, after all processing and potential executemany, if no errors before this point caused a rollback
        if conn: # Check if conn is still valid (not None)
            # Check if a transaction is active, which might not be the case if an error occurred before executemany
            # or if executemany itself failed and was rolled back.
            # A simple conn.commit() is usually fine here as SQLite handles nested transactions or commits outside transactions gracefully.
             conn.commit()
             logging.info("All data successfully processed and committed to the database.")


    except sqlite3.Error as e_db_main: 
        logging.error(f"Main database operation error: {e_db_main}", exc_info=True)
        if conn:
            conn.rollback()
    except requests.exceptions.RequestException as e_req_main: # Catch if session creation or other high-level request fails
        logging.error(f"Main requests library error: {e_req_main}", exc_info=True)
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
    
    logging.info("Script finished.")

if __name__ == "__main__":
    main()
```
