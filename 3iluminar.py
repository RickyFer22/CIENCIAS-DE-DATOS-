import sqlite3
import time
from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
# For other browsers, you might need:
# from selenium.webdriver.chrome.options import Options as ChromeOptions
# from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, TimeoutException
from bs4 import BeautifulSoup
from datetime import datetime
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(filename)s - [%(lineno)d] - %(message)s')

# Global script configurations
DB_FILENAME = 'precios_competencia.db'
TABLE_NAME = 'iluminar'
MAX_PAGE_CHECK = 100  # Max number of additional pages to check for each base_url

base_urls = [
    "https://iluminar.ar/product-category/electricidad/",
    "https://iluminar.ar/product-category/iluminacion/"
]

def get_script_directory():
    """Returns the absolute path to the directory where the script is located."""
    return os.path.dirname(os.path.abspath(__file__))

def init_webdriver():
    """Initializes and returns a Selenium WebDriver instance."""
    options = FirefoxOptions() # Adjust for other browsers if needed
    options.headless = True
    # options.add_argument("--window-size=1920,1200") # Optional: Can help with some page layouts
    logging.info("Initializing WebDriver.")
    # If geckodriver (for Firefox) or chromedriver is not in PATH, specify executable_path:
    # from selenium.webdriver.firefox.service import Service as FirefoxService
    # driver = webdriver.Firefox(service=FirefoxService(executable_path='/path/to/geckodriver'), options=options)
    driver = webdriver.Firefox(options=options)
    return driver

def process_products_from_soup(soup, url_processed=""):
    """Extracts product data from a BeautifulSoup object. Returns a list of tuples."""
    products_found = []
    # Using a more specific selector if possible, or the one provided if it's accurate.
    product_divs = soup.select("div.product-small.col.has-hover.product")

    if not product_divs:
        logging.info(f"No product divs found on {url_processed} using selector 'div.product-small.col.has-hover.product'.")
        return products_found

    logging.info(f"Found {len(product_divs)} product entries on {url_processed}.")
    for product_div in product_divs:
        fecha_actual = datetime.now().strftime("%d/%m/%Y")
        product_name = "No disponible"
        product_price = "No disponible"

        try:
            name_element = product_div.select_one("p.name.product-title.woocommerce-loop-product__title > a")
            if name_element:
                product_name = name_element.text.strip()
            else:
                logging.warning(f"Product name element not found for an item on {url_processed}. Product div: {product_div.select_one('p.name.product-title.woocommerce-loop-product__title')}")


            # Enhanced price selectors - WooCommerce can have various structures
            price_selectors = [
                "div.price-wrapper span.price ins span.woocommerce-Price-amount.amount bdi", # Sale price
                "div.price-wrapper span.price span.woocommerce-Price-amount.amount bdi",   # Regular price
                "div.price-wrapper span.price bdi", # Simpler structure
                "div.price-wrapper bdi", # Even simpler
                "span.woocommerce-Price-amount.amount bdi" # More generic
            ]
            price_element = None
            for selector in price_selectors:
                price_element = product_div.select_one(selector)
                if price_element:
                    break 
            
            if price_element:
                product_price = price_element.text.strip()
            else:
                logging.warning(f"Price element not found for product: '{product_name}' on {url_processed} using selectors: {price_selectors}. Price wrapper HTML: {product_div.select_one('div.price-wrapper')}")
            
            products_found.append((fecha_actual, product_name, product_price))
        except Exception as e:
            logging.error(f"Error parsing a product item on {url_processed}: {e}. Product HTML snippet: {product_div.prettify()[:500]}", exc_info=True)
            # Append with error information for traceability in DB
            products_found.append((fecha_actual, f"Error en parseo - {product_name}", str(e)[:50])) 
            
    return products_found

def scroll_page_fully(driver_instance, url_processed=""):
    """Scrolls the page to ensure all dynamic content is loaded."""
    logging.info(f"Scrolling page: {url_processed}")
    last_height = driver_instance.execute_script("return document.body.scrollHeight")
    scroll_attempts = 0
    consecutive_no_change = 0 # Counter for consecutive times scroll height hasn't changed

    while scroll_attempts < 15: # Max attempts to prevent infinite loops on problematic pages
        driver_instance.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        # Wait for scroll to take effect and potential JS loading to start
        time.sleep(0.8) # Increased slightly
        try:
            # Wait for document to be complete, and for a brief period afterwards for JS
            WebDriverWait(driver_instance, 15).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            time.sleep(2.5) # Increased sleep for complex JS rendering
        except TimeoutException:
            logging.warning(f"Timeout waiting for page {url_processed} to reach readyState 'complete' after scroll.")
        
        new_height = driver_instance.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            consecutive_no_change += 1
            if consecutive_no_change >= 2: # If height is same for 2 checks, assume end
                 logging.info(f"Scroll height stabilized at {new_height} for {url_processed} after {scroll_attempts + 1} attempts. Reached end of page or no new content loaded.")
                 break
        else:
            consecutive_no_change = 0 # Reset if height changed
        
        last_height = new_height
        scroll_attempts += 1
        logging.info(f"Scrolled down page {url_processed}, new height: {new_height}, attempt: {scroll_attempts + 1}")
    
    if scroll_attempts >= 15:
        logging.warning(f"Exceeded {scroll_attempts} scroll attempts for {url_processed}. Assuming end of page or scroll issue.")


def main():
    logging.info("Script started.")
    script_dir = get_script_directory()
    db_path = os.path.join(script_dir, DB_FILENAME)

    conn = None
    driver = None
    all_products_globally = [] # Collect all products from all base_urls for one final commit

    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        logging.info(f"Successfully connected to database: {db_path}")

        c.execute(f'''CREATE TABLE IF NOT EXISTS {TABLE_NAME}
                        (Fecha TEXT, Descripcion TEXT, Precio TEXT)''')
        conn.commit() # Commit DDL (table creation)
        logging.info(f"Table '{TABLE_NAME}' ensured to exist or already exists.")

        driver = init_webdriver()

        for base_url in base_urls:
            logging.info(f"Processing base URL: {base_url}")
            current_url_products = [] # Products for the current base_url and its pages
            
            # Process page 1 (the base_url itself)
            try:
                driver.get(base_url)
                # Wait for a main container or body to be present
                WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, "body"))) 
                scroll_page_fully(driver, base_url) # Scroll for dynamic content
                
                html = driver.page_source
                soup = BeautifulSoup(html, 'html.parser')

                if soup.find(string="¡Ups! No pudimos encontrar esa página."):
                    logging.warning(f"Page not found (error message on page) for base URL: {base_url}")
                    continue # Skip to next base_url
                
                products_from_page = process_products_from_soup(soup, base_url)
                if products_from_page:
                    current_url_products.extend(products_from_page)
                    logging.info(f"Collected {len(products_from_page)} products from {base_url}")

            except WebDriverException as e:
                logging.error(f"WebDriverException for {base_url}: {e}", exc_info=True)
                continue 
            except Exception as e: # Catch other unexpected errors for the base URL
                logging.error(f"Unexpected error processing {base_url}: {e}", exc_info=True)
                continue 

            # Process additional pages for the current base_url
            for page_num in range(2, MAX_PAGE_CHECK + 1):
                page_url = f"{base_url}page/{page_num}/"
                logging.info(f"Attempting to process additional page: {page_url}")
                try:
                    driver.get(page_url)
                    # Wait for a key element that indicates page content is loaded
                    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.shop-container"))) # Example selector
                    time.sleep(1.5) # Brief pause for any final JS rendering

                    html = driver.page_source
                    soup = BeautifulSoup(html, 'html.parser')

                    if soup.find(string="¡Ups! No pudimos encontrar esa página."):
                        logging.info(f"Page not found (error message on page) for {page_url}. Assuming no more pages for {base_url}.")
                        break # Stop trying more pages for this base_url

                    products_from_page = process_products_from_soup(soup, page_url)
                    
                    if not products_from_page:
                        # Check for WooCommerce's "No products were found matching your selection."
                        no_product_notice = soup.select_one("p.woocommerce-info") # Common selector for notices
                        if no_product_notice and ("No se encontraron productos" in no_product_notice.text or "No products were found" in no_product_notice.text):
                            logging.info(f"Explicit 'No products found' message on {page_url}. Stopping pagination for {base_url}.")
                            break
                        # Heuristic: if no products and no explicit message, after a few pages, assume it's the end.
                        if page_num > 5: 
                            logging.info(f"No products found on {page_url} (page {page_num}) and no explicit 'no products' message. Assuming end of products for {base_url}.")
                            break
                    
                    if products_from_page:
                        current_url_products.extend(products_from_page)
                        logging.info(f"Collected {len(products_from_page)} products from {page_url}")
                    # If products_from_page is empty, loop continues unless a break condition above was met.

                except TimeoutException:
                    logging.warning(f"TimeoutException on {page_url}. This might indicate an empty or non-existent page after several attempts. Breaking pagination for {base_url}.")
                    break # Often timeouts on non-existent pages or pages that fail to load properly
                except WebDriverException as e:
                    # More specific network error checks could be added here if needed
                    logging.warning(f"WebDriverException for {page_url}: {e}. Breaking pagination for {base_url}.")
                    break # Stop pagination for this base_url on other WebDriver errors
                except Exception as e: # Catch other unexpected errors for additional pages
                    logging.error(f"Unexpected error processing {page_url}: {e}", exc_info=True)
                    logging.warning(f"Breaking pagination for {base_url} due to unexpected error.")
                    break
            
            if current_url_products:
                all_products_globally.extend(current_url_products)
                logging.info(f"Finished processing {base_url}. Total products collected from this base URL: {len(current_url_products)}.")
            else:
                logging.info(f"No products collected for base_url: {base_url}")

        # After processing all base_urls and their pages, insert all collected products
        if all_products_globally:
            logging.info(f"Preparing to insert {len(all_products_globally)} products in total from all processed URLs.")
            try:
                c.executemany(f"INSERT INTO {TABLE_NAME} (Fecha, Descripcion, Precio) VALUES (?, ?, ?)", all_products_globally)
                logging.info(f"Successfully executed insert for {len(all_products_globally)} products.")
                conn.commit() # Single commit for all data
                logging.info("All data committed to the database.")
            except sqlite3.Error as e:
                logging.error(f"Database error during executemany or commit: {e}", exc_info=True)
                if conn: # conn should exist here
                    conn.rollback()
                    logging.info("Rolled back database changes due to error during bulk insert/commit.")
        else:
            logging.info("No products collected from any URL to insert.")

    except sqlite3.Error as e: # Catch DB connection or initial setup errors
        logging.error(f"Database connection or initial setup error: {e}", exc_info=True)
    except WebDriverException as e: # Catch WebDriver initialization errors
        logging.error(f"Fatal WebDriver error during initialization: {e}", exc_info=True)
    except Exception as e: # Catch any other critical errors in main try block
        logging.error(f"An unexpected critical error occurred in main: {e}", exc_info=True)
        if conn: # If conn exists and an error happened before commit, rollback.
            try: # This try-except is for the rollback itself
                conn.rollback()
                logging.info("Rolled back database changes due to unexpected critical error in main.")
            except sqlite3.Error as rb_err:
                logging.error(f"Error during rollback attempt: {rb_err}", exc_info=True)

    finally:
        if driver:
            try:
                driver.quit()
                logging.info("WebDriver closed.")
            except WebDriverException as e:
                logging.error(f"Error while quitting WebDriver: {e}", exc_info=True)
        if conn:
            try:
                conn.close()
                logging.info("Database connection closed.")
            except sqlite3.Error as e:
                logging.error(f"Error while closing database connection: {e}", exc_info=True)
    
    logging.info("Script finished.")

if __name__ == "__main__":
    main()
```
