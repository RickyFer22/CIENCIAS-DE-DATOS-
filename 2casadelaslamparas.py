import time
from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException, TimeoutException
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(filename)s - %(message)s')

# Obtener la ruta del directorio del script actual
script_dir = os.path.dirname(os.path.abspath(__file__))

# Crear la conexión con la base de datos SQLite
db_filename = 'precios_competencia.db'
db_path = os.path.join(script_dir, db_filename)

urls = [
    "https://www.lacasadelaslamparas.com.ar/productos"
]

# Configurar Selenium con Firefox
options = FirefoxOptions()
options.headless = True  # Para ejecutar en segundo plano sin abrir el navegador
driver = None # Initialize driver to None for finally block
conn = None   # Initialize conn to None for finally block

logging.info("Script started.")

try:
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    logging.info(f"Successfully connected to database: {db_path}")

    # Crear una tabla para almacenar los datos
    tabla_nombre = 'lacasadelaslamparas'
    c.execute(f'''CREATE TABLE IF NOT EXISTS {tabla_nombre}
                    (Fecha TEXT, Descripcion TEXT, Precio TEXT)''')
    conn.commit() # Commit DDL (table creation)
    logging.info(f"Table '{tabla_nombre}' ensured to exist.")

    driver = webdriver.Firefox(options=options)
    logging.info("WebDriver initialized.")

    for url in urls:
        logging.info(f"Processing URL: {url}")
        try:
            driver.get(url)
        except WebDriverException as e:
            logging.error(f"Error loading URL {url}: {e}")
            continue # Skip to next URL

        last_height = driver.execute_script("return document.body.scrollHeight")

        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.5) # Allow JS to start reacting to scroll
            try:
                WebDriverWait(driver, 10).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                # Optional: Add a small additional fixed wait if content still loads slowly
                time.sleep(1.5) # Wait for potential dynamic content loading after readyState is complete
            except TimeoutException:
                logging.warning(f"Timeout waiting for page to load completely for {url} after scroll.")

            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                logging.info("Reached the end of the page (scroll height did not change).")
                break
            last_height = new_height
            logging.info("Scrolled down, new page height: %s", new_height)


        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        product_divs = soup.find_all("div", class_="item-description py-4 px-1")
        logging.info(f"Found {len(product_divs)} product entries on {url}.")

        products_to_insert = []
        for product_div in product_divs:
            try:
                fecha_actual = datetime.now().strftime("%d/%m/%Y")
                product_name = "No disponible"
                product_price = "No disponible"

                link_tag = product_div.find("a")
                if link_tag:
                    product_name_div = link_tag.find("div", class_="js-item-name item-name mb-3")
                    if product_name_div:
                        product_name = product_name_div.text.strip()
                    else:
                        logging.warning(f"Product name div not found for an item in {url}.")

                    price_container_outer = link_tag.find("div", class_="item-price-container mb-1")
                    if price_container_outer:
                        price_container_inner = price_container_outer.find("span", class_="js-price-display item-price")
                        if price_container_inner:
                            product_price = price_container_inner.text.strip()
                        else:
                            logging.warning(f"Inner price span not found for product '{product_name}' in {url}.")
                    else:
                        logging.warning(f"Outer price container not found for product '{product_name}' in {url}.")
                else:
                    logging.warning(f"Link tag not found for a product item in {url}.")

                products_to_insert.append((fecha_actual, product_name, product_price))

            except Exception as e:
                logging.error(f"Error processing a product item from {url}: {e}. Item data: {product_div.prettify()}", exc_info=True)
                continue # Skip to the next product

        if products_to_insert:
            c.executemany(f"INSERT INTO {tabla_nombre} (Fecha, Descripcion, Precio) VALUES (?, ?, ?)", products_to_insert)
            conn.commit() # Commit DML (inserts) once per page
            logging.info(f"Inserted {len(products_to_insert)} products from {url} into the database.")
        else:
            logging.info(f"No products to insert from {url}.")

except sqlite3.Error as e:
    logging.error(f"Database error: {e}", exc_info=True)
except WebDriverException as e:
    logging.error(f"WebDriver error: {e}", exc_info=True)
except Exception as e:
    logging.error(f"An unexpected error occurred: {e}", exc_info=True)
finally:
    if driver:
        driver.quit()
        logging.info("WebDriver closed.")
    if conn:
        conn.close()
        logging.info("Database connection closed.")

logging.info("Script finished.")
