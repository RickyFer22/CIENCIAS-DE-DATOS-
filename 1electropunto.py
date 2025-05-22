import requests
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

logging.info("Script_started")

try:
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    urls = [
    "https://electropunto.com.ar/search/?q=cable+",
    "https://electropunto.com.ar/protecciones/",
    "https://electropunto.com.ar/cajas-y-canerias/",
    "https://electropunto.com.ar/accesorios-de-electricidad/"
]

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}

    # Crear una tabla para almacenar los datos
    tabla_nombre = 'electropunto'
    c.execute(f'''CREATE TABLE IF NOT EXISTS {tabla_nombre}
                    (Fecha TEXT, Descripcion TEXT, Precio TEXT)''')
    conn.commit() # Commit table creation

    for url in urls:
        logging.info(f"Processing URL: {url}")
        try:
            response = requests.get(url, headers=headers, timeout=10) # Added timeout
            response.raise_for_status() # Raises an HTTPError for bad responses (4XX or 5XX)
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching URL {url}: {e}")
            continue # Skip to the next URL

        soup = BeautifulSoup(response.content, 'html.parser')

        # Encuentra todos los divs de los productos
        product_divs = soup.find_all("div", class_="js-item-name h2 item-name")

        if not product_divs:
            logging.info(f"No products found for URL: {url}")
            continue

        logging.info(f"Found {len(product_divs)} products for URL: {url}")

        for product_div in product_divs:
            try:
                # Obtiene la fecha actual
                fecha_actual = datetime.now().strftime("%d/%m/%Y")

                # Obtiene el nombre del producto
                product_name_tag = product_div
                if product_name_tag:
                    product_name = product_name_tag.text.strip()
                else:
                    logging.warning(f"Product name not found for an item in {url}. Skipping item.")
                    continue

                # Obtiene el identificador del producto (opcional, not used in DB insert directly but good to have)
                # product_id = product_div.attrs.get("data-store", "").replace("product-item-name-", "")

                # Busca el precio del producto
                product_price = "No disponible" # Default price
                price_container_outer = product_div.find_next_sibling("div", class_="item-price-container")
                if price_container_outer:
                    price_container_inner = price_container_outer.find("div", class_="js-price-display price item-price")
                    if price_container_inner:
                        product_price = price_container_inner.text.strip()
                    else:
                        logging.warning(f"Price (inner container) not found for product '{product_name}' in {url}.")
                else:
                    logging.warning(f"Price (outer container) not found for product '{product_name}' in {url}.")

                # Insertar los datos en la tabla
                c.execute(f"INSERT INTO {tabla_nombre} (Fecha, Descripcion, Precio) VALUES (?, ?, ?)", (fecha_actual, product_name, product_price))
            except AttributeError as e:
                logging.error(f"Error parsing product data for an item in {url}: {e}. Skipping item.")
                continue
            except Exception as e:
                logging.error(f"An unexpected error occurred while processing a product in {url}: {e}. Skipping item.")
                continue


    conn.commit() # Commit all changes once after the loop
    logging.info("All data committed to the database.")

except sqlite3.Error as e:
    logging.error(f"Database error: {e}")
except Exception as e:
    logging.error(f"An unexpected error occurred: {e}")
finally:
    if 'conn' in locals() and conn:
        conn.close()
        logging.info("Database connection closed.")

logging.info("Script finished successfully.")
