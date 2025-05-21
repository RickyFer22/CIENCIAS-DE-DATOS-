import os
import subprocess
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(filename)s - [%(lineno)d] - %(message)s')

# Lista de scripts a ejecutar
scripts = [
    "1electropunto.py",
    "2casadelaslamparas.py",
    "3iluminar.py",
    "4electrolineas.py",
    "5electromisiones.py",
    "6listas_en_excel.py",
    "7envio_email.py"
]

# Flag to track overall success
overall_success = True

logging.info("Main script started. Beginning execution of sub-scripts.")

# Ejecutar cada script
for script in scripts:
    script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), script) # Assuming scripts are in the same directory
    logging.info(f"Ejecutando {script_path}...")
    try:
        # Using capture_output=True to get stdout/stderr for logging
        process = subprocess.run(["python", script_path], check=True, capture_output=True, text=True, encoding='utf-8')
        logging.info(f"{script} ejecutado exitosamente.")
        if process.stdout: # Log stdout if any
            logging.info(f"Output de {script}:\n{process.stdout}")
        # Stderr might contain warnings even on success, log if present
        if process.stderr:
            logging.warning(f"Stderr de {script} (aunque no lanzó error):\n{process.stderr}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error al ejecutar {script}. Código de retorno: {e.returncode}")
        if e.stdout:
            logging.error(f"Stdout de {script} al fallar:\n{e.stdout}")
        if e.stderr:
            logging.error(f"Stderr de {script} al fallar:\n{e.stderr}")
        overall_success = False
    except FileNotFoundError:
        logging.error(f"Error: El script {script_path} no fue encontrado. Verifique la ruta.")
        overall_success = False
    except Exception as ex: # Catch any other unexpected errors during subprocess run
        logging.error(f"Un error inesperado ocurrió al intentar ejecutar {script_path}: {ex}", exc_info=True)
        overall_success = False

# Check overall success and exit accordingly
if overall_success:
    logging.info("Todos los scripts se ejecutaron exitosamente.")
    sys.exit(0) # Explicitly exit with 0 for success
else:
    logging.error("Uno o más scripts fallaron durante la ejecución.")
    sys.exit(1) # Exit with 1 to indicate failure
```
