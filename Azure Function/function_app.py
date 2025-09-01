import logging
import azure.functions as func
import requests
import json
import time
import os
from azure.storage.blob import BlobServiceClient
from datetime import datetime


import requests
import json
from bs4 import BeautifulSoup
import time
import logging
import os
from typing import List, Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import pandas as pd

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RutasScraper:
    def __init__(self, base_url: str = 'http://www.mio.com.co/index.php/rutas-119/'):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.lock = threading.Lock()
        
    def get_rutas_from_page(self, url: str) -> List[str]:
        """Obtiene las rutas de una página específica"""
        try:
            logger.info(f"Obteniendo datos de: {url}")
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.find('table', class_='category')
            
            if not table:
                logger.warning(f"No se encontró tabla en: {url}")
                return []
            
            rutas = []
            for row in table.find_all('tr'):
                link = row.find('a')
                if link and link.text:
                    ruta = link.text.strip().replace("\n\t\t\t\t\t\t", "")
                    if ruta:  # Solo agregar si no está vacío
                        rutas.append(ruta)
            
            logger.info(f"Encontradas {len(rutas)} rutas en {url}")
            return rutas
            
        except requests.RequestException as e:
            logger.error(f"Error al obtener {url}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error inesperado al procesar {url}: {e}")
            return []
    
    def scrape_all_rutas(self, types: List[str] = None, indices: List[int] = None) -> List[str]:
        """Scrapes todas las rutas de todos los tipos e índices"""
        if types is None:
            types = ["troncales", "expresas", "pretroncales", "alimentadoras"]
        if indices is None:
            indices = [0, 10, 20, 30, 40, 50]
        
        all_rutas = []
        
        for tipo in types:
            logger.info(f"Procesando tipo: {tipo}")
            for i in indices:
                url = f"{self.base_url}rutas-{tipo}.html?start={i}"
                rutas = self.get_rutas_from_page(url)
                
                if not rutas:  # Si no hay rutas, probablemente llegamos al final
                    logger.info(f"No se encontraron más rutas para {tipo} en índice {i}")
                    break
                
                all_rutas.extend(rutas)
                time.sleep(2.5)  # Delay para ser respetuoso con el servidor
            time.sleep(2.5)
        
        # Eliminar duplicados manteniendo el orden
        unique_rutas = list(dict.fromkeys(all_rutas))
        logger.info(f"Total de rutas únicas encontradas: {len(unique_rutas)}")
        
        return unique_rutas

    def get_paradas(self, ruta: str) -> Optional[Dict[str, Any]]:
        """Obtiene las paradas para una ruta específica"""
        try:
            url = f"https://servicios.siur.com.co/buscarutas/marks.php?ruta={ruta}&0.8074243741234619"
            logger.info(f"Obteniendo paradas para ruta: {ruta}")
            
            response = self.session.post(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Paradas obtenidas para {ruta}: {len(data) if isinstance(data, list) else 'N/A'}")
            
            return {
                'ruta': ruta,
                'paradas': data,
                'timestamp': time.time()
            }
            
        except requests.RequestException as e:
            logger.error(f"Error al obtener paradas para {ruta}: {e}")
            return {
                'ruta': ruta,
                'paradas': [],
                'error': str(e),
                'timestamp': time.time()
            }
        except json.JSONDecodeError as e:
            logger.error(f"Error al decodificar JSON para {ruta}: {e}")
            return {
                'ruta': ruta,
                'paradas': [],
                'error': f'JSON decode error: {e}',
                'timestamp': time.time()
            }
        except Exception as e:
            logger.error(f"Error inesperado al obtener paradas para {ruta}: {e}")
            return {
                'ruta': ruta,
                'paradas': [],
                'error': str(e),
                'timestamp': time.time()
            }

    def get_gps_mio(self, ruta: str) -> Dict[str, Any]:
        """Obtiene datos GPS para una ruta específica"""
        try:
            url = f"https://servicios.siur.com.co/buscarutas/marks2.php?ruta={ruta}"
            logger.info(f"Obteniendo GPS para ruta: {ruta}")
            
            response = self.session.post(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            return {
                'ruta': ruta,
                'gps_data': data,
                'timestamp': time.time(),
                'success': True
            }
            
        except requests.RequestException as e:
            logger.error(f"Error al obtener GPS para {ruta}: {e}")
            return {
                'ruta': ruta,
                'gps_data': [],
                'error': str(e),
                'timestamp': time.time(),
                'success': False
            }
        except json.JSONDecodeError as e:
            logger.error(f"Error al decodificar JSON GPS para {ruta}: {e}")
            return {
                'ruta': ruta,
                'gps_data': [],
                'error': f'JSON decode error: {e}',
                'timestamp': time.time(),
                'success': False
            }
        except Exception as e:
            logger.error(f"Error inesperado al obtener GPS para {ruta}: {e}")
            return {
                'ruta': ruta,
                'gps_data': [],
                'error': str(e),
                'timestamp': time.time(),
                'success': False
            }

    def process_ruta_gps(self, ruta: str) -> Dict[str, Any]:
        """Procesa una ruta individual para obtener GPS (para uso con ThreadPoolExecutor)"""
        return self.get_gps_mio(ruta)

def main():
    """Función principal"""
    scraper = RutasScraper()
    
    # Obtener rutas
    logger.info("Iniciando scraping de rutas...")
    rutas = scraper.scrape_all_rutas()
    
    if not rutas:
        logger.error("No se encontraron rutas. Saliendo...")
        return
    
    logger.info(f"Procesando {len(rutas)} rutas para obtener datos GPS...")
    
    
    # Usar ThreadPoolExecutor para procesar múltiples rutas en paralelo
    gps_data = []
    max_workers = 10  # Ajustar según la capacidad del servidor
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Enviar todas las tareas
        future_to_ruta = {executor.submit(scraper.process_ruta_gps, ruta): ruta for ruta in rutas}
        
        # Procesar resultados conforme se completan
        completed = 0
        for future in as_completed(future_to_ruta):
            ruta = future_to_ruta[future]
            try:
                result = future.result()
                gps_data.append(result)
                completed += 1
                
                # Log progreso cada 10 rutas
                if completed % 10 == 0:
                    logger.info(f"Progreso: {completed}/{len(rutas)} rutas procesadas")
                
                # Guardar datos parciales cada 50 rutas para no perder progreso
                if completed % 50 == 0:
                    partial_file = f"tracking_data/gps_data_partial_{completed}.json"
                    with open(partial_file, "w", encoding='utf-8') as f:
                        json.dump(gps_data, f, indent=2, ensure_ascii=False)
                    logger.info(f"Datos parciales guardados en {partial_file}")
                
            except Exception as e:
                logger.error(f"Error procesando ruta {ruta}: {e}")
                gps_data.append({
                    'ruta': ruta,
                    'gps_data': [],
                    'error': str(e),
                    'timestamp': time.time(),
                    'success': False
                })
    
    # Guardar datos finales en Azure Blob Storage
    try:
        # Obtener la conexión de Azure Storage desde variables de entorno
        connection_string = os.environ.get('AzureWebJobsStorage')
        container_name = os.environ.get('BLOB_CONTAINER_NAME', 'mio-data')
        
        if connection_string:
            # Crear cliente de Blob Storage
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            container_client = blob_service_client.get_container_client(container_name)
            
            # Crear el contenedor si no existe
            try:
                container_client.get_container_properties()
            except:
                container_client.create_container()
                logger.info(f"Contenedor '{container_name}' creado exitosamente")
            
            # Generar nombre del archivo con timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            blob_name = f"MIO_gps_Data/gps_data_{timestamp}.parquet"
            
            # Convertir datos a DataFrame y luego a bytes
            df = pd.DataFrame(gps_data)
            parquet_bytes = df.to_parquet(index=False)
            
            # Subir a Blob Storage
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(parquet_bytes, overwrite=True)
            
            logger.info(f"Datos GPS guardados exitosamente en Azure Blob: {container_name}/{blob_name}")
            logger.info(f"Total de rutas procesadas: {len(gps_data)}")
            
            # Estadísticas
            rutas_con_gps = sum(1 for p in gps_data if p.get('success') and p.get('gps_data'))
            rutas_con_error = sum(1 for p in gps_data if not p.get('success'))
            
            logger.info(f"Rutas con GPS exitosas: {rutas_con_gps}")
            logger.info(f"Rutas con errores: {rutas_con_error}")
            
        else:
            # Fallback: guardar localmente si no hay conexión de Azure Storage
            output_file = os.path.join("tracking_data", f"gps_data_{time.strftime('%Y%m%d_%H%M%S')}.parquet")
            df = pd.DataFrame(gps_data)
            df.to_parquet(output_file, index=False)
            logger.info(f"Datos GPS guardados localmente en {output_file} (sin conexión Azure Storage)")
            
    except Exception as e:
        logger.error(f"Error al guardar archivo: {e}")
        # Fallback: guardar localmente en caso de error
        try:
            output_file = os.path.join("tracking_data", f"gps_data_error_{time.strftime('%Y%m%d_%H%M%S')}.parquet")
            df = pd.DataFrame(gps_data)
            df.to_parquet(output_file, index=False)
            logger.info(f"Datos guardados localmente como fallback: {output_file}")
        except Exception as fallback_error:
            logger.error(f"Error en fallback también: {fallback_error}")


app = func.FunctionApp()

@app.timer_trigger(schedule="* 5-23 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def MIORutasTrigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logger.info('The timer is past due!')
    main()
    logger.info('Python timer trigger function executed.')
