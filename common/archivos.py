#
# Conjunto de funciones para el tratamiento de ficheros
#

from pathlib import Path
import logging
import json
import csv

import requests
import yaml 
from fake_useragent import UserAgent, FakeUserAgentError
from PIL import Image, ImageChops # Pillow


logger = logging.getLogger(__name__)


def leer_json(origen):

    '''
    Origen: Path de un fichero yaml. PATH o STR 
    Salida: Los datos en el formato salvado o False en caso de error. TYPE DATA FILE o FALSE
    '''
    
    salida = None
    
    try:
        with open(Path(origen)) as f:
            salida = json.load(f)
            
    except (ValueError, AttributeError, TypeError, OSError):
        logger.exception('leer_json')
        salida = False
    else:
        logger.info("Json file read: OK ")  
    finally:
        return salida
        
    
def registro_json(destino, informacion, tipo="w"): 

    '''
    Destino: Path de un fichero csv. PATH o STR
    Informacion: datos a escribir.
    Tipo: Formato de escritura. STR
    '''
    
    salida = None
    
    try:
        with open(Path(destino), tipo) as f:
            json.dump(informacion, f)
            
    except (ValueError, AttributeError, TypeError, OSError):
        logger.exception('registro_json')
        salida = False
    else:
        logger.info("Json file write: OK ")  
        salida = True
    finally:
        return salida
        
    
def leer_csv(origen, fieldnames = None, encoding = "utf-8", fdict = True):

    '''
    Origen: path de un fichero csv. PATH o STR
    Fieldnames: Es una lista con los nombres de campo o columna en una secuencia de claves 
    que identifican el orden en el que pasaron los valores del diccionario. LIST
    Encoding: codificacion usada con el fichero. STR
    Fdict: Formato dict o csv normal. TRUE/FALSE
    Salida: Una lista con los distintos Dict o Str dependiendo si esta o no activo el 
    formato dict o un False en caso de error.  LIST o FALSE
    '''
    
    salida = None
    
    try:
        with open(Path(origen), encoding=encoding) as csvfile:
            if fdict:
                if fieldnames != None:
                    reader = csv.DictReader(csvfile, fieldnames=fieldnames)
                else:
                    reader = csv.DictReader(csvfile)
            else:
                reader = csv.reader(csvfile)

            salida = [row for row in reader]

    except (ValueError, AttributeError, TypeError, OSError):
        logger.exception('leer_csv')
        salida = False
    else:    
        logger.info("CSV file read: OK ")
    finally:
        return salida
        
    
def registro_csv(destino, informacion, fieldnames = None, 
                 tipo = "a+", encoding = "utf-8"):

    '''
    Destino: Path de un fichero csv. PATH o STR
    Informacion: Datos a escribir en formato en csv. STR o LIST o TUPLE
    Fieldnames: Es una lista con los nombres de campo o columna, es una secuencia de claves 
    que identifican el orden en el que pasaron los valores del diccionario. LIST
    Tipo: Formato de escritura. STR
    Encoding: codificacion usada con el fichero. STR
    Salida: True en caso de ir todo bien o False si da un error. TRUE/FALSE
    '''
    
    salida = None
    
    try:
        with open(Path(destino), tipo, encoding=encoding) as csvfile:
            
            if fieldnames != None and type(informacion) in [list, tuple]:
                for i in informacion: # Dict list/tuple
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writerow(i)
                    salida = True
                logger.info("CSV file write: OK ")
            elif fieldnames == None and type(informacion) in [list, tuple]: 
                for i in informacion: # Normal list/tuple
                    writer = csv.writer(csvfile)
                    writer.writerow(i)
                    salida = True
                logger.info("CSV file write: OK ")
            elif fieldnames != None and type(informacion) is dict:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writerow(informacion) # Dict str
                salida = True
                logger.info("CSV file write: OK ")
            elif fieldnames == None and type(informacion) is str:
                writer = csv.writer(csvfile) # Normal str
                writer.writerow(informacion)
                salida = True
                logger.info("CSV file write: OK ")
            else:
                logger.info("CSV file write: Type no sorpotado ") 
                salida = False
                
    except (ValueError, AttributeError, TypeError, OSError):
        logger.exception('registro_csv')
        salida = False
    finally:
        return salida


def leer_yaml(origen):

    '''
    Origen: Path de un fichero yaml. PATH o STR 
    Salida: Los datos en el formato salvado o False en caso de error. TYPE DATA FILE o FALSE
    '''
    
    salida = None
    
    try:
        with open(Path(origen)) as f:
            salida = yaml.safe_load(f, Loader=yaml.FullLoader)
            
    except (ValueError, AttributeError, TypeError, OSError):
        logger.exception('leer_yaml')
        salida = False
    else:
        logger.info("Yaml file read: OK ")  
    finally:
        return salida
    
    
def registro_yaml(destino, informacion, tipo="w"):

    '''
    Destino: Path de un fichero yaml. PATH o STR
    Informacion: datos a escribir.
    Tipo: Formato de escritura. STR
    '''
    
    salida = None
    
    try:
        with open(Path(destino), tipo) as f:
            yaml.dump(informacion, f)
            
    except (ValueError, AttributeError, TypeError, OSError):
        logger.exception('registro_yaml')
        salida = False
    else:
        logger.info("Yaml file write: OK ")  
        salida = True
    finally:
        return salida
         

def leer_fichero(origen, encoding="utf-8"):

    '''
    Origen: Path de un fichero texto. PATH o STR 
    Encoding: codificacion usada con el fichero. STR
    Salida: Los datos en el formato salvado o False en caso de error. LIST o FALSE
    '''
    
    salida = None
    
    try:
        with open(Path(origen),'r', encoding=encoding) as registros: #open for reading
            salida = registros.read().splitlines() #informacion recuperada en lista
            
    except (ValueError, AttributeError, TypeError, OSError):
        logger.exception('leer_fichero')
        salida = False
    else:
        logger.info(f"Read file: OK ")     
    finally:
        return salida
        

def registro_fichero(destino, informacion, encoding="utf-8", tipo="a+"):

    '''
    Destino: Path de un fichero de texto. PATH o STR
    Informacion: datos a escribir. STR
    Encoding: codificacion usada con el fichero. STR
    Tipo: Formato de escritura. STR
    '''
    
    salida = None
    
    try:
        with open(Path(destino), tipo, encoding=encoding) as registro: #open for writing
            if type(informacion) in [list, tuple]:
                for i in informacion:
                    registro.write(i) #write the info
                salida = True
                logger.info("Write file: OK") 
            elif type(informacion) is str:         
                registro.write(informacion) #write the info
                salida = True  
                logger.info("Write file: OK") 
            else:
                logger.info("Write file: Type no sorpotado") 
                salida = False
            
    except (ValueError, AttributeError, TypeError, OSError):
        logger.exception('registro_fichero')
        salida = False       
    finally:
        return salida    
        
        
def descarga_fichero_binario(origen, destino, my_referer = None):

    '''
    Recogemos un fichero url y lo descargamos en el path de destino.
    Origen: Url desde la que descargamos el fichero binario. STR
    Destino: Path donde descargamos la imagen. PATH o STR
    '''
    
    mensaje = f"{origen};{destino};{my_referer}"
    salida = None
    
    try:
        try:
            ua = UserAgent()
        except FakeUserAgentError:
            logger.exception(f"FakeUserAgentError")
            userAgent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36'
        else:
            userAgent = ua.random

        headers = {'User-Agent': userAgent}
        # Añadimos referencia si esta disponible
        if my_referer != None:
            headers.update({'referer': my_referer})

        fichero = requests.get(origen, headers = headers) #le pasamos cabecera para que no de error
        
        if fichero.ok:
            try:
                with open(Path(destino),'wb') as writer: #open for writing in binary mode
                    writer.write(fichero.content) #write the info
            except (ValueError, AttributeError, TypeError, OSError):
                logger.exception(f"E001.C.File download: {mensaje}")
                salida = False
            else:
                logger.info("File download: OK") 
                salida = True  
        else:
            logger.error(f"E001.A.File download: {mensaje}, {fichero}")

    except ConnectionError:
        logging.exception(f"E001.B.File download: {mensaje}")
        salida = False
    finally:
        return salida
        
			
def crear_carpetas(names, root = "."):

    '''
    Comprovamos la existencia de las carpetas y de ser necesario las creamos.
    names: Nombre de las carpetas a crear. LIST
    root: Raiz desde la que se crean las carpetas. PATH o STR
    salida: Informa si ya existia la ultima carpeta. TRUE/FALSE
    Devuelve el Path final de la ultima carpeta cread en arbol o False si falla. PATH o FALSE
    '''
    
    folder = str(root)
    exists = None
    folder_temp = False
    
    try:
        for a in names:
            folder += f"/{a}"
            folder_temp = Path(folder)
            if folder_temp.exists():
                exists = True
            else:
                folder_temp.mkdir()
                exists = False
                
    except (ValueError, AttributeError, TypeError, OSError):
        logger.exception('crear_carpetas')
        exists = None
    else:        
        logger.info("folder created: OK")  
    finally:        
        return exists, folder_temp
        
    
def contar_archivos(carpeta, tipo='*'):

    ''' 
    Recuperamos el numero de archivos en la carpeta, se puede discriminar por tipo.
    Carpeta: Directorio donde se realiza la busqueda. PATH o STR
    Tipo: Tipo de fichero a buscar. STR 
    salida: Numero de imagenes encontradas. INT
            Lista con el path de los archivos encontrados. LIST[PATH]
    '''
    
    num_archivos = False
    archivos = False
    
    try:
        archivos = sorted(Path(carpeta).glob(f'*.{tipo}'))
        num_archivos = len(archivos)
        
    except (ValueError, AttributeError, TypeError, OSError):
        logger.exception('contar_archivos')
    else:        
        logger.info(f"contar_archivos: {num_archivos}")  
    finally:        
        return num_archivos, archivos
    
 
def comparar_img(img1, img2):

    '''
    Comparamos dos imagenes para ver si son iguales.
    img1: Path de la imagen a trata. PATH o STR
    img2: Path de la imagen a trata. PATH o STR
    Salida: Informamos si son iguales o no, None si falla. TRUE/FALSE o NONE
    '''
    
    salida = None
    
    try:
        load_img1 = Image.open(Path(img1)).convert('RGB')
        load_img2 = Image.open(Path(img2)).convert('RGB')
    
        diff = ImageChops.difference(load_img1, load_img2)

        if not diff.getbbox():
            salida = True # Son iguales
        else:
            salida = False
            
    except (ValueError, AttributeError, TypeError, OSError):
        logger.exception('comparar_img')
    else:        
        logger.info(f"comparar_img: {img1}, {img2}")  
    finally:        
        return salida
        
        
def tipo_fichero(fichero):

    '''
    Localizamos la extesion del fichero.
    fichero: Path del fichero a tratar. PATH o STR
    salida: Extension del fichero con el punto o False si da error. STR o FALSE
    '''
    
    salida = None
    
    try:
        salida = Path(fichero).suffix
        
    except (ValueError, AttributeError, TypeError, OSError):
        logger.exception('tipo_fichero')
        salida = False
    else:        
        logger.info(f"tipo_fichero: {salida}")  
    finally:        
        return salida
        
        