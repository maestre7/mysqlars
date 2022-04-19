#
# Conjunto de funciones para el tratamiento de ficheros
#

from pathlib import Path
import logging
import json
import yaml


logger = logging.getLogger(__name__)


def leer_json(origen):

    '''Origen: Path de un fichero yaml. PATH o STR
    Salida: Los datos en el formato salvado o False en caso de error. TYPE DATA FILE o FALSE'''

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


def leer_yaml(origen):

    '''Origen: Path de un fichero yaml. PATH o STR
    Salida: Los datos en el formato salvado o False en caso de error. TYPE DATA FILE o FALSE'''
    
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
       
        
def tipo_fichero(fichero):

    '''Localizamos la extesion del fichero.
    fichero: Path del fichero a tratar. PATH o STR
    salida: Extension del fichero con el punto o False si da error. STR o FALSE'''
    
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
        
        