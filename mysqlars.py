#########################################################################################################################
#
# Conexion, campos requeridos en formato DICT:
#   user: usario de la BBDD
#   password: clave del user para BBDD
#   db: BBDD a la que se desea conectar
#   host: servidor de BBDD
#   charset: enconding de la informacion
#
#
# Formato DICT para la ejecucion de peticiones a BBDD:
#   #table: Nombre de la tabla.
#   #where: dict con las claves de las condiciones de la busqueda.
#       {'column': ['comparador', 'valor', 'relacion con la siguiente condicion']} - Condiciones multiples
#       ej: {#where: {'id': ['=', '1234', 'and']}
#       {'column': ['comparador', 'valor']} - Para una condicion unica
#       ej: {#where: {'id': ['=', '1234']}
#       {'column': ['Condicion en codigo SQL']} - Para introducir la condicion en sql directamente
#       ej: {#where: {'a.id = b.id'}
#   Cualquier otro dato en el dict se considera {'column': 'value'} ya sea para update o insert
#
# Campos unicos de select:
#   #reading_type: tipo de lectura o rows a recuperar por defecto one.
#       {'#reading_type': 'one'} - Para un unico registro o row
#       {'#reading_type': 'all'} o {'#reading_type': 'fetchall'} - Para todos los registros que cumplan las condiciones.
#       {'#reading_type': int} - Para un numero determinado de registros o rows
#   #dict: recuperacion de los registros en formato dict.
#       {'#dict': ''} - Para formato dict, el valor es indistinto no se usa.
#   #column: Campos o columnas que se desean recuperar en los registros o rows. Por defecto todos los de la tabla.
#       {'#column': 'column1, column2, etc...'} - Para indicar columnas que se desean recuperar.
#   #order_by: Orden de los registros recuperados.
#       ej: {'#order_by': 'column1, column2, etc...'}
#           {'#order_by': 'ASC' o 'DESC'}
#
#########################################################################################################################

import logging # Log
from pathlib import Path, WindowsPath # Path

import pymysql # Conexion sql

from common.archivos import tipo_fichero, leer_yaml, leer_json


# Gestion de conexion con sql a traves de dict para su conversion a SQL
class PyMySqlArs:

    '''Management of connection with sql through dict for its conversion to SQL.'''
 
 
    def __init__(self):
        
        self.logger = logging.getLogger(__name__)
        self.conn = None
        
        
    def conexion(self, login="sql/login/login_sql.yaml"):
        '''Funcion para el proceso de conexion con sql
        login: Informacion necesaria para el login. DICT o STR/PATH de un fichero YAML o JSON'''
         
        salida = None
        
        try:
            if type(login) is dict:
                data = login
            else:
                data = self.rec_data(login)
            
            if data != False:
                self.conn = pymysql.connect(user = data['user'],
                                    password = data['password'],
                                    db = data['db'],
                                    host = data['host'],
                                    charset = data['charset'])
            else:
                self.logger.error('conexion: Error in login_sql')
                                    
        except (ValueError, KeyError, pymysql.Error):
            self.logger.exception("conexion sql")
            salida = False
        else: 
            salida = self.conn
        finally:
            return salida
            
            
    def rec_data(self, login):
        '''Discriminamos si el login nos viene en un dict directamente o en fichero.
        Login: Informacion para el login. STR o PATH de un fichero YAML o JSON
        salida: informacion en formato Dict o False en caso de error. DICT o FALSE/NONE'''
        
        salida = None
        
        try:
            if type(login) is WindowsPath or type(login) is str:
                tipo = tipo_fichero(login)
                
                if tipo == False:
                    self.logger.info(f'rec_data: tipo_fichero reporta un error, {tipo}')
                elif tipo == None:
                    self.logger.warning(f'rec_data: tipo_fichero reporta un error desconocido, {tipo}')
                else:
                    if tipo.upper() == '.YAML':
                        data = leer_yaml(login) # Recuperamos los datos de conexion del fichero de login
                    elif tipo.upper() == '.JSON':
                        data = leer_json(login)
                    else:
                        self.logger.error('rec_data: Extesion no soportada')
                          
            else:
                self.logger.error('rec_data: Tipo de fichero no reconocido')
                
        except (ValueError, AttributeError, TypeError):
            self.logger.exception("rec_data")
            salida = False
        else:
            if type(data) is dict:
                self.logger.info('rec_data: Recuperados datos login sql')
                salida = data
            else:
                self.logger.error('rec_data: Datos recuperados no en formato Dict')
                salida = False
        finally:
            return salida
            
        
    def tratar_datos(self, datos):
        '''Discriminamos si los datos son un Dict o list/tupla de Dict. 
        Datos: Informacion a procesar. DICT o LIST/TUPLA de DICT
        Salida: Informacon pre-procesada o False en caso de error. DICT o LIST[DICT] o FALSE/NONE'''
        
        salida = None
        
        try:
            if type(datos) is list or type(datos) is tuple:
                datos_temp = []
                for d in datos:
                    if type(d) is dict:
                        dict_pre = self.tratar_dict(d)
                        if not dict_pre in [False, None]:
                            datos_temp.append(dict_pre)
                        else:
                            self.logger.warning('tratar_datos: tratar_dict reporta error')
                    else:
                        self.logger.warning(f'tratar_datos: Informacion no en formato dict {d}')
                        
                salida = datos_temp
                    
            elif type(datos) is dict:
                salida = self.tratar_dict(datos)  
                
            else:
                self.logger.error('tratar_datos: Tipo de datos no soportada')
                
        except (ValueError, AttributeError, TypeError):
            self.logger.exception("tratar_datos")
            salida = False
        else:
            self.logger.info('tratar_datos: Datos entrantes pre-procesados')
        finally:
            return salida
            
            
    def tratar_dict(self, datos_dict):
        '''Tratamos el dict entrante para adatarlo poco a poco formato sql.
        datos_dict: dict con la informacion a tratar. DICT
        salida: un dict con la informacion base tratada o un False de dar error. DICT o FALSE/NONE'''
        
        salida = None
        table_exists = False
        dict_salida = {}
        cabecera_temp = []
        values_temp = []
        where_value = []
        where = ''
        
        try:
            for key, value in datos_dict.items():
                if key == "#table":
                    dict_salida.update({key: value})
                    table_exists = True
                    
                elif key == "#where" and value != "":
                    for k,v in value.items():
                        if len(v) == 3:
                            where += f"{k} {v[0]} %s {v[2]} "
                        elif len(v) == 1:
                            where += f"{k} {v[0]}"
                        else:
                            where += f"{k} {v[0]} %s"
                            
                        where_value.append(v[1])
                        
                    dict_salida.update({key: [where, where_value]})
                else:
                    cabecera_temp.append(key)
                    values_temp.append(value)
                    
        except (ValueError, AttributeError, TypeError):
            self.logger.exception("tratar_dict")
            salida = False
        else:        
            if table_exists and cabecera_temp != [] and values_temp != []:
                dict_salida.update({'#column': [cabecera_temp, values_temp]})
                salida = dict_salida
            elif table_exists and cabecera_temp == [] and values_temp == []: 
                salida = dict_salida # El delete no necesita column 
            else:
                self.logger.error(f"tratar_dict: no table;{datos_dict}")
        finally:
            return salida
            

    def check_conn(self, conn):
        '''conn: False para solo recibir el update listo para ejecucion.'''

        if type(conn) is dict or type(conn) is WindowsPath or type(conn) is str:
            self.conexion(conn)
        elif conn = False:
            self.no_conn = True
        else:    
            self.conn = conn     

            
    def update(self, datos, conn=None):
        '''Recivimos los datos a updatear, los tratamos y ejecutamos el cursor.
        datos: un dict o una lista de ellos con la informacion a tratar. DICT o LIST/TUPLA de DICT
        conn: Conexion previamente establecida o datos para establecer una nueva.
        conn: False para solo recibir el update listo para ejecucion. 
        OBJ, DICT o STR/PATH de un fichero YAML o JSON. FALSE.'''
        
        salida = None
        self.no_conn = False
        
        data = self.tratar_datos(datos)
        
        try:
            if conn != None:
                self.check_conn(conn)
                    
            if not data in [False, None]:
                if type(data) is list:
                    salida_update = []
                    for i in data:
                        if self.no_conn:
                            s_update = self.tratar_update(i)
                            salida_update.append(s_update)
                        else:
                            self.tratar_update(i)
                    
                elif type(data) is dict:
                    if self.no_conn:
                        salida_update = self.tratar_update(data)
                    else:
                        self.tratar_update(data)
                        
            else:
                self.logger.warning('update: tratar_datos reporta error')
                    
        except (ValueError, AttributeError, TypeError):
            self.logger.exception("update")
            salida = False
        else:
            if self.no_conn:
                salida = salida_update
            else:
                salida = True
        finally:
            return salida


    def tratar_update(self, data):
        '''Preparamos el UPDATE con los datos del dict entrante.
        data: Dict tratado y pre-procesado para crear el update. DICT'''
        
        datos_update = []
        salida = None
        
        try:
            if len(data['#column'][0]) == 1:
                cabecera = f"{data['#column'][0][0]} = %s"
            else:
                cabecera = ' = %s, '.join(data['#column'][0])
                cabecera += ' = %s'
                    
            datos_update.extend(data['#column'][1]) # Valores
            datos_update.extend(data['#where'][1])
            
            UPDATE = f"UPDATE {data['#table']} SET {cabecera} WHERE {data['#where'][0]}" 
            
        except (ValueError, AttributeError, TypeError):
            self.logger.exception("tratar_update")
        else:
            if self.no_conn:
                salida = [UPDATE, datos_update]
            else:
                self.ejecutar_update(UPDATE, datos_update)
        finally:
            if self.no_conn:
                return salida
            

    def ejecutar_update(self, UPDATE, datos_update):
        '''UPDATE: Un str con el update en formato sql. STR
        datos_update: Valores de los campos del update. LIST'''

        try:
            c_update = self.conn.cursor() # Declarramos cursor 

            c_update.execute(UPDATE,(datos_update))
            
            self.conn.commit()
            
        except pymysql.Error:
            self.logger.exception(f"ejecutar_update: ({UPDATE},({datos_update}))")   
        else:
            c_update.close()
            self.logger.info(f"ejecutar_update ok: {UPDATE}")
            
        
    def insert(self, datos, conn=None):
        '''Recivimos los datos a insertar, los tratamos y ejecutamos el cursor.
        datos: un dict o una lista de ellos con la informacion a tratar. DICT o LIST/TUPLA de DICT
        conn: Conexion previamente establecida o datos para establecer una nueva. 
        conn: False para solo recibir el insert listo para ejecucion.
        OBJ, DICT o STR/PATH de un fichero YAML o JSON'''
        
        salida = None
        self.no_conn = False
        
        data = self.tratar_datos(datos)
        
        try:
            if conn != None:
                self.check_conn(conn)
                    
            if not data in [False, None]:
                if type(data) is list:
                    salida_insert = []
                    
                    for i in data:
                        if self.no_conn:
                            s_insert = self.tratar_insert(i)
                            salida_insert.append(s_insert)
                        else:
                            self.tratar_insert(i)
                    
                elif type(data) is dict: 
                    if self.no_conn:
                        salida_insert = self.tratar_insert(data)
                    else:
                        self.tratar_insert(data)
                    
            else:
                self.logger.warning('insert: tratar_datos reporta error')
                    
        except (ValueError, AttributeError, TypeError):
            self.logger.exception("insert")
            salida = False
        else:
            if self.no_conn:
                salida = salida_insert
            else:
                salida = True
        finally:
            return salida
            
            
    def tratar_insert(self, data):
        '''Preparamos el INSERT con los datos del dict entrante.
        data: Dict tratado y pre-procesado para crear el insert. DICT'''
        
        datos_insert = []
        salida = None
        
        try:
            if len(data['#column'][0]) == 1:
                cabecera = data['#column'][0][0]
            else:
                cabecera = ', '.join(data['#column'][0])
                
            var_string = str('%s, ' * len(data['#column'][1]))[:-2]
            INSERT = f"INSERT IGNORE INTO {data['#table']}({cabecera}) VALUES (%s);" % var_string
            
            datos_insert = data['#column'][1][:]
            
        except (ValueError, AttributeError, TypeError):
            self.logger.exception("tratar_insert")
        else:
            if self.no_conn:
                salida = [INSERT, datos_insert]
            else:
                self.ejecutar_insert(INSERT, datos_insert)
        finally:
            if self.no_conn:
                return salida
            
            
    def ejecutar_insert(self, INSERT, datos_insert):
        '''INSERT: Un str con el insert en formato sql. STR
        datos_insert: Valores de los campos del insert. LIST'''

        try:
            c_insert = self.conn.cursor() # Declarramos cursor 

            c_insert.execute(INSERT,(datos_insert))
            
            self.conn.commit()
            
        except pymysql.Error:
            self.logger.exception(f"ejecutar_insert: ({INSERT},({datos_insert}))")   
        else:
            c_insert.close() 
            self.logger.info(f"ejecutar_insert ok: {INSERT}")
            
            
    def delete(self, datos, conn=None):
        '''Recivimos los datos a delete, los tratamos y ejecutamos el cursor.
        datos: un dict o una lista de ellos con la informacion a tratar. DICT o LIST/TUPLA de DICT
        conn: Conexion previamente establecida o datos para establecer una nueva. 
        conn: False para solo recibir el update listo para ejecucion. 
        OBJ, DICT o STR/PATH de un fichero YAML o JSON. FALSE.'''
        
        salida = None
        self.no_conn = False
        
        data = self.tratar_datos(datos)
        
        try:
            if conn != None:
                self.check_conn(conn)
                    
            if not data in [False, None]:
                if type(data) is list:
                    salida_delete = []
                    for i in data:
                        if self.no_conn:
                            s_delete = self.tratar_delete(i)
                            salida_delete.append(s_delete)
                        else:
                            salida_delete.append(self.tratar_delete(i))
                    
                elif type(data) is dict:
                    if self.no_conn:
                        salida_delete = self.tratar_delete(data)
                    else:
                        salida_delete = self.tratar_delete(data)
                        
            else:
                self.logger.warning('delete: tratar_datos reporta error')
                    
        except (ValueError, AttributeError, TypeError):
            self.logger.exception("delete")
            salida = False
        else:
            #if self.no_conn:
            salida = salida_delete
        finally:
            return salida
            
            
    def tratar_delete(self, data):
        '''Preparamos el delete con los datos del dict entrante.
        data: Dict tratado y pre-procesado para crear el delete. DICT'''
        
        datos_delete = []
        salida = None
        
        try:
            datos_delete.extend(data['#where'][1])
            
            DELETE = f"DELETE FROM {data['#table']} WHERE {data['#where'][0]}" 
            
        except (ValueError, AttributeError, TypeError):
            self.logger.exception("tratar_delete")
        else:
            if self.no_conn:
                salida = [DELETE, datos_delete]
            else:
                salida = self.ejecutar_delete(DELETE, datos_delete)
                #salida = True
        finally:
            #if self.no_conn:
            return salida
                
                
    def ejecutar_delete(self, DELETE, datos_delete):
        '''DELETE: Un str con el delete en formato sql. STR
        datos_delete: Valores de los campos del delete. LIST'''
        
        salida = None
        
        try:
            c_delete = self.conn.cursor() # Declarramos cursor 

            c_delete.execute(DELETE,(datos_delete))
            
            self.conn.commit()
            
        except pymysql.Error:
            self.logger.exception(f"ejecutar_delete: ({DELETE},({datos_delete}))")   
        else:
            c_delete.close()
            self.logger.info(f"ejecutar_delete ok: {DELETE}")
            salida = True
        finally:
            return salida
               
            
    def select(self, data, conn=None):
        '''Funcion para la creacion de una select SQL a partir de un dict.
        data: Informacion requerida para formar la select. DICT o LIST[DICT]
        conn: Conexion previamente establecida o datos para establecer una nueva.
        conn: False para solo recibir el select listo para ejecucion.        
        OBJ, DICT o STR/PATH de un fichero YAML o JSON
        salida: Datos recuperados con la select o False si da error. DICT o LIST[DICT/LIST] o FALSE/NONE'''
        
        salida = None
        self.no_conn = False
        
        try:
            if conn != None:
                self.check_conn(conn)   
                    
            if type(data) is list:
                salida_select = []
                
                for i in data:
                    s_select = self.tratar_select(i)
                    salida_select.append(s_select)
                    
                salida = salida_select
                
            elif type(data) is dict:    
                salida = self.tratar_select(data)
                
            else:
                self.logger.error('select: Tipo de formato no soportada')
            
        except (ValueError, AttributeError, TypeError):
            self.logger.exception("select")
            salida = False
        finally:
            return salida
            
            
    def tratar_select(self, data):
        '''Preparamos los datos para la creacion de la select.
        data: Datos a tratar para la creacion de la select. DICT
        salida: Datos recuperados de la ejecucion de la select. DICT o LIST'''
        
        read = "one"
        self.column = "*"
        where_switch = False
        order_by_switch = False
        format_dict = False
        table_switch = False
        records = None
        many = "1"
        where = []
        where_values = []

        try:
            for key, value in data.items():
                if key == "#table" and value != "":
                    self.table = value
                    table_switch = True
                    
                elif key == "#reading_type" and value != "":
                    if type(value) is int:
                       many = value
                       read = "many"
                    elif value.upper() == "FETCHALL" or value.upper() == "ALL":
                       read = "all"
                    elif value.upper() == "ONE":
                       read = "one"
                       
                elif key == "#dict":
                    format_dict = True
                        
                elif key == "#column" and value != "":
                    self.column = value
                    
                elif key == "#where" and value != "":
                    for k,v in value.items():
                        if len(v) == 3:
                            where.append(f"{k} {v[0]} %s {v[2]}")
                            where_values.append(v[1])
                        elif len(v) == 1:
                            where.append(f"{k} {v[0]}")
                        else:
                            where.append(f"{k} {v[0]} %s")
                            where_values.append(v[1])
                        
                    self.where_keys = ' '.join(where)
                    where_switch = True
                    
                elif key == "#order_by" and value != "":
                    self.order_by_value = value
                    order_by_switch = True
                    
                else:
                    self.logger.warning(f"tratar_select: clave desconocida {key}:{value}")
                    
        except (ValueError, AttributeError, TypeError):
            self.logger.exception("tratar_select")
            records = False
        else:

            if table_switch:
                SELECT = self.mold_select(where_switch, order_by_switch)

                if self.no_conn:
                    records = [SELECT, where_values]
                else:    
                    records = self.ejecutar_select(SELECT, where_values, format_dict, where_switch, read, many)
            else:
                self.logger.error(f"tratar_select: no table {data}")
        finally:
            return records 
            
            
    def mold_select(self, where_switch, order_by_switch):
        '''Formamos la select con los datos entrantes.
        salida: SELECT en formato SQL para su ejecucion. STR'''
        
        SELECT = f"SELECT {self.column} FROM {self.table}" 

        if where_switch:
            SELECT += " WHERE " + self.where_keys

        if order_by_switch:
            SELECT += " ORDER BY " + self.order_by_value
            
        return SELECT
        
        
    def ejecutar_select(self, SELECT, where_values, format_dict=False, where_switch=False, read = "one", many=3):
        '''
        SELECT: Codigo SQL para la recuperacion de datos en BBDD.
        salida: registro recuperados de la peticion SQL. LIST o DICT
        '''
        
        try:
            if format_dict:
                c_select = self.conn.cursor(pymysql.cursors.DictCursor) # Declarramos cursor Dict
            else:
                c_select = self.conn.cursor(pymysql.cursors.Cursor) # Declarramos cursor Normal

            if where_switch:
                c_select.execute(SELECT,(where_values))
            else:
                c_select.execute(SELECT)
            
            if read == "one":    
                records = c_select.fetchone()
            elif read == "all":
                records = c_select.fetchall()
            elif read == "many":
                records = c_select.fetchmany(int(many))
                
        except (pymysql.Error,ValueError, AttributeError, TypeError):
            self.logger.exception(f"ejecutar_select: ({SELECT},({where_values}))")   
        else:
            c_select.close() 
            self.logger.info(f"ejecutar_select ok: {SELECT}")
        finally:
            return records
            