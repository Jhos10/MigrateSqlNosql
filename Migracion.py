import sqlite3
import pprint as pp
from functools import reduce
from pymongo import MongoClient



class Migration:

    # Constructor de la clase Migration
    def __init__(self,pRuta:str,localhost:str,bd_nosql)->None:
        # Atributos de la clase

        # Conexion a la base de datos
        self.db = sqlite3.connect(pRuta)
        # Cursor para interactuar con la base de datos
        self.cursor = self.db.cursor()
        # Conexion a la base de datos no relacional
        self.cliente = MongoClient(localhost)
        self.db_nosql = self.cliente[bd_nosql]
    
    # Funciona para obtener el nombre de las entidades
    def getNamesEntity(self)->list:
        # Consulta a la base de datos
        consulta = "SELECT * FROM Sqlite_master WHERE type = 'table';"
        # Ejecuta la consulta 
        self.cursor.execute(consulta)
        # Lista de entidades
        lista_entidades = self.cursor.fetchall()
        # Traer obtener solo el nombre de la entidad y agregarla a una lista.
        self.lista_entidades = list(x[1] for x in lista_entidades)
        # Se retorna la lista con el nombre de las entidades de la base de datos.
        return self.lista_entidades

    # Funciona para obtener los atributos de cada una de las entidades de la base de datos
    def getAtributeEntity(self)->list:
        self.getNamesEntity()
        # Lista de los atributos de las entidades
        self.lista_atributos_entidad = []
        # Se itera la lista de los nombres de cada entidad.
        for nombre_entidad in self.lista_entidades:
            # Se hace la consulta obtener la configuracion de la entidad
            consulta = f"Pragma Table_info({nombre_entidad});"
            # Se ejecuta la consulta
            self.cursor.execute(consulta)
            # Se obtine la lista de tuplas con la respectiva configuracion de la entidad
            configuracion_entidad = self.cursor.fetchall()
            # Se saca solo los atributos de la lista de tuplas.
            atributos = tuple(x[1] for x in configuracion_entidad)
            # Se agrega los atributos de la entidad a una lista de atributos la cual va a tener contenido cada una de las configuraciones de los atributos
            self.lista_atributos_entidad.append(atributos)

        # Se retorna la lista de atributos
        return self.lista_atributos_entidad
    
    # Funcion para obtener los registros de las tablas
    def getRegisterEntity(self)->list[tuple]:
        self.getNamesEntity()
        self.lista_registros = []
        for nombre_entidad in self.lista_entidades:
            consulta = f"Select * from {nombre_entidad};"
            self.cursor.execute(consulta)
            registros = self.cursor.fetchall()
            self.lista_registros.append(registros)
        
        return self.lista_registros
    
    def unionInformationEntity(self)->dict:
        self.getRegisterEntity()
        self.getAtributeEntity()
        self.informacion_db_sql = {}
        for indice in range(len(self.lista_entidades)):
            lista_informacion = list(map(lambda x: dict(zip(self.lista_atributos_entidad[indice],x)),self.lista_registros[indice]))
            self.informacion_db_sql[self.lista_entidades[indice]] = lista_informacion
        return self.informacion_db_sql
    
    def migrateNosql(self)->int:
       self.unionInformationEntity()
       for nombre_entidad in self.informacion_db_sql:
            coleccion = self.db_nosql[nombre_entidad]
            coleccion.insert_many(self.informacion_db_sql[nombre_entidad])



    
                
        


migracion = Migration('ProyectosConstruccion.db',"mongodb://localhost:27017","ProyectoConstruccion")
# migracion.getNamesEntity()
migracion.migrateNosql()

        



# Consulta a la base de datos.
# Ejecuta la consulta de la base de datos.
# Funcion que saca el listado de los nombres de las tablas de cada entidad.

# Saca de cada una de las tablas que contienen informacion de cada entidad, unicamente el nombre.


# def getNamesEntity()

# Diccionario que va contener cada informacion de cada una de las tablas
# diccionario = {}
# for nombre_tabla in nombrelist:
#     # Consulta para sacar el nombre de cada atributo de la entidad
#     columnasNombre = f"Pragma table_info({nombre_tabla});"
#     # Ejecuta la consulta sql anterior.
#     cursor.execute(columnasNombre)
#     # Trae la informacion de la consulta en una lista de tuplas
#     nombreColumnas = cursor.fetchall()
#     # Se trae solamente la informacion de los nombres de cada atributo
#     nombreColumnas = tuple([x[1] for x in nombreColumnas])
#     consulta = f"SELECT * FROM {nombre_tabla} limit 5;"
#     cursor.execute(consulta)
#     informacionEntidad = cursor.fetchall()

#     informacionEntidad = list(map(lambda x: dict(zip(nombreColumnas,x)),informacionEntidad))
#     diccionario[nombre_tabla] = informacionEntidad

# pp.pprint(diccionario)



