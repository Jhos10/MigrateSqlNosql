import sqlite3
import pprint as pp
from functools import reduce
from pymongo import MongoClient

# Conexion a la base de datos
db = sqlite3.connect('ProyectosConstruccion.db')
cursor = db.cursor()

# Consulta a la base de datos.
consulta = "SELECT * FROM Sqlite_master WHERE type = 'table';"
# Ejecuta la consulta de la base de datos.
cursor.execute(consulta)
# Funcion que saca el listado de los nombres de las tablas de cada entidad.
nombrelist = cursor.fetchall()
# Saca de cada una de las tablas que contienen informacion de cada entidad, unicamente el nombre.
nombrelist = list(x[1] for x in nombrelist)

# Diccionario que va contener cada informacion de cada una de las tablas
diccionario = {}
for nombre_tabla in nombrelist:
    
    columnasNombre = f"Pragma table_info({nombre_tabla});"
    cursor.execute(columnasNombre)
    nombreColumnas = cursor.fetchall()
    nombreColumnas = [x[1] for x in nombreColumnas]
    lista = tuple([x[1] for x in nombreColumnas])
    consulta = f"SELECT * FROM {nombre_tabla};"
    cursor.execute(consulta)
    informacionEntidad = cursor.fetchall()

    prueba = list(map(lambda x: dict(zip(nombreColumnas,x)), informacionEntidad))
    diccionario[nombre_tabla] = prueba

    # pp.pprint(informacionEntidad)
    # diccionario = dict(zip)

# informacion = reduce(lambda x,y: x+y, informacion)
# pp.pprint(informacion)
pp.pprint(diccionario)
pp.pprint(type(diccionario['Tipo'][0]['Area_Max']))

# for nombre_entidad in diccionario:
#     print(nombre_entidad)
#     pp.pprint(diccionario[nombre_entidad])

