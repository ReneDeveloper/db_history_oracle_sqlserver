"""etl_import_empresas.py"""


import os
import pandas as pd
from sqlalchemy import create_engine
#from cryptography.fernet import Fernet
import sqlalchemy
#import cx_Oracle

ruta_files = 'D:/__DATA__/__EMPRESAS__/'


from sqlalchemy.engine import URL
connection_url = URL.create(
    "mssql+pyodbc",
    username="TEST",
    password="TEST",
    host="localhost\\XE19",
    #port=1433,
    database="test",
    query={
        "driver": "ODBC Driver 17 for SQL Server",
        "TrustServerCertificate": "yes",
        #"authentication": "ActiveDirectoryIntegrated",
    },
)

def _log(var):
    """function _log"""
    __log_active__ = True
    if __log_active__:
        print(f"LOG:{var}")

def get_engine_target():
    """function get_engine_target"""
    __target_url__ = "mssql+pymssql://user:pass@host/db"
    __target_url__ = "mssql+pymssql://TEST:TEST@192.168.1.5\\XE19"
    __target_url__ = connection_url



#Driver={ODBC Driver 17 for SQL Server};Server=serverName\instanceName;Database=myDataBase;Trusted_Connection=yes;

    engine = create_engine(connection_url)
    return engine

def target_copy_to_table(data,table_name):
    """function"""
    status = "ok"
    _log(f"target_copy_to_table:START:{table_name}")
    try:
        engine = get_engine_target()
        cnx = engine.connect()
        data.to_sql(f'{table_name}',if_exists='append',con=cnx,index=False,chunksize=100)
        del data
        _log(f"target_copy_to_table:END:{table_name}")
    finally:
        cnx.close()
        engine.dispose()
    return status

def save_comunas():

    file_comunas_xlsx = f'{ruta_files}comunas/cut_2018_v04.xlsx'

    df = pd.read_excel(file_comunas_xlsx, sheet_name="Sheet 1")

    print(df)
    target_copy_to_table(df,"comunas_2018")


def export_empresas(file_csv_empresas):
    """Exporta historia de las tablas contenidas en un owner"""
    file_csv = f'{ruta_files}{file_csv_empresas}'
    #ruta_csv = cfg.get_par('out_dir') + 'out_METADATA/' + file_csv
    df_empresas = pd.read_csv(file_csv, sep='\t', encoding='latin-1')
    target_copy_to_table(df_empresas,"TEST_2020_2024")
    #sql_metadata = f"select * from v_RESTANTES where owner='{owner_}'"
    #df_metadata = self.target_execute(sql_metadata)
    #for _ , row in df_empresas.iterrows():
    #    print (row)
        #owner_ = row['owner']
        #table_name_ = row['table_name']
        #column_name = row['column_name']
        #pars__ = {}
        #if table_name_.startswith("BIN$"):
        #    continue

        #pars__['_OWNER'] = owner_
        #pars__['_TABLE_NAME'] = table_name_
        #pars__['_COLUMN_NAME'] = column_name
        #pars__['year_'] = '2022'
        #self.export_history_table(owner_,table_name_)
    return "OK"

#valor = export_empresas('PUB_EMPRESAS_PJ_2020_A_2024.txt')


#a = get_engine_target()


import fiona

ruta = "D:/__DATA__/__EMPRESAS__/Comunas/"
src =  fiona.open(ruta + "comunas.shp") 
    # Hacer algo con los datos aquí

import simplekml

kml = simplekml.Kml()
pos = 0
# Iterar a través de las características del archivo shp y crear polígonos en el archivo KML
for feature in src:
    pos+=1
    print(f"imprimiendo{pos}")
    print(list(feature.keys()))
    print(list(feature["properties"].keys()))
    print(f"comuna{feature['properties']['Comuna']}")

    polygon = kml.newpolygon(name=feature["properties"]["Comuna"], outerboundaryis=feature["geometry"]["coordinates"])

# Guardar el archivo KML
#print(kml.kml())
#kml.save(ruta + "nombre_del_archivo.kml")

print("la palabra es una mapa, la descripcion es un ruido, la cosa es la cosa y está por debajo del lenguaje")
print("a la mierda las definiciones")
print("ya hablaremos de los limites")


save_comunas()
