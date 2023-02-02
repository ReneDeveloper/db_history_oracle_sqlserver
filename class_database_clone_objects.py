"""class_database_clone_objects.py"""
import pandas as pd
from sqlalchemy import create_engine
from class_config import Config
cfg = Config()

class DatabaseCloneObjectSqllite():
    """DatabaseCloneObjectSqllite: clona vistas"""
    __tagname__ = ""
    __source_url__ = ""
    __target_url__ = ""
    
    def __init__(self,__tagname__):
        base = 'BRAHMS1P'#TODO: CAMBIAR A INSTALL, y apuntar a INSTALL_EXPORT_HISTORY.db
        self.__source_url__ = f"sqlite:///{cfg.get_par('out_path')}/{base}_EXPORT_HISTORY.db"
        self.__target_url__ = f"sqlite:///{cfg.get_par('out_path')}/{__tagname__}_EXPORT_HISTORY.db"
        self.__tagname__ = __tagname__

    def get_engine_source(self):
        engine = create_engine(self.__source_url__)
        return engine

    def get_engine_target(self):
        engine = create_engine(self.__target_url__)
        return engine

    def execute_sql_source(self,sql__):
        """function execute_sql_source"""
        engine = self.get_engine_source()
        data   = pd.read_sql(sql__, engine)
        return data

    def execute_sql_target_without_result(self,sql__):
        """function execute_sql_target_without_result"""
        status = "ok"
        engine = self.get_engine_target()
        con = engine.connect()
        con.execute(sql__)
        return status

    def clone_objects(self,__type__):
        query_views=f"""
            SELECT 
            NAME,SQL
            FROM 
                sqlite_master  
            WHERE 
                type='{__type__}' 
                --AND name='v_HISTORY'
            """
        data_views = self.execute_sql_source(query_views)
        for _, row in data_views.iterrows():
            row = dict(row)
            name = row['name']
            sql = row['sql']
            print(f"name:{name}")
            print(f"sql:{sql}")

            sql = sql.replace('CREATE TABLE ','CREATE TABLE IF NOT EXISTS ')
            sql = sql.replace('CREATE VIEW ','CREATE VIEW IF NOT EXISTS ')
            

            self.execute_sql_target_without_result(sql)

        print(f"data_views:{data_views}")
        return query_views

clone = DatabaseCloneObjectSqllite('BI_DB_test')

#clone_views = clone.clone_objects('table')
clone_views = clone.clone_objects('view')

print(f"clone_views:{clone_views}")
