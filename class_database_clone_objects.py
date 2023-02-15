"""class_database_clone_objects.py"""
import pandas as pd
from sqlalchemy import create_engine
from class_config import Config
from class_base_class import BaseClass

cfg = Config()

class DatabaseCloneObjectSqllite(BaseClass):
    """DatabaseCloneObjectSqllite: clona objetos"""
    __tagname__ = ""
    __source_url__ = ""
    __from__ = ""
    __to__ = ""

    def __init__(self,__from__,__to__,__log_active__):
        """__init__"""
        super().__init__(__log_active__)
        #base = 'BRAHMS1P'#TODO: CAMBIAR A INSTALL, y apuntar a INSTALL_EXPORT_HISTORY.db
        self.__source_url__ = f"sqlite:///{cfg.get_par('out_path')}/{__from__}_EXPORT_HISTORY.db"
        self.__target_url__ = f"sqlite:///{cfg.get_par('out_path')}/{__to__}_EXPORT_HISTORY.db"
        self.__from__ = __from__
        self.__to__ = __to__

    def get_engine_source(self):
        """get_engine_source"""
        engine = create_engine(self.__source_url__)
        return engine

    def get_engine_target(self):
        """get_engine_target"""
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
        """clone_objects: will clone the object structures
        from the source to the target, both SQLLITE
        __type__ can be a
        """
        status = "OK"
        self.art_msg('cloning')
        self._log(f'CLONING OBJECTS:TYPE:{__type__}')
        query_objects=f"""
            SELECT 
            NAME,SQL
            FROM 
                sqlite_master  
            WHERE 
                type='{__type__}' 
                --AND name='v_HISTORY'
            """
        data_objects = self.execute_sql_source(query_objects)
        for _, row in data_objects.iterrows():
            row = dict(row)
            name = row['name']
            sql = row['sql']
            #self._log()
            self._log(f"CLONING:name:{name}")
            #self._log(f"sql:{sql}")

            sql = sql.replace('CREATE TABLE ','CREATE TABLE IF NOT EXISTS ')
            sql = sql.replace('CREATE VIEW ','CREATE VIEW IF NOT EXISTS ')

            self.execute_sql_target_without_result(sql)

        #self._log(f"data_views:{data_views}")
        return status

#clone = DatabaseCloneObjectSqllite('BI_DB_test')
#clone_views = clone.clone_objects('table')
#clone_views = clone.clone_objects('view')
#print(f"clone_views:{clone_views}")
