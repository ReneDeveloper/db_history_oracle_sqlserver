"""class_database_clone_objects.py"""
import pandas as pd
from sqlalchemy import create_engine
from class_config import Config
from class_base_class import BaseClass

cfg = Config('NO_INICIALIZADO','NO_INICIALIZADO')

class DatabaseCloneObjectSqllite(BaseClass):
    """DatabaseCloneObjectSqllite: 
    to cloning database objects from template to the target database"""
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

    def execute_ddl(self,sql__):
        """function execute_ddl"""
        status = True
        try:
            self._log(f"target_execute:START:{sql__}")
            engine = self.get_engine_target()
            cnx=engine.connect()
            data = cnx.execute(sql__)
        except sqlalchemy.exc.OperationalError:
            self._log(f"target_execute:OperationalError:{sql__}")
            status = False
        finally:
            self._log(f"target_execute:finally:{sql__}")
            cnx.close()
            engine.dispose()
        self._log(f"target_execute:END:{sql__}")
        return status

    def clone_objects(self,__type__):
        """clone_objects: will clone the object structures
        from the source to the target, both SQLLITE
        __type__ can be: 'view' or 'table'
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
            """
        data_objects = self.execute_sql_source(query_objects)
        for _, row in data_objects.iterrows():
            row = dict(row)
            name = row['name']
            sql = row['sql']
            self._log(f"CLONING:name:{name}")
            sql = sql.replace('CREATE TABLE ','CREATE TABLE IF NOT EXISTS ')
            sql = sql.replace('CREATE VIEW ','CREATE VIEW IF NOT EXISTS ')
            self.execute_ddl(sql)
        return status
