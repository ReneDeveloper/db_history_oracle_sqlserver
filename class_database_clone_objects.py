"""class_database_clone_objects.py"""
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# -----------------------------------------------------------------------------
# Project: Database History Report
# Author : René Silva
# Email  : rsilcas@outlook.es
# Date   : 2023-02-03
# Updated: 2023-02-21
# -----------------------------------------------------------------------------
#
# Description:
#
# This Python script clones the SQLLITE Database Structure
# that means: generates the views on the target SQLLITE database from template database

import pandas as pd
from sqlalchemy import create_engine
from class_config import Config
from class_base_class import BaseClass

#cfg = Config('NO_INICIALIZADO','NO_INICIALIZADO')

class DatabaseCloneObjectSqllite(BaseClass):
    """DatabaseCloneObjectSqllite: 
    to cloning database objects from template to the target database"""
    __tagname__ = ""
    __source_url__ = ""
    __from__ = ""
    __to__ = ""

    def __init__(self,cfg_,__from__,__to__,__log_active__):
        """__init__"""
        super().__init__(__log_active__)
        #base = 'BRAHMS1P'#TODO: CAMBIAR A INSTALL, y apuntar a INSTALL_EXPORT_HISTORY.db
        self.__source_url__ = f"sqlite:///{cfg_.get_cfg('out_path')}/{__from__}_EXPORT_HISTORY.db"
        self.__target_url__ = f"sqlite:///{cfg_.get_cfg('out_path')}/{__to__}_EXPORT_HISTORY.db"
        self.__from__ = __from__
        self.__to__ = __to__

    def get_engine_source(self):
        """method get_engine_source"""
        engine = create_engine(self.__source_url__)
        return engine

    def get_engine_target(self):
        """method get_engine_target"""
        engine = create_engine(self.__target_url__)
        return engine

    def execute_sql_source(self,sql__):
        """method execute_sql_source"""
        engine = self.get_engine_source()
        data   = pd.read_sql(sql__, engine)
        return data

    def execute_ddl(self,sql__):
        """method execute_ddl"""
        status = True
        try:
            #self._log(f"target_execute:START:{sql__}")
            engine = self.get_engine_target()
            cnx=engine.connect()
            data = cnx.execute(sql__)
        except sqlalchemy.exc.OperationalError:
            self._log(f"target_execute:OperationalError:{sql__}")
            status = False
        finally:
            #self._log(f"target_execute:finally:{sql__}")
            cnx.close()
            engine.dispose()
        self._log(f"target_execute:END:{sql__}")
        return status

    def clone_objects(self,__type__):
        """method  clone_objects: 
        will clone the object structures
        from the source to the target, both in SQLLITE
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
            sql = sql.replace('CREATE VIEW ' ,'CREATE VIEW  IF NOT EXISTS ')
            self.execute_ddl(sql)
        return status


    def target_copy_to_table(self,data,table_name):
        """function"""
        status = "ok"
        self._log(f"target_copy_to_table:START:{table_name}")
        try:
            engine = self.get_engine_target()
            cnx = engine.connect()
            data.to_sql(f'{table_name}',if_exists='append',con=cnx,index=False,chunksize=100)
            #data.to_sql(f'{table_name}',if_exists='replace',con=cnx,index=False,chunksize=100)
            del data
            self._log(f"target_copy_to_table:END:{table_name}")
        finally:
            cnx.close()
            engine.dispose()
        return status


    def add_fecha_to_df(self,data):
        """add_fecha_to_df"""
        self._log("add_fecha_to_df:START")
        fecha = self.get_today()
        data['fecha']=fecha
        return data

    def get_today(self):
        """get_today"""

        from datetime import datetime
        fecha = datetime.today().strftime('%Y%m%d')
        return fecha

        #print(f"fecha:{fecha}")

    def move_history(self):
        """method  move_history: 
        will move the history from source to the target, both SQLLITE
        """
        status = "OK"
        #self.art_msg('move_history')
        table_name_ = 'history_report'
        self._log(f'CLONING move_history:table_name_:{table_name_}')
        query_objects=f"""
            SELECT      * from {table_name_}
            """
        data = self.execute_sql_source(query_objects)
        data = self.add_fecha_to_df(data)

        self.target_copy_to_table(data,table_name_)

        return status