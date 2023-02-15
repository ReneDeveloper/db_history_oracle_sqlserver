"""class_history_report.py"""
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# -----------------------------------------------------------------------------
# Project: Database History Report
# Author : Rene Silva
# Email  : rsilcas@outlook.es
# Date   : 2023-02-03
# -----------------------------------------------------------------------------
#
# Description:
#
# This Python script can Generate the history report of databases
# that means: generates the database history by year/month by Owner/table
# such as the project/owner/or database, this need a user to enter inside the engine
# to connect: the configuration of this, is made by SQLALCHEMY URL
# to prevent: the url with the user and pass needs to be encripted before the use

#██╗  ██╗██╗███████╗████████╗ ██████╗ ██████╗ ██╗   ██╗
#██║  ██║██║██╔════╝╚══██╔══╝██╔═══██╗██╔══██╗╚██╗ ██╔╝
#███████║██║███████╗   ██║   ██║   ██║██████╔╝ ╚████╔╝
#██╔══██║██║╚════██║   ██║   ██║   ██║██╔══██╗  ╚██╔╝
#██║  ██║██║███████║   ██║   ╚██████╔╝██║  ██║   ██║
#╚═╝  ╚═╝╚═╝╚══════╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝   ╚═╝

#██████╗ ███████╗██████╗  ██████╗ ██████╗ ████████╗
#██╔══██╗██╔════╝██╔══██╗██╔═══██╗██╔══██╗╚══██╔══╝
#██████╔╝█████╗  ██████╔╝██║   ██║██████╔╝   ██║
#██╔══██╗██╔══╝  ██╔═══╝ ██║   ██║██╔══██╗   ██║
#██║  ██║███████╗██║     ╚██████╔╝██║  ██║   ██║
#╚═╝  ╚═╝╚══════╝╚═╝      ╚═════╝ ╚═╝  ╚═╝   ╚═╝

import pandas as pd
from sqlalchemy import create_engine
from cryptography.fernet import Fernet
import sqlalchemy
import cx_Oracle
from class_config import Config
from class_base_class import BaseClass
from class_database_clone_objects import DatabaseCloneObjectSqllite

cfg = Config()

cx_Oracle.init_oracle_client(lib_dir= cfg.get_par('lib_dir'))

class HistoryReport(BaseClass):
    """Procesa la historia de servidor"""
    def __init__(self,report_name__,__engine__,__flavor__,__log_active__):
        super().__init__(__log_active__)
        self.report_name__ = report_name__
        self.__flavor__ = __flavor__
        fernet = Fernet(cfg.get_par('crkey'))
        self.__source_url__ = fernet.decrypt(__engine__).decode()
        target_db_name = f'{report_name__}_EXPORT_HISTORY.db'
        self.__target_url__ = f"sqlite:///{cfg.get_par('out_path')}/{target_db_name}"

    def get_engine_source(self):
        """function get_engine_source"""
        #oracle_cnx_string = 'oracle+cx_oracle://{username}:{password}@{host_}:{port}/{database}'
        engine = create_engine(self.__source_url__,pool_timeout=999999)
        return engine

    def get_engine_source_no_encriptado(self):
        """function """
        oracle_cnx_string = 'oracle+cx_oracle://{username}:{password}@{host_}:{port}/{database}'
        engine = create_engine(
            oracle_cnx_string.format(
                username = cfg.get_par('username'),
                password = cfg.get_par('password'),
                host_    = cfg.get_par('hostname'),
                port     = cfg.get_par('port'),
                database = cfg.get_par('database')
            )
            ,pool_timeout=99999
        )
        return engine

    def target_execute(self,sql_):
        """function target_execute"""
        data = None
        try:
            self._log(f"target_execute:START:{sql_}")
            engine = self.get_engine_target()
            cnx=engine.connect()
            data = cnx.execute(sql_)
        except sqlalchemy.exc.OperationalError:
            self._log(f"target_execute:OperationalError:{sql_}")
        finally:
            self._log(f"target_execute:finally:{sql_}")
            cnx.close()
            engine.dispose()
        self._log(f"target_execute:END:{sql_}")
        return data

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

    def get_engine_target(self):
        """function get_engine_target"""
        engine = create_engine(self.__target_url__)
        return engine

    def execute_sql_source(self,sql__):
        """function execute_sql_source"""
        engine = self.get_engine_source()
        data   = None
        try:
            data   = pd.read_sql(sql__, engine)
        except sqlalchemy.exc.DatabaseError as exception_database:
            self._log("Exception at Database:")
            self._log("_____________________")
            self._log(str(exception_database))
            self._log("_____________________")
        except Exception as exception_unknown:
            self._log("Exception not at database:")
            self._log("_____________________")
            self._log(str(exception_unknown))
            self._log("_____________________")
        return data

    def execute_sql_source_without_result(self,sql__):
        """function execute_sql_source_without_result"""
        status = "ok"
        engine = self.get_engine_source()
        con = engine.connect()
        con.execute(sql__)
        return status

    def export_sql_csv_header(self, sql__, file__):
        """function export_sql_csv_header will extract sql results and puts in a CSV file"""
        data = self.execute_sql_source(sql__)
        data.to_csv(cfg.get_par('out_path')+file__,index=False,
            sep=";",decimal=",",mode='a',header=True)
        self._log(f"Export ejecutado:{file__}")
        return data

    def export_metadata_counts(self):
        """function export_metadata_counts"""
        #self.art_msg('creating')
        self.art_msg('metadata')
        self._log("export_metadata_counts:START")
        self._log("export_metadata_counts:Obtaining counts by table")

        parameter_name = self.__flavor__ + '_QUERY_METADATA_COUNTS'
        sql_  = cfg.get_par(parameter_name)
        data  = self.execute_sql_source(sql_)
        #TODO:ELIMINAR METADATA_TABLE_DATE
        sql_delete = "DELETE FROM METADATA_COUNTS"
        self.target_execute(sql_delete)
        self.target_copy_to_table(data,'METADATA_COUNTS')
        self._log("export_metadata_counts:END")
        return data

    def export_metadata_daily_space(self):
        """function export_metadata_daily_space"""
        self._log("export_metadata_daily_space:START")
        self._log("export_metadata_daily_space:Obtaining space used by table")
        parameter_name = self.__flavor__ + '_QUERY_METADATA_DAILY_SPACE'
        sql_  = cfg.get_par(parameter_name)
        data  = self.execute_sql_source(sql_)
        #ELIMINAR METADATA_DAILY_SPACE
        sql_delete = "DELETE FROM METADATA_DAILY_SPACE"
        self.target_execute(sql_delete)
        self.target_copy_to_table(data,'METADATA_DAILY_SPACE')
        self._log("export_metadata_daily_space:END")
        return data

    def export_metadata_table_date(self,owner__):
        """function export_metadata_table_date"""
        self._log(f'export_metadata_table_date:owner__:{owner__}:START')
        sql_  = cfg.get_par(self.__flavor__ +'_QUERY_METADATA_TABLE_DATE')
        sql__ = sql_.format(__OWNER__=owner__)
        self._log(f'export_metadata:sql__:{sql__}')
        data  = self.execute_sql_source(sql__)
        #DELETE METADATA_TABLE_DATE
        sql_delete = f"DELETE FROM METADATA_TABLE_DATE where OWNER = '{owner__}'"
        self.target_execute(sql_delete)
        #INSERT METADATA_TABLE_DATE
        self.target_copy_to_table(data,'METADATA_TABLE_DATE')

        file_csv = f'METADATA_TABLE_DATE_{owner__}.csv'
        ruta_csv = cfg.get_par('out_path') + 'out_METADATA/' + file_csv
        data.to_csv(ruta_csv,index=False,sep=";",decimal=",",header=True)
        #f'METADATA_TABLE_DATE_{owner_}.csv'

        self._log(f"export_metadata_table_date:owner__:{owner__}:END")
        return data

    def query_history(self,pars_):
        """function """
        #where_  =
        select_ = f"""--GENERATOR: script tabla:{pars_["_TABLE_NAME"]} 
                SELECT 
                '{pars_["_OWNER"]}' as OWNER,
                '{pars_["_TABLE_NAME"]}' AS TABLE_NAME, 
                EXTRACT(YEAR FROM \"{pars_["_COLUMN_NAME"]}\") as YEAR_,
                EXTRACT(YEAR FROM \"{pars_["_COLUMN_NAME"]}\") * 100 +  EXTRACT(MONTH FROM \"{pars_["_COLUMN_NAME"]}\") AS CODMES, 
                COUNT(1) AS CNT FROM {pars_["_OWNER"]}.{pars_["_TABLE_NAME"]}
                GROUP BY EXTRACT(YEAR FROM \"{pars_["_COLUMN_NAME"]}\"),EXTRACT(YEAR FROM \"{pars_["_COLUMN_NAME"]}\") * 100 +  EXTRACT(MONTH FROM \"{pars_["_COLUMN_NAME"]}\")
                """
        return select_


    def ERROR_query_history_parametrico(self,pars_):
        """function """
        #where_  =
        select_ = cfg.get_par(self.__flavor__ + '_QUERY_HISTORY_COMPLETE')
        _OWNER = pars_["_OWNER"]
        _TABLE_NAME = pars_["_TABLE_NAME"]
        _COLUMN_NAME = pars_["_COLUMN_NAME"]
        print("_OWNER:" + _OWNER)
        select_ = select_.format( __OWNER__=_OWNER,  __TABLE_NAME__=_TABLE_NAME, __COLUMN_NAME__=_COLUMN_NAME)

        print("DEBUG___________________HISTORY PARAMETRICO")
        print(select_)
        print("DEBUG___________________HISTORY PARAMETRICO")

        return select_

#TODO:PASAR A LISTA Y A PROCESO SEPARADO
    def export_history_owner(self,owner_):
        """Exporta historia de las tablas contenidas en un owner"""
        file_csv = f'METADATA_TABLE_DATE_{owner_}.csv'
        ruta_csv = cfg.get_par('out_path') + 'out_METADATA/' + file_csv
        df_metadata = pd.read_csv(ruta_csv, sep=";")

        #sql_metadata = f"select * from v_RESTANTES where owner='{owner_}'"
        #df_metadata = self.target_execute(sql_metadata)
        for _ , row in df_metadata.iterrows():
            #print (df)
            owner_ = row['owner']
            table_name_ = row['table_name']
            #column_name = row['column_name']
            #pars__ = {}
            if table_name_.startswith("BIN$"):
                continue

#            pars__['_OWNER'] = owner_
#            pars__['_TABLE_NAME'] = table_name_
#            pars__['_COLUMN_NAME'] = column_name
#            pars__['year_'] = '2022'
            self.export_history_table(owner_,table_name_)

    def export_history_table(self,owner_,table):
        """Exporta historia de la tabla en un owner"""
        file_csv = f'METADATA_TABLE_DATE_{owner_}.csv'
        ruta_csv = cfg.get_par('out_path') + 'out_METADATA/' + file_csv
        df_metadata = pd.read_csv(ruta_csv, sep=";")

        for _ , row in df_metadata.iterrows():
            #print (df)
            owner_ = row['owner']
            table_name_ = row['table_name']
            column_name = row['column_name']
            pars__ = {}
            #year_ = '2023'
            pars__['_OWNER'] = owner_
            pars__['_TABLE_NAME'] = table_name_
            pars__['_COLUMN_NAME'] = column_name
            #pars__['year_'] = year_

            if table_name_.startswith("BIN$"):#is a BIN table
                #TODO:MARCAR TABLA COMO TABLA EN PAPELERA DE RECICLAJE
                continue




            if table_name_==table:

                #hist__query = self.query_history(pars__)
                print("PARRS::::::::::")
                print(pars__)
                hist__query = self.query_history(pars__)

                self._log(hist__query)
                #df_hist = self.export_sql_csv_header(hist__query,f'HIST__{owner_}__.csv')
                df_hist = self.execute_sql_source(hist__query)

                if isinstance(df_hist, type(None)):
                    #doesn't exist or error

                    #self._log(f'COUNT:{df_hist.count()}')
                    self._log(f'ISSUE_NO_TABLE:{owner_}{table}')
                    return False

                empty_history = len(df_hist.index)==0
                
                if empty_history:#issue NO_HISTORY
                    target = cfg.get_par('TARGET_NAME_ISSUE')
                    data_issue = cfg.get_par('data_issue').copy()
                    sql_delete = f"DELETE FROM {target} where OWNER = '{owner_}' and TABLE_NAME='{table}'"
                    self.target_execute(sql_delete)

                    print("-----------------------DEBUG____________________")
                    print(data_issue)
                    print("-----------------------DEBUG____________________")
                    data_issue['TABLE_NAME']=table_name_
                    data_issue['OWNER']=owner_
                    data_issue['ISSUE']='NO_HISTORY'
                    data_issue['VALUE']=0
                    print(data_issue)
                    print("-----------------------DEBUG__________DATAFRAME________")
                    #columnas = ['OWNER', 'TABLE_NAME', 'ISSUE', 'VALUE']

                    #df_hist = pd.DataFrame.from_dict(data_issue, orient='index')
                    df_hist = pd.DataFrame([data_issue])

                    print("-----------------------DEBUG_______SAVING_________")
                    print(df_hist)
                    #pd.DataFrame.from_dict(data_issue)

                    self.target_copy_to_table(df_hist,target)
                    self._log(f'LEN:{len(df_hist.index)}')

                if not empty_history:
                    target = cfg.get_par('TARGET_NAME')
                    sql_delete = f"DELETE FROM {target} where OWNER = '{owner_}' and TABLE_NAME='{table}'"
                    self.target_execute(sql_delete)
                    self.target_copy_to_table(df_hist,target)

                return True

    def process_owner(self,owner_):
        """Exporta la metadata de un owner y luego genera la historia usando esa metadata"""
        self.export_metadata_table_date(owner_)
        #self.export_history(f'METADATA_{owner_}.csv',2022)
        self.export_history_owner(owner_)

    def start(self):
        """function start: creates the metadata of the server"""
        #self.art_msg('start')
        self.art_msg('database')
        self.art_msg('aging')
        self.export_metadata_daily_space()
        self.export_metadata_counts()
        clone = DatabaseCloneObjectSqllite('BRAHMS1P', self.report_name__, self.__log_active__)
        #clone_views = clone.clone_objects('table')
        clone_objects = clone.clone_objects('view')
        self._log(f"clone_objects:{clone_objects}")
        self.art_msg('created')

    def estimate_all(self):
        """function start: creates the metadata of the server"""
        self.art_msg('estimate_all')

    def process_all(self):
        """function start: creates the metadata of the server"""
        self.art_msg('process_all')
