"""class_history_report_type_sqlserver.py"""
"""class_history_report.py"""
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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
# This Python script can Generate the history report of databases
# that means: generates the database history by year/month by Owner/table
# such as the project/owner/or database, this need a user to enter inside the engine
# to connect: the configuration of this, is made by encripted SQLALCHEMY URL
# to prevent: the url with the user and pass needs to be encripted before the use

#import warnings
import pandas as pd
from sqlalchemy import create_engine
from cryptography.fernet import Fernet
import sqlalchemy
from class_config import Config
from class_base_class import BaseClass
from class_database_clone_objects import DatabaseCloneObjectSqllite
#TODO:move to html generator
from class_history_charts import HistoryCharts

cfg = Config("NO_INICIALIZADO",'NO_INICIALIZADO')

class HistoryReportTypeSQLServer():
    """Procesa la historia de servidor"""
    __source_url__ = None
    __cfg__ = None


    def __init__(self,cfg__):
        #self._log('DEPRECATED:HistoryReport:__init__')
        report_name__ = cfg__.get_cfg('report_name')
        #__flavor__ =
        self.__cfg__ = cfg__
        #super().__init__(cfg__.get_cfg('log_active'))
        target_db_name = f'{report_name__}_EXPORT_HISTORY.db'

        self.report_name__ = report_name__
        self.__flavor__ = cfg__.get_cfg('sql_flavor')
        self.__target_url__ = f"sqlite:///{cfg__.get_cfg('out_path')}/{target_db_name}"

        fernet = Fernet(cfg__.get_cfg('crkey'))
        self.__source_url__ = fernet.decrypt(cfg__.get_encripted_source_url()).decode()

    def _log(self,var):
        """method _log"""#self.__cfg__.get_cfg('log_active')|self.__log_active__
        if self.__cfg__.get_cfg('log_active'):
            print(f"LOG:HistoryReportTypeSQLServer:{var}")

    def get_config(self):
        """get_config"""
        return self.__cfg__

    def get_cfg(self,key):
        """get_cfg"""
        return self.__cfg__.get_cfg(key)

    def get_engine_source(self):
        """method get_engine_source"""
        engine = create_engine(self.__source_url__,pool_timeout=999999)
        return engine

    def target_execute(self,sql_):
        """method target_execute"""
        data = None
        try:
            self._log(f"target_execute:START:{sql_}")
            engine = self.get_engine_target()
            cnx=engine.connect()
            data = cnx.execute(sql_)
        except sqlalchemy.exc.OperationalError:
            self._log(f"target_execute:OperationalError:{sql_}")
        finally:
            #self._log(f"target_execute:finally:{sql_}")
            cnx.close()
            engine.dispose()
        #self._log(f"target_execute:END:{sql_}")
        return data

    def read_sql_query(self,sql_):
        """method read_sql_query"""
        data = None
        cnx = None
        try:
            self._log(f"target_execute_select:START:{sql_}")
            engine = self.get_engine_target()
            cnx=engine.connect()
            data = pd.read_sql_query(sql_, engine)
            #data = cnx.execute(sql_)
        except sqlalchemy.exc.OperationalError:
            self._log(f"target_execute:OperationalError:{sql_}")
        finally:
            #self._log(f"target_execute:finally:{sql_}")
            cnx.close()
            engine.dispose()
        #self._log(f"target_execute:END:{sql_}")
        return data

    def target_execute_select(self,sql_):
        """method target_execute"""
        data = None
        cnx = None
        try:
            self._log(f"target_execute_select:START:{sql_}")
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
        """method target_copy_to_table"""
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
        data.to_csv(self.__cfg__.get_cfg('out_path')+file__,index=False,
            sep=";",decimal=",",mode='a',header=True)
        self._log(f"Export ejecutado:{file__}")
        return data

    def export_metadata_counts(self):
        """function export_metadata_counts"""
     
        self._log("export_metadata_counts:START")
        self._log("export_metadata_counts:Obtaining counts by table")

        parameter_name = self.__flavor__ + '_QUERY_METADATA_COUNTS'
        #sql_  = cfg.get_par(parameter_name)
        sql_  = self.__cfg__.get_query(parameter_name)
        data  = self.execute_sql_source(sql_)
        data = self.add_fecha_to_df(data)
        sql_delete = "DELETE FROM METADATA_COUNTS"
        self.target_execute(sql_delete)
        self.target_copy_to_table(data,'METADATA_COUNTS')
        self._log("export_metadata_counts:END")
        return data



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

    def export_metadata_daily_space(self):
        """function export_metadata_daily_space"""
        self._log("export_metadata_daily_space:START")
        self._log("export_metadata_daily_space:Obtaining space used by table")
        parameter_name = self.__flavor__ + '_QUERY_METADATA_DAILY_SPACE'
        #sql_  = cfg.get_par(parameter_name)
        sql_  = self.__cfg__.get_query(parameter_name)
        data  = self.execute_sql_source(sql_)


        data = self.add_fecha_to_df(data)


        sql_delete = "DELETE FROM METADATA_DAILY_SPACE"
        self.target_execute(sql_delete)
        self.target_copy_to_table(data,'METADATA_DAILY_SPACE')
        self._log("export_metadata_daily_space:END")
        return data

    def export_oracle_alert(self):
        """function export_oracle_alert"""
        self._log("export_oracle_alert:START")
        self._log("export_oracle_alert:Obtaining use of secuences")
        #parameter_name = self.__flavor__ + '_QUERY_METADATA_DAILY_SPACE'
        #sql_  = cfg.get_par(parameter_name)
        sql_  = self.__cfg__.get_query('ORACLE_ALERT_SECUENCES')
        data  = self.execute_sql_source(sql_)
        data = self.add_fecha_to_df(data)
        sql_delete = "DELETE FROM ORACLE_ALERT_SECUENCES"
        self.target_execute(sql_delete)
        self.target_copy_to_table(data,'ORACLE_ALERT_SECUENCES')
        self._log("export_oracle_alert:END")
        return data

    def query_history(self,pars_):
        """function """
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

    def export_history_owner(self,owner_):#TODO:MOVE TO LIST AND SEPARATE PROCESS
        """Exporta historia de las tablas contenidas en un owner"""
        file_csv = f'METADATA_TABLE_DATE_{owner_}.csv'
        ruta_csv = self.__cfg__.get_cfg('out_path') + 'out_METADATA/' + file_csv
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

            self.export_history_table(owner_,table_name_)

    def export_history_table(self,owner_,table):
        """Exporta historia de la tabla en un owner"""
        file_csv = f'METADATA_TABLE_DATE_{owner_}.csv'
        ruta_csv = self.__cfg__.get_cfg('out_path') + 'out_METADATA/' + file_csv
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
                hist__query = self.query_history(pars__)
                self._log(hist__query)
                df_hist = self.execute_sql_source(hist__query)

                if isinstance(df_hist, type(None)):
                    #table doesn't exist or error
                    #self._log(f'COUNT:{df_hist.count()}')
                    self._log(f'ISSUE_NO_TABLE:{owner_}{table}')
                    return False

                empty_history = len(df_hist.index)==0

                if empty_history:#issue NO_HISTORY
                    target = cfg.get_par('TARGET_NAME_ALERT')
                    #data_issue = cfg.get_par('data_issue').copy()
                    data_issue = {}
                    sql_delete = f"""
                    DELETE FROM {target} where OWNER = '{owner_}' and TABLE_NAME='{table}'
                    """
                    self.target_execute(sql_delete)
                    data_issue['TABLE_NAME']=table_name_
                    data_issue['OWNER']=owner_
                    data_issue['ALERT_TYPE']='NO_HISTORY'
                    data_issue['VALUE']=0
                    data_issue = self.add_fecha_to_df(data_issue)
                    df_hist = pd.DataFrame([data_issue])
                    self.target_copy_to_table(df_hist,target)
                    self._log(f'LEN:{len(df_hist.index)}')

                if not empty_history:
                    target = cfg.get_par('TARGET_NAME')
                    sql_delete = f"""
                    DELETE FROM {target} where OWNER = '{owner_}' and TABLE_NAME='{table}'
                    """
                    self.target_execute(sql_delete)
                    df_hist = self.add_fecha_to_df(df_hist)
                    self.target_copy_to_table(df_hist,target)

                return True

    def process_owner(self,owner_):
        """Exporta la metadata de un owner y luego genera la historia usando esa metadata"""
        self.export_metadata_table_date(owner_)
        #self.export_history(f'METADATA_{owner_}.csv',2022)
        self.export_history_owner(owner_)

    def alerta(self,dict, alerta_):
        """Exporta la metadata de un owner y luego genera la historia usando esa metadata"""
        self._log(f'INICIO ALERTA:{len(df_hist.index)}')
        fecha__ = self.get_today()
        self._log(f'fecha__:{fecha__}')

        #self.export_metadata_table_date(owner_)
        #self.export_history(f'METADATA_{owner_}.csv',2022)
        #self.export_history_owner(owner_)
        table_name_ = dict['table_name']
        owner_ = dict['owner']
        target = cfg.get_par('TARGET_NAME_ALERTA')
        history_alert = cfg.get_par('history_alert').copy()
        sql_delete = f"""
        --DELETE FROM {target} where OWNER = '{owner_}' and TABLE_NAME='{table}'
        """
        self.target_execute(sql_delete)
        history_alert['TABLE_NAME']= 'TEST_TABLE' #table_name_
        history_alert['OWNER']= 'TEST_OWNER ' #owner_
        history_alert['alerta']='GLOSA DE LA ALERTA' # + alerta_
        history_alert['fecha']=fecha__
        df_hist = pd.DataFrame([history_alert])
        self.target_copy_to_table(df_hist,target)
        self._log(f'ALERTA-LARGO-DATOS:{len(df_hist.index)}')

    def export_metadata_table_date_owners(self):
        """Exporta historia de las tablas contenidas en un owner"""
        owners_ = "SELECT * FROM v_METADATA_OWNER_SPACE"
        df_metadata = self.read_sql_query(owners_)
        print("df_metadata:" + str(type(df_metadata)) )
        #for row in df_metadata:
        for _ , row in df_metadata.iterrows():
            owner_ = row['owner']
            print (row)
            print(f'owner_:{owner_}')
            self.export_metadata_table_date_owner(owner_)

    def export_metadata_table_date_owner(self,owner__):
        """function export_metadata_table_date"""
        self._log(f'export_metadata_table_date:owner__:{owner__}:START')
        #sql_  = cfg.get_par(self.__flavor__ +'_QUERY_METADATA_TABLE_DATE')
        sql_  = self.get_config().get_query(self.__flavor__ +'_QUERY_METADATA_TABLE_DATE')
        sql__ = sql_.format(__OWNER__=owner__)
        self._log(f'export_metadata:sql__:{sql__}')
        data  = self.execute_sql_source(sql__)

        #DELETE METADATA_TABLE_DATE
        sql_delete = f"DELETE FROM METADATA_TABLE_DATE where OWNER = '{owner__}'"
        self.target_execute(sql_delete)
        #INSERT METADATA_TABLE_DATE
        self.target_copy_to_table(data,'METADATA_TABLE_DATE')

        file_csv = f'METADATA_TABLE_DATE_{owner__}.csv'
        ruta_csv = self.__cfg__.get_cfg('out_path') + 'out_METADATA/' + file_csv
        data.to_csv(ruta_csv,index=False,sep=";",decimal=",",header=True)

        self._log(f"export_metadata_table_date:owner__:{owner__}:END")
        return data

    def start(self):
        """function start: creates the metadata of the server"""
        self.export_metadata_daily_space()
        self.export_metadata_counts()
        #self.export_oracle_alert()

        clone = DatabaseCloneObjectSqllite(self.get_config(),'BRAHMS1P',
            self.report_name__, self.__cfg__.get_cfg('log_active'))
        #clone_views = clone.clone_objects('table')
        clone_objects = clone.clone_objects('view')

        #with this creating metadata table of date columns to all owners
        self.export_metadata_table_date_owners()

        #TODO:THIS NEEDS TO CHANGE IN SQLSERVER CASES
        self.export_history_owner('SYSTEM')#to create the basic structure
        #self.export_history_owner('SYS')#to create the basic structure

        self._log(f"clone_objects:{clone_objects}")


    def generate_report_start_step_1(self):
        """generate_report_start_step_1"""
        history_charts= HistoryCharts(self.__cfg__,self.__target_url__)
        history_charts.report_v1_indexes()
        history_charts.report_v1_issues()
        history_charts.report_v1_space()

    def generate_report_start_step_2(self):
        """generate_report_start_step_2"""
        history_charts= HistoryCharts(self.__cfg__,self.__target_url__)
        history_charts.report_v1_history()
        #history_charts.dev_rpt_evolutivo()

    def generate_report_start_step_3(self):
        """generate_report_start_step_3"""
        history_charts= HistoryCharts(self.__cfg__,self.__target_url__)
        history_charts.generate_report_start_step_3()
        history_charts.generate_report_start_step_3_2()
        #history_charts.dev_rpt_evolutivo()

    def dev(self):
        history_charts= HistoryCharts(self.__cfg__,self.__target_url__)
        history_charts.dev()

    def generate_excel_complete(self):
        """generate_excel_complete"""
        filename = f'{self.report_name__}_COMPLETE.xlsx'
        out__ = f'{self.__cfg__.get_cfg("out_path")}{self.report_name__}/{filename}'

        # Create an Excel file from the DataFrame
        writer = pd.ExcelWriter(out__, engine='xlsxwriter')

        __type__ = 'view'
        self._log(f'CLONING OBJECTS:TYPE:{__type__}')
        query_objects=f"""
            SELECT
            NAME,SQL
            FROM 
                sqlite_master
            --WHERE type='{__type__}'
            """
        data_objects = self.read_sql_query(query_objects)
        for _, row in data_objects.iterrows():
            row = dict(row)
            name_view = row['name']
            sql_object = f"SELECT * from {name_view}"

            self.generate_excel_add(writer,name_view,sql_object)
        #self.generate_excel_add(writer,'POSIBLE_BK',"SELECT * from v_HISTORY_YEARS_POSIBLE_BK")
        #self.generate_excel_add(writer,'POSIBLE_BK',"SELECT * from v_HISTORY_YEARS_POSIBLE_BK")
        #self.generate_excel_add(writer,'POSIBLE_BK',"SELECT * from v_HISTORY_YEARS_POSIBLE_BK")
        #self.generate_excel_add(writer,'POSIBLE_BK',"SELECT * from v_HISTORY_YEARS_POSIBLE_BK")
        #self.generate_excel_add(writer,'POSIBLE_BK',"SELECT * from v_HISTORY_YEARS_POSIBLE_BK")
        #self.generate_excel_add(writer,'POSIBLE_BK',"SELECT * from v_HISTORY_YEARS_POSIBLE_BK")
        #self.generate_excel_add(writer,'POSIBLE_BK',"SELECT * from v_HISTORY_YEARS_POSIBLE_BK")
        #datos = self.read_sql_query(sql_)

        #datos = datos.replace(0, None)

        #datos.to_excel(writer, sheet_name='v_HISTORY_YEARS_POSIBLE_BK', index=False)


        writer.save()

    def generate_excel_add(self,writer_,sheet_name_,sql_query):
        """generate_excel_add"""
        datos = self.read_sql_query(sql_query)
        datos = datos.replace(0, None)
        # ADD TO Excel file from the DataFrame
        #writer = pd.ExcelWriter(out__, engine='xlsxwriter')
        datos.to_excel(writer_, sheet_name=sheet_name_, index=False)
        #writer.save()




"""class_history_sqlserver.py"""
SQLSERVER_QUERY_METADATA_DATABASES="""
SQLSERVER_QUERY_METADATA_DATABASES

"""

SQLSERVER_QUERY_METADATA_COUNTS="""
SELECT DB_NAME() AS [owner], 
       t.NAME AS [table_name], 
       SUM(p.rows) AS [num_rows]
FROM sys.tables t 
INNER JOIN sys.partitions p ON t.object_id = p.OBJECT_ID
WHERE t.is_ms_shipped = 0 AND p.index_id IN (1,0)
GROUP BY t.NAME 
ORDER BY num_rows DESC
--SELECT owner,
--       table_name,
--       num_rows
--from sys.dba_tables
--where owner not in ('SYS','SYSTEM','WMSYS','XDB','DBSNMP')
"""

SQLSERVER_QUERY_METADATA_DAILY_SPACE = """
SELECT 'tablespace_name' as tablespace_name,'owner' as owner, 'segment_name' as segment_name, 
'segment_type' as segment_type,
'table_name_normalizado' as table_name_normalizado,
0 as total_mb,
0 as total_mb_index,
0 as total_mb_table,
0 AS cnt_seg  

"""

SQLSERVER_QUERY_METADATA_TABLE_DATE = """
SELECT 
'owner' as owner,
'table_name' as table_name, 
'column_name' as column_name

"""

class HistorySqlServer(HistoryReportTypeSQLServer):
    """Procesa la historia de un servidor SQL SERVER"""
    def __init__(self,cfg):
        """__init__"""
        self.report_name__ = cfg.get_cfg('report_name')
        #self.__engine_url__ = None
        HistoryReportTypeSQLServer.__init__(self,cfg)
        self.set_default_queries()

    def set_default_queries(self):
        """set_default_queries"""
        cfg_=self.get_config()
        cfg_.set_query('SQLSERVER_QUERY_METADATA_DATABASES',SQLSERVER_QUERY_METADATA_DATABASES)
        cfg_.set_query('SQLSERVER_QUERY_METADATA_COUNTS',SQLSERVER_QUERY_METADATA_COUNTS)
        cfg_.set_query('SQLSERVER_QUERY_METADATA_DAILY_SPACE',SQLSERVER_QUERY_METADATA_DAILY_SPACE)
        cfg_.set_query('SQLSERVER_QUERY_METADATA_TABLE_DATE',SQLSERVER_QUERY_METADATA_TABLE_DATE)




"""test_banco_sqlserver.py"""

#from class_config import Config
#from class_history_sqlserver import HistorySqlServer

#FORMAT engine: sql_string='mssql+pyodbc://{username}:{password}@{host_}:{port}/{database}?TrustServerCertificate=yes&driver=SQL+Server'

engine_encriptado = b'gAAAAABkGcNUiRXO-n-qWss_bYQEm8NOc6BiqNdAqP7gBRdvIuUoCMG7QyZXdi3M1kQU7l-N5QDsGBrWiYQH141k17aGfQPEoNJPLumwWoPa7U1BGOkUdIU2vFtPdUbzGDwVkwt5nZU7d5KaarCqhY3-HGYu6ax1Q7gwM0VW0_KUdi5rClv3nYL_kIYr1Buo89OCXh3-oO_j'

name_ = 'sql_ve_paris_20230525_Q001'

STATUS_TEST = "prueba_bbdd"#prueba_bbdd,prueba_reporte
#status_test = "prueba_reporte"#prueba_bbdd,prueba_reporte

if STATUS_TEST=="prueba_bbdd":
    cfg = Config(name_,'SQLSERVER')
    cfg.set_cfg('log_active',True)
    cfg.set_encripted_source_url(engine_encriptado)

    #hp = HistoryOracle(cfg,'BRAHMS1P_stable_002','url_mala')
    hp = HistorySqlServer(cfg)
    hp.start()
    #hp.generate_report_start_step_1()
