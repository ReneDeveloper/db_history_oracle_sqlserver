"""class_history_oracle.py"""
from class_history_report import HistoryReport
from class_config import Config

class HistoryOracle(HistoryReport):
    """Procesa la historia de un servidor ORACLE"""
    def __init__(self,report_name__,__engine__):
        """Inicializa"""
        self.report_name__ = None
        self.__engine_url__ = None
        HistoryReport.__init__(self,report_name__,__engine__,'ORACLE',__log_active__=True)


    def oracle_table_cost (self,owner_,table_name_,column_name_):
        """Obtiene """
        pars__ = {}
        #year_ = '2023'
        pars__['_OWNER'] = owner_
        pars__['_TABLE_NAME'] = table_name_
        pars__['_COLUMN_NAME'] = column_name_
        pars__['year_'] = -1
        query_ = self.query_history(pars__)
        self._log(f'test:query_:{query_}')
        self.execute_sql_source_without_result(f'DELETE FROM PLAN FOR {query_}')
        self.execute_sql_source_without_result(f'EXPLAIN PLAN FOR {query_}')
        
        #data = self.execute_sql_source("select * from table(dbms_xplan.display(null, null, 'SERIAL'))")
        data = self.execute_sql_source("select * PLAN")
        
        salida = {}
        #data = self.execute_sql_source('select * from table(dbms_xplan.display)')
    

        self._log(data)
        """Inicializa, con el inicio del nombre de los archivos, ej: caso HIST"""


