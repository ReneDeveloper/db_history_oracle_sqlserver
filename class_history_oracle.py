"""class_history_oracle.py"""
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#██╗  ██╗██╗███████╗████████╗ ██████╗ ██████╗ ██╗   ██╗
#██║  ██║██║██╔════╝╚══██╔══╝██╔═══██╗██╔══██╗╚██╗ ██╔╝
#███████║██║███████╗   ██║   ██║   ██║██████╔╝ ╚████╔╝
#██╔══██║██║╚════██║   ██║   ██║   ██║██╔══██╗  ╚██╔╝
#██║  ██║██║███████║   ██║   ╚██████╔╝██║  ██║   ██║
#╚═╝  ╚═╝╚═╝╚══════╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝   ╚═╝

# ██████╗ ██████╗  █████╗  ██████╗██╗     ███████╗
#██╔═══██╗██╔══██╗██╔══██╗██╔════╝██║     ██╔════╝
#██║   ██║██████╔╝███████║██║     ██║     █████╗  
#██║   ██║██╔══██╗██╔══██║██║     ██║     ██╔══╝  
#╚██████╔╝██║  ██║██║  ██║╚██████╗███████╗███████╗
# ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝╚══════╝╚══════╝

# -----------------------------------------------------------------------------
# Project: Database History Report
# Author : René Silva
# Email  : rsilcas@outlook.es
# Date   : 2023-02-03
# Updated: 2023-03-16
# -----------------------------------------------------------------------------
#
# Description:
#
# This Python script generates the history report of one ORACLE instance
# that means: generates the database history by year/month by Owner/Table
# such as the project/owner/or database, this need a user to enter inside the engine
# to connect: the configuration of this, is made by encripted SQLALCHEMY URL
# to prevent: the url with the user and pass needs to be encripted before the use

from class_history_report import HistoryReport
from class_config import Config

ORACLE_QUERY_METADATA_COUNTS="""
SELECT owner,
       table_name,
       num_rows
from sys.dba_tables
where owner not in ('SYS','SYSTEM','WMSYS','XDB','DBSNMP')
"""

ORACLE_QUERY_METADATA_DAILY_SPACE = """
SELECT tablespace_name,owner, segment_name, segment_type,table_name_normalizado,
SUM(bytes)/1024/1024 total_mb,
SUM(CASE WHEN segment_type='INDEX' THEN bytes ELSE 0 END)/1024/1024 total_mb_index,
SUM(CASE WHEN segment_type='TABLE' THEN bytes ELSE 0 END)/1024/1024 total_mb_table,
COUNT(1) AS cnt_seg  
FROM 
    (
    SELECT segments_.tablespace_name,segments_.owner, segments_.segment_name, segments_.segment_type,
    CASE 
        WHEN segments_.segment_type = 'INDEX' THEN indexes_.table_name 
        WHEN segments_.segment_type = 'TABLE' THEN segments_.segment_name
        WHEN segments_.segment_type = 'LOBSEGMENT' THEN lobs_.table_name
        WHEN segments_.segment_type = 'TABLE PARTITION' THEN partitions_.table_name
        WHEN segments_.segment_type = 'INDEX PARTITION' THEN index_partitions_.table_name
        
        --HERE ADD OTHER CASES
    END AS table_name_normalizado,
    segments_.bytes
    FROM dba_segments segments_ 
    LEFT JOIN dba_indexes indexes_ ON segments_.segment_type='INDEX' AND indexes_.index_name = segments_.segment_name AND indexes_.owner = segments_.owner 
    LEFT JOIN dba_lobs lobs_ ON segments_.segment_type='LOBSEGMENT' AND segments_.owner = lobs_.owner AND segments_.segment_name = lobs_.segment_name AND segments_.tablespace_name = lobs_.tablespace_name
    LEFT JOIN  (
                SELECT --*
                DISTINCT a.tablespace_name, a.table_owner, a.table_name
                FROM   dba_tab_partitions a 
                --where owner = 'DMNORMA_ADM'  
                )  partitions_ ON segments_.segment_type='TABLE PARTITION' AND segments_.owner = partitions_.table_owner AND segments_.segment_name = partitions_.table_name AND segments_.tablespace_name = partitions_.tablespace_name
    LEFT JOIN ( --SE CONSTRUYE UN SUBQUERY DADO QUE UN INDICE PUEDE TENER N PARTICIONES DE INDICES
                SELECT DISTINCT a.tablespace_name, a.index_owner, a.index_name, b.table_name
                FROM  dba_ind_partitions a
                LEFT JOIN DBA_INDEXES b on a.index_owner=b.owner  and a.index_name = b.index_name --FAIL ADDING THIS BECAUSE EXISTS TABLESPACE_NAME NULL IN DBA_INDEXES
              ) index_partitions_ ON segments_.segment_type='INDEX PARTITION' AND indexes_.owner = index_partitions_.index_owner AND indexes_.index_name = index_partitions_.index_name AND indexes_.tablespace_name = index_partitions_.tablespace_name
    
        --HERE ADD OTHER CASES
    ) RESULT
--where segment_type = 'TABLE PARTITION' AND owner = 'DMNORMA_ADM'
GROUP BY tablespace_name,OWNER, segment_name,segment_type,table_name_normalizado
ORDER BY segment_type
"""

ORACLE_QUERY_METADATA_TABLE_DATE = """
SELECT owner,table_name, column_name
FROM DBA_TAB_COLUMNS c WHERE (owner, table_name, column_id) in (
    SELECT owner,table_name, min(column_id) as min_column_id 
    from DBA_TAB_COLUMNS c WHERE 
    owner in ('{__OWNER__}') and 
    data_type in ('DATE')
    group by owner,table_name
)
order by owner,table_name,column_name
"""

class HistoryOracle(HistoryReport):
    """Procesa la historia de un servidor ORACLE"""
    def __init__(self,cfg):
        """__init__"""
        self.report_name__ = cfg.get_cfg('report_name')
        #self.__engine_url__ = None
        HistoryReport.__init__(self,cfg)
        self.set_default_queries()

    def set_default_queries(self):
        """set_default_queries"""
        cfg_=self.get_config()
        cfg_.set_query('ORACLE_QUERY_METADATA_COUNTS',ORACLE_QUERY_METADATA_COUNTS)
        cfg_.set_query('ORACLE_QUERY_METADATA_DAILY_SPACE',ORACLE_QUERY_METADATA_DAILY_SPACE)
        cfg_.set_query('ORACLE_QUERY_METADATA_TABLE_DATE',ORACLE_QUERY_METADATA_TABLE_DATE)

    def oracle_table_cost (self,owner_,table_name_,column_name_):#TODO:OBTAIN TOTAL COST IN ORACLE
        """Obtiene oracle_table_cost"""
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
        data = self.execute_sql_source("select * FROM PLAN")

        salida = {}
        #data = self.execute_sql_source('select * from table(dbms_xplan.display)')

        self._log(data)
        """Inicializa, con el inicio del nombre de los archivos, ej: caso HIST"""
