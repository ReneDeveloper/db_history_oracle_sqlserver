"""configuration class"""
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#██╗  ██╗██╗███████╗████████╗ ██████╗ ██████╗ ██╗   ██╗
#██║  ██║██║██╔════╝╚══██╔══╝██╔═══██╗██╔══██╗╚██╗ ██╔╝
#███████║██║███████╗   ██║   ██║   ██║██████╔╝ ╚████╔╝
#██╔══██║██║╚════██║   ██║   ██║   ██║██╔══██╗  ╚██╔╝
#██║  ██║██║███████║   ██║   ╚██████╔╝██║  ██║   ██║
#╚═╝  ╚═╝╚═╝╚══════╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝   ╚═╝

# ██████╗ ██████╗ ███╗   ██╗███████╗██╗ ██████╗ 
#██╔════╝██╔═══██╗████╗  ██║██╔════╝██║██╔════╝ 
#██║     ██║   ██║██╔██╗ ██║█████╗  ██║██║  ███╗
#██║     ██║   ██║██║╚██╗██║██╔══╝  ██║██║   ██║
#╚██████╗╚██████╔╝██║ ╚████║██║     ██║╚██████╔╝
# ╚═════╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝     ╚═╝ ╚═════╝ 

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
# This Python script have the minimal parameters to obtain the history report of database
# that means: generates the database history by year/month by Owner/table using parametric queries
#             those queries are different to different SQL flavors and engines
#             for example for SQLSERVER obtain the METADATA from one way but in Oracle in other way
#             to obtain different behaviors on the process, the config class will have "flavor"
#             at this time the flavor is "ORACLE" or "SQLSERVER"
# such as the project/owner/or database, this need a user to enter inside the engine
# to connect: the configuration of this, is made by encripted SQLALCHEMY URL
# to prevent: the url with the user and pass needs to be encripted before the use
                                               
import warnings
from class_base_class import BaseClass

pars_ = {}

def get_parameter(par_):
    warnings.warn("the get_parameter module is deprecated")
    """Get config parameter"""
    return pars_[par_]

class Config(BaseClass):
    """class to obtain config parameters"""
    
    __parameters__ = {}
    __queries__ = {}
    __log_active__ = False
    __encripted_source_url__ = None

    def __init__(self,report_name__,sql_flavor__):
        super().__init__(True)
        self.set_cfg('report_name',report_name__)
        self.set_cfg('sql_flavor',sql_flavor__)
        self.set_default_values()

    def get_par(self, par__):
        """Get config parameter"""
        warnings.warn("DEPRECATED: use get_cfg('par__')")
        self._log('DEPRECATED:use Config.get_cfg')
        return get_parameter(par__)

    def set_par(self, par__, val__):
        """Set config parameter"""
        warnings.warn("DEPRECATED: use set_cfg('par__','val__')")
        self._log('DEPRECATED:Config.set_par')
        pars_[par__]=val__

    def set_encripted_source_url(self, par__):
        """set_encripted_source_url"""
        self.__encripted_source_url__ = par__

    def get_encripted_source_url(self):
        """get_encripted_source_url"""
        return self.__encripted_source_url__

    def set_cfg(self, cfg, val):
        """method set_cfg: Set config parameter"""
        self.__parameters__[cfg]=val

    def get_cfg(self, cfg_key):
        """method get_cfg: get config parameter"""
        return self.__parameters__[cfg_key]

    def set_query(self, query_key, query):
        """method set_query: set query parameter asociated to the flavor"""
        self.__queries__[query_key]=query

    def get_query(self, query_key):
        """method get_query: get query parameter"""
        return self.__queries__[query_key]

    def set_default_values(self):
        """method set_default_values: Set config default parameters"""
        self.set_cfg("crkey",b'ixg0tK8e3dlzVT5NBzMGqEgfkaeRfsPFc76wZxAaD-0=')
        self.set_cfg("lib_dir",'C:/Users/rcastillosi/Downloads/PORTABLE/instantclient_21_7')
        self.set_cfg("out_path",'C:/Users/rcastillosi/__SQL_DATABASE_STATS__/__EXPORT_DATA__/')

    def get_parameter_dict(self):#TODO:POR SEGURIDAD QUITAR ESTE METODO, SE USA SOLO PARA EFECTOS DE DEVELOP
        return self.__parameters__
        
#parametro crkey
pars_["crkey"]=b'ixg0tK8e3dlzVT5NBzMGqEgfkaeRfsPFc76wZxAaD-0='

#parametro de bbdd source
###DEPRECATED###pars_["username"]="username"
###DEPRECATED###pars_["password"]="password"
###DEPRECATED###pars_["hostname"]="hostname"
###DEPRECATED###pars_["port"]="1521"
###DEPRECATED###pars_["database"]="database"
#parametros de paths
#pars_["lib_dir"]="C:/Users/rsilc/Downloads/PORTABLE/instantclient_21_9/"#win11
pars_["lib_dir"]="C:/Users/rcastillosi/Downloads/PORTABLE/instantclient_21_7"#win10

###DEPRECATED###pars_["out_dir"]="C:/Users/rcastillosi/__SQL_DATABASE_STATS__/__EXPORT_DATA__/"
#parametros de bbdd TARGET
pars_["TARGET_NAME"]="HISTORY_REPORT"
pars_["TARGET_NAME_ISSUE"]="HISTORY_ISSUE"

#pars_["out_path"]="C:/Users/rcastillosi/__SQL_DATABASE_STATS__/__EXPORT_DATA__/"
###DEPRECATED####pars_["out_sqllit_file"]='sqlite:///{out_path}/SQLLITE_EXPORT_HISTORY_{out_owner}.db'
###DEPRECATED####pars_["out_sqllite"]=f'sqlite:///{pars_["out_path"]}/SQLLITE_EXPORT_HISTORY.db'
###DEPRECATED#####parametros para nombres de archivos
###DEPRECATED#####pars_["pre_metadata"]="METADATA_"

data_issue = {
'OWNER': ['THE_OWNER'], 'TABLE_NAME': ['THE_TABLE'], 'ISSUE': ['THE_ISSUE'], 
'VALUE': [0] 
}

pars_["data_issue"]=data_issue

# ██████╗ ██████╗  █████╗  ██████╗██╗     ███████╗
#██╔═══██╗██╔══██╗██╔══██╗██╔════╝██║     ██╔════╝
#██║   ██║██████╔╝███████║██║     ██║     █████╗  
#██║   ██║██╔══██╗██╔══██║██║     ██║     ██╔══╝  
#╚██████╔╝██║  ██║██║  ██║╚██████╗███████╗███████╗
# ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝╚══════╝╚══════╝



#TODO:DEPRECATED
warnings.warn('DEPRECATED:pars_["ORACLE_QUERY_METADATA_COUNTS"]')
pars_["ORACLE_QUERY_METADATA_COUNTS"] = """
SELECT owner,
       table_name,
       num_rows
from sys.dba_tables
where owner not in ('SYS','SYSTEM','WMSYS','XDB','DBSNMP')
"""

#TODO:DEPRECATED
warnings.warn('DEPRECATED:pars_["ORACLE_QUERY_METADATA_DAILY_SPACE"]')
pars_["ORACLE_QUERY_METADATA_DAILY_SPACE"] = """
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

#TODO:DEPRECATED
warnings.warn('DEPRECATED:pars_["ORACLE_QUERY_METADATA_TABLE_DATE"]')
pars_["ORACLE_QUERY_METADATA_TABLE_DATE"] = """
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

#TODO:FAILED
pars_["ORACLE_QUERY_HISTORY_COMPLETE"] = """
SELECT 
'{__OWNER__}' as OWNER,
'{__TABLE_NAME__}' AS TABLE_NAME, 
EXTRACT(YEAR FROM \"{__COLUMN_NAME__}\") as YEAR_,
EXTRACT(YEAR FROM \"{__COLUMN_NAME__}\") * 100 +  EXTRACT(MONTH FROM \"{__COLUMN_NAME__}\") AS CODMES, 
COUNT(1) AS CNT FROM {__OWNER__}.{__TABLE_NAME__}
--WHERE {__COLUMN_NAME__} BETWEEN 
--    to_date( '{pars_["year_"]}-01-01 00:00:00', 'yyyy-mm-dd hh24:mi:ss' ) AND 
--    to_date( '{pars_["year_"]}-12-31 23:59:59', 'yyyy-mm-dd hh24:mi:ss' )
GROUP BY EXTRACT(YEAR FROM \"{__COLUMN_NAME__}\"),EXTRACT(YEAR FROM \"{__COLUMN_NAME__}\") * 100 +  EXTRACT(MONTH FROM \"{__COLUMN_NAME__}\")
                            """

#███████╗ ██████╗ ██╗         ███████╗███████╗██████╗ ██╗   ██╗███████╗██████╗
#██╔════╝██╔═══██╗██║         ██╔════╝██╔════╝██╔══██╗██║   ██║██╔════╝██╔══██╗
#███████╗██║   ██║██║         ███████╗█████╗  ██████╔╝██║   ██║█████╗  ██████╔╝
#╚════██║██║▄▄ ██║██║         ╚════██║██╔══╝  ██╔══██╗╚██╗ ██╔╝██╔══╝  ██╔══██╗
#███████║╚██████╔╝███████╗    ███████║███████╗██║  ██║ ╚████╔╝ ███████╗██║  ██║
#╚══════╝ ╚══▀▀═╝ ╚══════╝    ╚══════╝╚══════╝╚═╝  ╚═╝  ╚═══╝  ╚══════╝╚═╝  ╚═╝

#TODO:NEED A QUERY THAT COUNT THE TOTAL OF ALL TABLES
pars_["SQLSERVER_QUERY_METADATA_COUNTS"] = """
select owner,
       table_name,
       num_rows
from sys.dba_tables
where owner not in ('SYS','SYSTEM','WMSYS','XDB','DBSNMP')
                            """
#TODO:NEED A QUERY OF SPACES OF SQLSERVER
pars_["SQLSERVER_QUERY_METADATA_DAILY_SPACE"] = """

set nocount on

USE [master]
--GO

if object_id('tempdb..#temp') is not null drop table #temp

CREATE TABLE #temp(
    rec_id      int IDENTITY (1, 1),
    db sysname, 
    TableName   varchar(128),
    SchemaName varchar(128),
    RowCounts   bigint,
    TotalSpaceMB    decimal(15,2),  
    UsedSpaceMB decimal(15,2),
    UnusedSpaceMB   decimal(15,2)   )


exec sp_MSforeachdb 'USE [?]; 
insert into #temp (db, TableName, SchemaName, RowCounts, TotalSpaceMB, UsedSpaceMB, UnusedSpaceMB)
SELECT  
''?'' as db,    
t.NAME AS TableName,    
s.Name AS SchemaName,    
p.rows AS RowCounts,    
SUM(a.total_pages) * 8/1024 AS TotalSpaceMB,     
SUM(a.used_pages) * 8/1024 AS UsedSpaceMB, 
(SUM(a.total_pages) - SUM(a.used_pages)) * 8/1024 AS UnusedSpaceMB 
FROM     sys.tables t 
INNER JOIN      sys.indexes i ON t.OBJECT_ID = i.object_id
INNER JOIN    sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id 
INNER JOIN     sys.allocation_units a ON p.partition_id = a.container_id
LEFT OUTER JOIN     sys.schemas s ON t.schema_id = s.schema_id 
WHERE    p.rows > 0 AND t.is_ms_shipped = 0    AND i.OBJECT_ID > 255 
GROUP BY     t.Name, s.Name, p.Rows 
ORDER BY p.rows DESC' ;

select db, TableName, SchemaName, RowCounts, TotalSpaceMB, UsedSpaceMB, UnusedSpaceMB
from #temp
order by db, SchemaName, TableName




                            """
#TODO:NEED A QUERY 
pars_["SQLSERVER_QUERY_METADATA_TABLE_DATE"] = """
select owner,table_name, column_name
from DBA_TAB_COLUMNS c WHERE (owner, table_name, column_id) in (
    select owner,table_name, min(column_id) as min_column_id 
    from DBA_TAB_COLUMNS c WHERE 
    owner in ('{__OWNER__}') and 
    data_type in ('DATE')
    group by owner,table_name
)
order by owner,table_name,column_name
                            """




######################################################3
###################################################3333
#REPORTS
######################################################3
###################################################3333

pars_['REPORT_img_001_table_vs_index']="""
SELECT owner as Grupo, total_mb_table as mb_table, total_mb_index as mb_index FROM v_METADATA_OWNER_SPACE ORDER BY total_mb_table desc
"""




































































































