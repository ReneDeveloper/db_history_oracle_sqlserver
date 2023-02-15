# db_history_oracle
Database History in Oracles

Updated: 20230215
Stable release: 20230215

Example of use:

from class_config import Config
from class_history_oracle import HistoryOracle

#FORMAT engine: oracle_string='oracle+cx_oracle://{username}:{password}@{host_}:{port}/{database}'_
engine_cript = b'BINARY ENCRIPTED ENGINE URL' #USE class_cripto.py to create a cripted engine url
hp = HistoryOracle('BI_DB_automatico_08',engine_cript)

hp.start()
hp.process_owner('DEV_BIPLATFORM')#this is the name of some OWNER in Oracle database


Example of execution:20230215 in a Oracle Instance


██████╗  █████╗ ████████╗ █████╗ ██████╗  █████╗ ███████╗███████╗
██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗██╔══██╗██╔══██╗██╔════╝██╔════╝
██║  ██║███████║   ██║   ███████║██████╔╝███████║███████╗█████╗  
██║  ██║██╔══██║   ██║   ██╔══██║██╔══██╗██╔══██║╚════██║██╔══╝  
██████╔╝██║  ██║   ██║   ██║  ██║██████╔╝██║  ██║███████║███████╗
╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚═════╝ ╚═╝  ╚═╝╚══════╝╚══════╝


 █████╗  ██████╗ ██╗███╗   ██╗ ██████╗ 
██╔══██╗██╔════╝ ██║████╗  ██║██╔════╝ 
███████║██║  ███╗██║██╔██╗ ██║██║  ███╗
██╔══██║██║   ██║██║██║╚██╗██║██║   ██║
██║  ██║╚██████╔╝██║██║ ╚████║╚██████╔╝
╚═╝  ╚═╝ ╚═════╝ ╚═╝╚═╝  ╚═══╝ ╚═════╝ 

LOG:export_metadata_daily_space:START
LOG:export_metadata_daily_space:Obtaining space used by table
LOG:target_execute:START:DELETE FROM METADATA_DAILY_SPACE
LOG:target_execute:OperationalError:DELETE FROM METADATA_DAILY_SPACE
LOG:target_execute:finally:DELETE FROM METADATA_DAILY_SPACE
LOG:target_execute:END:DELETE FROM METADATA_DAILY_SPACE
LOG:target_copy_to_table:START:METADATA_DAILY_SPACE
LOG:target_copy_to_table:END:METADATA_DAILY_SPACE
LOG:export_metadata_daily_space:END

███╗   ███╗███████╗████████╗ █████╗ ██████╗  █████╗ ████████╗ █████╗ 
████╗ ████║██╔════╝╚══██╔══╝██╔══██╗██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗
██╔████╔██║█████╗     ██║   ███████║██║  ██║███████║   ██║   ███████║
██║╚██╔╝██║██╔══╝     ██║   ██╔══██║██║  ██║██╔══██║   ██║   ██╔══██║
██║ ╚═╝ ██║███████╗   ██║   ██║  ██║██████╔╝██║  ██║   ██║   ██║  ██║
╚═╝     ╚═╝╚══════╝   ╚═╝   ╚═╝  ╚═╝╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝

LOG:export_metadata_counts:START
LOG:export_metadata_counts:Obtaining counts by table
LOG:target_execute:START:DELETE FROM METADATA_COUNTS
LOG:target_execute:OperationalError:DELETE FROM METADATA_COUNTS
LOG:target_execute:finally:DELETE FROM METADATA_COUNTS
LOG:target_execute:END:DELETE FROM METADATA_COUNTS
LOG:target_copy_to_table:START:METADATA_COUNTS
LOG:target_copy_to_table:END:METADATA_COUNTS
LOG:export_metadata_counts:END

 ██████╗██╗      ██████╗ ███╗   ██╗██╗███╗   ██╗ ██████╗ 
██╔════╝██║     ██╔═══██╗████╗  ██║██║████╗  ██║██╔════╝ 
██║     ██║     ██║   ██║██╔██╗ ██║██║██╔██╗ ██║██║  ███╗
██║     ██║     ██║   ██║██║╚██╗██║██║██║╚██╗██║██║   ██║
╚██████╗███████╗╚██████╔╝██║ ╚████║██║██║ ╚████║╚██████╔╝
 ╚═════╝╚══════╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝╚═╝  ╚═══╝ ╚═════╝ 

LOG:CLONING OBJECTS:TYPE:view
LOG:CLONING:name:v_METADATA_TABLE_SPACE
LOG:CLONING:name:v_HISTORY
LOG:CLONING:name:v_RESTANTES
LOG:CLONING:name:v_METADATA
LOG:CLONING:name:v_HISTORY_YEARS_QTY
LOG:CLONING:name:v_HISTORY_YEARS_MB
LOG:CLONING:name:v_HISTORY_YEARS_MB_OWNER
LOG:CLONING:name:v_METADATA_OWNER_SPACE
LOG:CLONING:name:v_RESTANTES_OWNERS
LOG:clone_objects:OK

 ██████╗██████╗ ███████╗ █████╗ ████████╗███████╗██████╗ 
██╔════╝██╔══██╗██╔════╝██╔══██╗╚══██╔══╝██╔════╝██╔══██╗
██║     ██████╔╝█████╗  ███████║   ██║   █████╗  ██║  ██║
██║     ██╔══██╗██╔══╝  ██╔══██║   ██║   ██╔══╝  ██║  ██║
╚██████╗██║  ██║███████╗██║  ██║   ██║   ███████╗██████╔╝
 ╚═════╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝   ╚═╝   ╚══════╝╚═════╝ 

LOG:export_metadata_table_date:owner__:LINEACRD:START
LOG:export_metadata:sql__:
SELECT owner,table_name, column_name
FROM DBA_TAB_COLUMNS c WHERE (owner, table_name, column_id) in (
    SELECT owner,table_name, min(column_id) as min_column_id 
    from DBA_TAB_COLUMNS c WHERE 
    owner in ('LINEACRD') and 
    data_type in ('DATE')
    group by owner,table_name
)
order by owner,table_name,column_name
                            
LOG:target_execute:START:DELETE FROM METADATA_TABLE_DATE where OWNER = 'LINEACRD'
LOG:target_execute:OperationalError:DELETE FROM METADATA_TABLE_DATE where OWNER = 'LINEACRD'
LOG:target_execute:finally:DELETE FROM METADATA_TABLE_DATE where OWNER = 'LINEACRD'
LOG:target_execute:END:DELETE FROM METADATA_TABLE_DATE where OWNER = 'LINEACRD'
LOG:target_copy_to_table:START:METADATA_TABLE_DATE
LOG:target_copy_to_table:END:METADATA_TABLE_DATE
LOG:export_metadata_table_date:owner__:LINEACRD:END
PARRS::::::::::
{'_OWNER': 'LINEACRD', '_TABLE_NAME': 'CREDITO', '_COLUMN_NAME': 'FECHACREACION'}
LOG:--GENERATOR: script tabla:CREDITO 
                SELECT 
                'LINEACRD' as OWNER,
                'CREDITO' AS TABLE_NAME, 
                EXTRACT(YEAR FROM "FECHACREACION") as YEAR_,
                EXTRACT(YEAR FROM "FECHACREACION") * 100 +  EXTRACT(MONTH FROM "FECHACREACION") AS CODMES, 
                COUNT(1) AS CNT FROM LINEACRD.CREDITO
                GROUP BY EXTRACT(YEAR FROM "FECHACREACION"),EXTRACT(YEAR FROM "FECHACREACION") * 100 +  EXTRACT(MONTH FROM "FECHACREACION")
                
LOG:target_execute:START:DELETE FROM HISTORY_REPORT where OWNER = 'LINEACRD' and TABLE_NAME='CREDITO'
LOG:target_execute:OperationalError:DELETE FROM HISTORY_REPORT where OWNER = 'LINEACRD' and TABLE_NAME='CREDITO'
LOG:target_execute:finally:DELETE FROM HISTORY_REPORT where OWNER = 'LINEACRD' and TABLE_NAME='CREDITO'
LOG:target_execute:END:DELETE FROM HISTORY_REPORT where OWNER = 'LINEACRD' and TABLE_NAME='CREDITO'
LOG:target_copy_to_table:START:HISTORY_REPORT
LOG:target_copy_to_table:END:HISTORY_REPORT
LOG:export_metadata_table_date:owner__:DEV_BIPLATFORM:START
LOG:export_metadata:sql__:
SELECT owner,table_name, column_name
FROM DBA_TAB_COLUMNS c WHERE (owner, table_name, column_id) in (
    SELECT owner,table_name, min(column_id) as min_column_id 
    from DBA_TAB_COLUMNS c WHERE 
    owner in ('DEV_BIPLATFORM') and 
    data_type in ('DATE')
    group by owner,table_name
)
order by owner,table_name,column_name
                            
LOG:target_execute:START:DELETE FROM METADATA_TABLE_DATE where OWNER = 'DEV_BIPLATFORM'
LOG:target_execute:finally:DELETE FROM METADATA_TABLE_DATE where OWNER = 'DEV_BIPLATFORM'
LOG:target_execute:END:DELETE FROM METADATA_TABLE_DATE where OWNER = 'DEV_BIPLATFORM'
LOG:target_copy_to_table:START:METADATA_TABLE_DATE
LOG:target_copy_to_table:END:METADATA_TABLE_DATE
LOG:export_metadata_table_date:owner__:DEV_BIPLATFORM:END
PARRS::::::::::
{'_OWNER': 'DEV_BIPLATFORM', '_TABLE_NAME': 'FR_SCHEDULER_DATA', '_COLUMN_NAME': 'START_TIME'}
LOG:--GENERATOR: script tabla:FR_SCHEDULER_DATA 
                SELECT 
                'DEV_BIPLATFORM' as OWNER,
                'FR_SCHEDULER_DATA' AS TABLE_NAME, 
                EXTRACT(YEAR FROM "START_TIME") as YEAR_,
                EXTRACT(YEAR FROM "START_TIME") * 100 +  EXTRACT(MONTH FROM "START_TIME") AS CODMES, 
                COUNT(1) AS CNT FROM DEV_BIPLATFORM.FR_SCHEDULER_DATA
                GROUP BY EXTRACT(YEAR FROM "START_TIME"),EXTRACT(YEAR FROM "START_TIME") * 100 +  EXTRACT(MONTH FROM "START_TIME")
                
LOG:target_execute:START:DELETE FROM HISTORY_ISSUE where OWNER = 'DEV_BIPLATFORM' and TABLE_NAME='FR_SCHEDULER_DATA'
LOG:target_execute:OperationalError:DELETE FROM HISTORY_ISSUE where OWNER = 'DEV_BIPLATFORM' and TABLE_NAME='FR_SCHEDULER_DATA'
LOG:target_execute:finally:DELETE FROM HISTORY_ISSUE where OWNER = 'DEV_BIPLATFORM' and TABLE_NAME='FR_SCHEDULER_DATA'
LOG:target_execute:END:DELETE FROM HISTORY_ISSUE where OWNER = 'DEV_BIPLATFORM' and TABLE_NAME='FR_SCHEDULER_DATA'
-----------------------DEBUG____________________
{'OWNER': ['THE_OWNER'], 'TABLE_NAME': ['THE_TABLE'], 'ISSUE': ['THE_ISSUE'], 'VALUE': [0]}
-----------------------DEBUG____________________
{'OWNER': 'DEV_BIPLATFORM', 'TABLE_NAME': 'FR_SCHEDULER_DATA', 'ISSUE': 'NO_HISTORY', 'VALUE': 0}
-----------------------DEBUG__________DATAFRAME________
-----------------------DEBUG_______SAVING_________
            OWNER         TABLE_NAME       ISSUE  VALUE
0  DEV_BIPLATFORM  FR_SCHEDULER_DATA  NO_HISTORY      0
LOG:target_copy_to_table:START:HISTORY_ISSUE
LOG:target_copy_to_table:END:HISTORY_ISSUE
LOG:LEN:1
PARRS::::::::::
{'_OWNER': 'DEV_BIPLATFORM', '_TABLE_NAME': 'HSS_COMPONENT', '_COLUMN_NAME': 'LAST_UPDATE_DT'}
LOG:--GENERATOR: script tabla:HSS_COMPONENT 
                SELECT 
                'DEV_BIPLATFORM' as OWNER,
                'HSS_COMPONENT' AS TABLE_NAME, 
                EXTRACT(YEAR FROM "LAST_UPDATE_DT") as YEAR_,
                EXTRACT(YEAR FROM "LAST_UPDATE_DT") * 100 +  EXTRACT(MONTH FROM "LAST_UPDATE_DT") AS CODMES, 
                COUNT(1) AS CNT FROM DEV_BIPLATFORM.HSS_COMPONENT
                GROUP BY EXTRACT(YEAR FROM "LAST_UPDATE_DT"),EXTRACT(YEAR FROM "LAST_UPDATE_DT") * 100 +  EXTRACT(MONTH FROM "LAST_UPDATE_DT")
                
LOG:target_execute:START:DELETE FROM HISTORY_REPORT where OWNER = 'DEV_BIPLATFORM' and TABLE_NAME='HSS_COMPONENT'
LOG:target_execute:finally:DELETE FROM HISTORY_REPORT where OWNER = 'DEV_BIPLATFORM' and TABLE_NAME='HSS_COMPONENT'
LOG:target_execute:END:DELETE FROM HISTORY_REPORT where OWNER = 'DEV_BIPLATFORM' and TABLE_NAME='HSS_COMPONENT'
LOG:target_copy_to_table:START:HISTORY_REPORT
LOG:target_copy_to_table:END:HISTORY_REPORT
PARRS::::::::::
{'_OWNER': 'DEV_BIPLATFORM', '_TABLE_NAME': 'S_NQ_ACCT', '_COLUMN_NAME': 'START_TS'}
LOG:--GENERATOR: script tabla:S_NQ_ACCT 
                SELECT 
                'DEV_BIPLATFORM' as OWNER,
                'S_NQ_ACCT' AS TABLE_NAME, 
                EXTRACT(YEAR FROM "START_TS") as YEAR_,
                EXTRACT(YEAR FROM "START_TS") * 100 +  EXTRACT(MONTH FROM "START_TS") AS CODMES, 
                COUNT(1) AS CNT FROM DEV_BIPLATFORM.S_NQ_ACCT
                GROUP BY EXTRACT(YEAR FROM "START_TS"),EXTRACT(YEAR FROM "START_TS") * 100 +  EXTRACT(MONTH FROM "START_TS")
                
LOG:target_execute:START:DELETE FROM HISTORY_ISSUE where OWNER = 'DEV_BIPLATFORM' and TABLE_NAME='S_NQ_ACCT'
LOG:target_execute:finally:DELETE FROM HISTORY_ISSUE where OWNER = 'DEV_BIPLATFORM' and TABLE_NAME='S_NQ_ACCT'
LOG:target_execute:END:DELETE FROM HISTORY_ISSUE where OWNER = 'DEV_BIPLATFORM' and TABLE_NAME='S_NQ_ACCT'
-----------------------DEBUG____________________
{'OWNER': ['THE_OWNER'], 'TABLE_NAME': ['THE_TABLE'], 'ISSUE': ['THE_ISSUE'], 'VALUE': [0]}
-----------------------DEBUG____________________
{'OWNER': 'DEV_BIPLATFORM', 'TABLE_NAME': 'S_NQ_ACCT', 'ISSUE': 'NO_HISTORY', 'VALUE': 0}
-----------------------DEBUG__________DATAFRAME________
-----------------------DEBUG_______SAVING_________
            OWNER TABLE_NAME       ISSUE  VALUE
0  DEV_BIPLATFORM  S_NQ_ACCT  NO_HISTORY      0
LOG:target_copy_to_table:START:HISTORY_ISSUE
LOG:target_copy_to_table:END:HISTORY_ISSUE
LOG:LEN:1
PARRS::::::::::
{'_OWNER': 'DEV_BIPLATFORM', '_TABLE_NAME': 'S_NQ_EPT', '_COLUMN_NAME': 'UPDATE_TS'}
LOG:--GENERATOR: script tabla:S_NQ_EPT 
                SELECT 
                'DEV_BIPLATFORM' as OWNER,
                'S_NQ_EPT' AS TABLE_NAME, 
                EXTRACT(YEAR FROM "UPDATE_TS") as YEAR_,
                EXTRACT(YEAR FROM "UPDATE_TS") * 100 +  EXTRACT(MONTH FROM "UPDATE_TS") AS CODMES, 
                COUNT(1) AS CNT FROM DEV_BIPLATFORM.S_NQ_EPT
                GROUP BY EXTRACT(YEAR FROM "UPDATE_TS"),EXTRACT(YEAR FROM "UPDATE_TS") * 100 +  EXTRACT(MONTH FROM "UPDATE_TS")
                
LOG:target_execute:START:DELETE FROM HISTORY_ISSUE where OWNER = 'DEV_BIPLATFORM' and TABLE_NAME='S_NQ_EPT'
LOG:target_execute:finally:DELETE FROM HISTORY_ISSUE where OWNER = 'DEV_BIPLATFORM' and TABLE_NAME='S_NQ_EPT'
LOG:target_execute:END:DELETE FROM HISTORY_ISSUE where OWNER = 'DEV_BIPLATFORM' and TABLE_NAME='S_NQ_EPT'
-----------------------DEBUG____________________
{'OWNER': ['THE_OWNER'], 'TABLE_NAME': ['THE_TABLE'], 'ISSUE': ['THE_ISSUE'], 'VALUE': [0]}
-----------------------DEBUG____________________
{'OWNER': 'DEV_BIPLATFORM', 'TABLE_NAME': 'S_NQ_EPT', 'ISSUE': 'NO_HISTORY', 'VALUE': 0}
-----------------------DEBUG__________DATAFRAME________
-----------------------DEBUG_______SAVING_________
            OWNER TABLE_NAME       ISSUE  VALUE
0  DEV_BIPLATFORM   S_NQ_EPT  NO_HISTORY      0
LOG:target_copy_to_table:START:HISTORY_ISSUE
LOG:target_copy_to_table:END:HISTORY_ISSUE
LOG:LEN:1
PARRS::::::::::
{'_OWNER': 'DEV_BIPLATFORM', '_TABLE_NAME': 'S_NQ_INSTANCE', '_COLUMN_NAME': 'BEGIN_TS'}
LOG:--GENERATOR: script tabla:S_NQ_INSTANCE 
                SELECT 
                'DEV_BIPLATFORM' as OWNER,
                'S_NQ_INSTANCE' AS TABLE_NAME, 
                EXTRACT(YEAR FROM "BEGIN_TS") as YEAR_,
                EXTRACT(YEAR FROM "BEGIN_TS") * 100 +  EXTRACT(MONTH FROM "BEGIN_TS") AS CODMES, 
                COUNT(1) AS CNT FROM DEV_BIPLATFORM.S_NQ_INSTANCE
                GROUP BY EXTRACT(YEAR FROM "BEGIN_TS"),EXTRACT(YEAR FROM "BEGIN_TS") * 100 +  EXTRACT(MONTH FROM "BEGIN_TS")
                
LOG:target_execute:START:DELETE FROM HISTORY_ISSUE where OWNER = 'DEV_BIPLATFORM' and TABLE_NAME='S_NQ_INSTANCE'
LOG:target_execute:finally:DELETE FROM HISTORY_ISSUE where OWNER = 'DEV_BIPLATFORM' and TABLE_NAME='S_NQ_INSTANCE'
LOG:target_execute:END:DELETE FROM HISTORY_ISSUE where OWNER = 'DEV_BIPLATFORM' and TABLE_NAME='S_NQ_INSTANCE'
-----------------------DEBUG____________________
{'OWNER': ['THE_OWNER'], 'TABLE_NAME': ['THE_TABLE'], 'ISSUE': ['THE_ISSUE'], 'VALUE': [0]}
-----------------------DEBUG____________________
{'OWNER': 'DEV_BIPLATFORM', 'TABLE_NAME': 'S_NQ_INSTANCE', 'ISSUE': 'NO_HISTORY', 'VALUE': 0}
-----------------------DEBUG__________DATAFRAME________
-----------------------DEBUG_______SAVING_________
            OWNER     TABLE_NAME       ISSUE  VALUE
0  DEV_BIPLATFORM  S_NQ_INSTANCE  NO_HISTORY      0
LOG:target_copy_to_table:START:HISTORY_ISSUE
LOG:target_copy_to_table:END:HISTORY_ISSUE
LOG:LEN:1
PARRS::::::::::
{'_OWNER': 'DEV_BIPLATFORM', '_TABLE_NAME': 'S_NQ_JOB', '_COLUMN_NAME': 'NEXT_RUNTIME_TS'}
LOG:--GENERATOR: script tabla:S_NQ_JOB 
                SELECT 
                'DEV_BIPLATFORM' as OWNER,
                'S_NQ_JOB' AS TABLE_NAME, 
                EXTRACT(YEAR FROM "NEXT_RUNTIME_TS") as YEAR_,
                EXTRACT(YEAR FROM "NEXT_RUNTIME_TS") * 100 +  EXTRACT(MONTH FROM "NEXT_RUNTIME_TS") AS CODMES, 
                COUNT(1) AS CNT FROM DEV_BIPLATFORM.S_NQ_JOB
                GROUP BY EXTRACT(YEAR FROM "NEXT_RUNTIME_TS"),EXTRACT(YEAR FROM "NEXT_RUNTIME_TS") * 100 +  EXTRACT(MONTH FROM "NEXT_RUNTIME_TS")
                
LOG:target_execute:START:DELETE FROM HISTORY_ISSUE where OWNER = 'DEV_BIPLATFORM' and TABLE_NAME='S_NQ_JOB'
LOG:target_execute:finally:DELETE FROM HISTORY_ISSUE where OWNER = 'DEV_BIPLATFORM' and TABLE_NAME='S_NQ_JOB'
LOG:target_execute:END:DELETE FROM HISTORY_ISSUE where OWNER = 'DEV_BIPLATFORM' and TABLE_NAME='S_NQ_JOB'
-----------------------DEBUG____________________
{'OWNER': ['THE_OWNER'], 'TABLE_NAME': ['THE_TABLE'], 'ISSUE': ['THE_ISSUE'], 'VALUE': [0]}
-----------------------DEBUG____________________
{'OWNER': 'DEV_BIPLATFORM', 'TABLE_NAME': 'S_NQ_JOB', 'ISSUE': 'NO_HISTORY', 'VALUE': 0}
-----------------------DEBUG__________DATAFRAME________
-----------------------DEBUG_______SAVING_________
            OWNER TABLE_NAME       ISSUE  VALUE
0  DEV_BIPLATFORM   S_NQ_JOB  NO_HISTORY      0
LOG:target_copy_to_table:START:HISTORY_ISSUE
LOG:target_copy_to_table:END:HISTORY_ISSUE
LOG:LEN:1
[Finished in 15.5s]
