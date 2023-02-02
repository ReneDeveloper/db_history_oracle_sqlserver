"""configuration class"""
pars_ = {}

def get_parameter(par_):
    """Get config parameter"""
    return pars_[par_]

class Config:
    """class to obtain config parameters"""
    def get_par(self, par__):
        """Get config parameter"""
        return get_parameter(par__)
    def set_par(self, par__, val__):
        """Set config parameter"""
        pars_[par__]=val__

#parametro crkey
pars_["crkey"]=b'ixg0tK8e3dlzVT5NBzMGqEgfkaeRfsPFc76wZxAaD-0='

#parametro de bbdd source
pars_["username"]="username"
pars_["password"]="password"
pars_["hostname"]="hostname"
pars_["port"]="1521"
pars_["database"]="database"
#parametros de paths
pars_["lib_dir"]="C:/Users/rcastillosi/Downloads/PORTABLE/instantclient_21_7/"
pars_["out_dir"]="C:/Users/rcastillosi/__SQL_DATABASE_STATS__/__EXPORT_DATA__/"
#parametros de bbdd TARGET
pars_["TARGET_NAME"]="HISTORY_REPORT"
pars_["out_path"]="C:/Users/rcastillosi/__SQL_DATABASE_STATS__/__EXPORT_DATA__/"
#pars_["out_sqllit_file"]='sqlite:///{out_path}/SQLLITE_EXPORT_HISTORY_{out_owner}.db'
#pars_["out_sqllite"]=f'sqlite:///{pars_["out_path"]}/SQLLITE_EXPORT_HISTORY.db'
#parametros para nombres de archivos
pars_["pre_metadata"]="METADATA_"
#parametro de queries
pars_["QUERY_METADATA_COUNTS"] = """
select owner,
       table_name,
       num_rows
from sys.dba_tables
where owner not in ('SYS','SYSTEM','WMSYS','JAEDOC','XDB','DBSNMP')
                            """

pars_["QUERY_METADATA_TABLE_DATE"] = """
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

pars_["QUERY_METADATA_DAILY_SPACE"] = """
select s.tablespace_name,s.owner, s.segment_name, s.segment_type,
case when s.segment_type = 'INDEX' then i.table_name when s.segment_type = 'TABLE' then s.segment_name end as table_name_normalizado,
sum(s.bytes)/1024/1024 total_mb, 
sum(case when s.segment_type='INDEX' then s.bytes else 0 end)/1024/1024 total_mb_index, 
sum(case when s.segment_type='TABLE' then s.bytes else 0 end)/1024/1024 total_mb_table, 
count(1) as cnt_seg  
from dba_segments s 
left join dba_indexes i on s.segment_type='INDEX' and i.index_name = s.segment_name and i.owner = s.owner
group by s.tablespace_name,s.owner, s.segment_name,s.segment_type,case when s.segment_type = 'INDEX' then i.table_name when s.segment_type = 'TABLE' then s.segment_name end
                            """















































































































