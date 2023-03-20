"""class_chart_base_class.py"""


import os
import random
from class_config import Config
from class_base_class import BaseClass


class ChartBaseClass(BaseClass):

    __chart_name__ = None
    __chart_dict__ = None
    __cfg__ = None
    __target_url__ = None

    def __init__(self,cfg,chart_dict,target_url__):
        """
        chart_dict {'CHART_NAME','TYPE','SQL','DATAFRAME'}
        """
        super().__init__(__log_active__)
        self.__chart_name__ = chart_dict['CHART_NAME']
        self.__chart_dict__ = chart_dict
        self.__cfg__ = cfg
        self.__target_url__ = target_url__
        
    def get(key):
        val = self.__chart_dict__[key]

    def get_engine_target(self):
        """function get_engine_target"""
        engine = create_engine(self.__target_url__)
        return engine

    def read_sql_query(self,cfg,sql_):
        """function read_sql_query"""
        data = None
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
        



