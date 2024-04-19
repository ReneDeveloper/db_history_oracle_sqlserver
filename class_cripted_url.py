"""class_cripto.py"""
"""
This code encripts a SOURCE URL to a binary String
#to understand what  URL needs to use:
#you can decide the flavor to use ORACLE or SQLSERVER 
#or other, but, need to exists in class_config.py the different queries flavor_QUERY_METADATA_COUNTS
#https://docs.sqlalchemy.org/en/20/core/engines.html
"""

from cryptography.fernet import Fernet
from class_config import Config
from class_base_class import BaseClass

#TODO: move this to Environment Parameter (and obtain by env parameter) or different value by each URL
crkey=b'ixg0tK8e3dlzVT5NBzMGqEgfkaeRfsPFc76wZxAaD-0='

class CriptedUrl(BaseClass):
    """"""
    def __init__(self):
        """__init__"""
        self._log("INIT:CriptedUrl")

    def cript_url(self,sqlalchemy_url):
        """__init__"""

        # encrypting SQLALCHEMY URLS: like the below strings:
        #message = 'oracle+cx_oracle://{username}:{password}@{host_}:{port}/{database}'
        #message = 'oracle+cx_oracle://user:pw@server:port/instance'
        #message = 'mssql+pymssql://{username}:{password}@{host_}:{port}/{database}'
        #message = 'mssql+pymssql://user:pw@server:1443/master'

        #TODO: remove this line
        self._log("CriptedUrl:cript_url:")

        # Instance the Fernet class with the key
        fernet = Fernet(crkey)

        # then use the Fernet class instance
        # to encrypt the string string must
        # be encoded to byte string before encryption
        encMessage = fernet.encrypt(sqlalchemy_url.encode())

        #print("original  string: ", message)
        print("SAVE THIS encrypted string, and use to create a HistoryReport instance")
        print("-------------------------------")
        print(encMessage)
        print("-------------------------------")


