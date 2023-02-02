
import cx_Oracle

import pandas as pd
from sqlalchemy import create_engine

oracle_connection_string = 'oracle+cx_oracle://{username}:{password}@{hostname}:{port}/{database}'
cx_Oracle.init_oracle_client(lib_dir= "C:/app/Administrador/product/11.2.0/client_2")
#C:/app/Administrador/product/11.2.0/client_1
engine = create_engine(
    oracle_connection_string.format(
        username='CALCULATING_CARL',
        password='12345',
        hostname='all.thedata.com',
        port='1521',
        database='everything',
    )
)

data = pd.read_sql("SELECT * FROM dba_users", engine)



oracle_connection_string = 'oracle+cx_oracle://{username}:{password}@' 
cx_Oracle.makedsn('{hostname}', '{port}', service_name='{service_name}')

app = Flask(__name__)
#CORS(app)
app.config['SQLALCHEMY_DATABASE_URI'] = oracle_connection_string.format(
  username='',
  password='',
  hostname='',
  port='1521',
  service_name=''
)

class DatabaseMetadata:

    def __init__(self, name):
        self.name = name
        self.tricks = []

    def __cnx__(self):
    	return None


    def getMetadata(self, owner):
        cnx = self.__cnx__()
        self.owner = owner


p = DatabaseMetadata('EVALUACION')
p.getMetadata('EVALUACION')

