"""class_cripto.py"""

from cryptography.fernet import Fernet
from class_config import Config

cfg = Config('NO_INICIALIZADO','NO_INICIALIZADO')

#to understand what  URL needs to use:
#you can decide the flavor to use ORACLE or SQLSERVER 
#or other, but, need to exists in class_config.py the different queries flavor_QUERY_METADATA_COUNTS
#https://docs.sqlalchemy.org/en/20/core/engines.html

# we will be encrypting the below string.
#message = 'oracle+cx_oracle://{username}:{password}@{host_}:{port}/{database}'
#message = 'oracle+cx_oracle://user:pw@server:port/instance'

#message = 'mssql+pymssql://{username}:{password}@{host_}:{port}/{database}'
message = 'mssql+pymssql://user:pw@server:1443/master'




#engine = create_engine("mssql+pyodbc://scott:tiger@mydsn")


#engine = create_engine("mssql+pymssql://scott:tiger@hostname:port/dbname")

#key = Fernet.generate_key()
key = cfg.get_par('crkey')
#print(f'key:{key}')

# Instance the Fernet class with the key
fernet = Fernet(key)

# then use the Fernet class instance
# to encrypt the string string must
# be encoded to byte string before encryption
encMessage = fernet.encrypt(message.encode())

print("original  string: ", message)
print("encrypted string: ", encMessage)

# decrypt the encrypted string with the
# Fernet instance of the key,
# that was used for encrypting the string
# encoded byte string is returned by decrypt method,
# so decode it to string with decode methods
decMessage = fernet.decrypt(encMessage).decode()

print("decrypted string: ", decMessage)




