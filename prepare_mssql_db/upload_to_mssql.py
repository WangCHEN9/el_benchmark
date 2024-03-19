
from sqlalchemy import create_engine
import urllib
import pyodbc
import pandas as pd

import os

SERVER = 'tcp:el-fmk-poc.database.windows.net,1433'
DATABASE = 'el-fmk-poc'
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')

connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'


df = pd.read_parquet(r"UNSW-NB15/Network-Flows/UNSW_Flow.parquet")
print(df.head(1))
quoted = urllib.parse.quote_plus(connectionString)
print(quoted)
df = df.map(str)
print(len(df))
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
df.to_sql('TargetTable', schema='dbo', con = engine, chunksize=20, method='multi', index=False, if_exists='replace')

print("upload done !")
