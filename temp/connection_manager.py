import cx_Oracle
import pandas as pd

# Point the directory to Oracle Client files

cx_Oracle.init_oracle_client(lib_dir='C:\distribution_dashboard\src\python\etl\instantclient-basic-windows.x64-21.3.0.0.0\instantclient_21_3')

dsn = cx_Oracle.makedsn(
    'gmdx.tdk.biz',
    '1521',
    service_name='gmds_srv'
)
conn = cx_Oracle.connect(
    user='ORA_DM',
    password='B4u9Srj4h',
    dsn=dsn
)
c = conn.cursor()
c.execute('SELECT * FROM DISTI WHERE ROWNUM<=5')
print(pd.DataFrame(c))

for row in c:
    print(row)