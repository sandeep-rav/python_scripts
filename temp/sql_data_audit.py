import cx_Oracle
import csv
import numpy as np
import time

# Point the directory to Oracle Client files

cx_Oracle.init_oracle_client(lib_dir='C:\distribution_dashboard\src\python\etl\instantclient-basic-windows.x64-21.3.0.0.0\instantclient_21_3')
begin = time.time()
dsn = cx_Oracle.makedsn(
    'gmdx.tdk.biz',
    '1521',
    service_name='gmds_srv'
)
conn = cx_Oracle.connect(
    user='ORA_DM',
    password='B4u9Srj4h',
    dsn=dsn)

cursor = conn.cursor()

csv_file = open("C:/discrepancies_check/gmds_oracle_server/" + "20230103_ALL_TABLES" + ".csv", "w")
writer = csv.writer(csv_file, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC)
r = cursor.execute("SELECT owner,table_name,column_name,data_type FROM ALL_TAB_COLUMNS")
# this stores the column names
col_names = [row[0].lower() for row in cursor.description]
writer.writerow(col_names)
# reading and writing the first ten columns
for row in cursor:
    writer.writerow(row)
print("File Created.")
cursor.close()
# conn.close()
csv_file.close()