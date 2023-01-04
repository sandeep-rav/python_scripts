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
    dsn=dsn
)

cursor = conn.cursor()
file_list = ['GBUDGET_BASE_MONTHLY_V_X','OSIS_CUSTOMER_V','CUSTOMER','SAO_GROUP',
            'CUSTOMER_GROUP','DISTY_TYPE','DIVSB','PLNGRP',
            'PLNSUM','PLNIT',#'REGION',
             'GISLS_PROCESS_DAILY_V_X',
            'GIBB_PROCESS_DAILY_V_X','GIORDER_BASE_DAILY_V_X','DISTI',
             'EAG_PRODUCT_HIERARCHY_V','END_CUST_COUNTRY']




for file in file_list:
    csv_file = open("C:/discrepancies_check/gmds_oracle_server/" + file + ".csv", "w")
    writer = csv.writer(csv_file, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC)
    r = cursor.execute("SELECT * FROM " + file + " WHERE ROWNUM <=10")
# this stores the column names
    col_names = [row[0].lower() for row in cursor.description]
    writer.writerow(col_names)
    i = 0
# reading and writing the first ten columns
    for row in cursor:
        i += 1
        print("Creating  " + file + " copy from the database" )
        print("The number rows written", i)
        writer.writerow(row)

cursor.close()
# conn.close()
csv_file.close()
# store ending time
end = time.time()

# total time taken
print(f"Total runtime of the program is {np.round((end - begin)/60,2)} minutes")



