import cx_Oracle
import csv
import time
import datetime
import warnings
import pandas as pd
warnings.filterwarnings("ignore")
time_stamp = datetime.datetime.today().strftime('%Y%m%d')


# Point the directory to Oracle Client files
cx_Oracle.init_oracle_client(lib_dir="C:\\distribution_dashboard\\src\\python\\etl\\instantclient-basic-windows.x64-21.3.0.0.0\\instantclient_21_3")
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

for chunk_size in pd.read_sql("SELECT * FROM CUSTOMER_GROUP",conn,chunksize=200):
    print(chunk_size)

def oracle_query_txt(input_field_list:list,output_file_path:str):
    for i in input_field_list:
        input_query = "SELECT * FROM " + i
        df = pd.read_sql(input_query,conn)
        print("File Created.")
    #Closing the file and the connection after completion
    txt_file.close()
    cursor.close()

# Defining the arguments of the function
input_file_list =  [ #'GBUDGET_BASE_MONTHLY_V_X' ,'OSIS_CUSTOMER_V','CUSTOMER','SAO_GROUP','CUSTOMER_GROUP','DISTY_TYPE','DIVSB','PLNGRP','PLNSUM','PLNIT','REGION',
                   'DISTI','EAG_PRODUCT_HIERARCHY_V',
                   'END_CUST_COUNTRY','END_CUST_INDUSTRY','MARKETING_FIELD',
                   'MARKET_GROUP','EXR_BASE_V','EXRDAY','GISLS_PROCESS_DAILY_V_X',
                   'GIBB_PROCESS_DAILY_V_X','GIORDER_BASE_DAILY_V_X']
output_file_path = "C://oracle_export//txt_files//"

#Calling the Function
oracle_query_txt(input_file_list,output_file_path)
