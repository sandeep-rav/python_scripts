# Importing the packages and required functions
import pandas as pd
import csv
import cx_Oracle
import datetime
import pyarrow as pa
from pyarrow import parquet as pq
import warnings
warnings.filterwarnings("ignore")
time_stamp = datetime.datetime.today().strftime('%Y%m%d')

# Establish the Oracle connection
# Point the directory to Oracle Client files
cx_Oracle.init_oracle_client(lib_dir="C:\\distribution_dashboard\\src\\python\\etl\\instantclient-basic-windows.x64-21.3.0.0.0\\instantclient_21_3")

dsn = cx_Oracle.makedsn(
    'gmdx.tdk.biz',
    '1521',
    service_name='gmds_srv'
)
conn = cx_Oracle.connect(
    user='ORA_DM',
    password='B4u9Srj4h',
    dsn=dsn,encoding = 'iso-8859-1')

cursor = conn.cursor()


# Function for exporting tables from Oracle to Local in .txt and .paraquet format
def oracle_table_processor(output_file_path_csv:str,output_file_path_parquet:str,output_file_path_csv_diff:str):
    for i in input_file_list:
        input_query= "SELECT COUNT(*) as count_ FROM " + i
        extract_query =  "SELECT * FROM " + i
        print("Reading the query")
        dim = pd.read_sql(input_query, conn)
        if (dim['COUNT_'].values < 500000):
            print("Writing the queries for ",i)
            df = pd.read_sql(extract_query,conn)
            df.to_csv(output_file_path_csv+time_stamp+'_'+str(i).lower()+'.txt',sep='\t',
                      index=False,header=True,escapechar='\t')
            print("txt File Created for ",i)
            table = pa.Table.from_pandas(df)
            pq.write_table(table, output_file_path_parquet+time_stamp+'_'+str(i).lower()+'.parquet')
            print("Paraquet File Created for ",i)
        else:
            print("Reading in chunks for file ",i)
            extract_query = "SELECT * FROM " + i
            header = True
            for chunk in pd.read_sql(extract_query, conn,chunksize=8000):
                print("Started writing..")
                chunk.to_csv(output_file_path_csv_diff+time_stamp+'_'+str(i).lower()+'.txt',
                             sep='\t',escapechar='\t',index=False,header=header,mode='a')
                header = False
            print("File Created.")
    print("All the specified files are converted to Parquet format")

# Declare the arguments of the function
input_file_list =  ['OSIS_CUSTOMER_V','END_CUST_INDUSTRY','EXR_BASE_V','GISLS_PROCESS_DAILY_V_X','GIBB_PROCESS_DAILY_V_X',
                    'GIORDER_BASE_DAILY_V_X','DISTI','OSIS_CUSTOMER_V']

output_file_path_csv = "C:\\distribution_dashboard\\data\\oracle_export\\txt_files\\"
output_file_path_csv_diff = "C:\\distribution_dashboard\\data\\oracle_export\\txt_files_freq\\"
output_file_path_parquet = "C:\\distribution_dashboard\\data\\oracle_export\\parquet_files\\"

# Call the function
oracle_table_processor(output_file_path_csv,output_file_path_parquet,output_file_path_csv_diff)






'''
'END_CUST_INDUSTRY','EXR_BASE_V']
chunk_files_list = ['CUSTOMER','SAO_GROUP','CUSTOMER_GROUP','DISTY_TYPE','DIVSB','PLNGRP',
                   'PLNSUM','PLNIT','REGION','EAG_PRODUCT_HIERARCHY_V',
                   'END_CUST_COUNTRY','MARKETING_FIELD','MARKET_GROUP','EXRDAY',
                    'GBUDGET_BASE_MONTHLY_V_X','GBUDGET_BASE_MONTHLY_V_X','GISLS_PROCESS_DAILY_V_X',
'GIBB_PROCESS_DAILY_V_X','GIORDER_BASE_DAILY_V_X','DISTI','OSIS_CUSTOMER_V']
'''