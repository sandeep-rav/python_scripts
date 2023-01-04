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


def ts_oracle_export(output_file_path_csv:str,output_file_parquet:str):
    for i in input_file_list:
        session = "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYYMMDD'"
        nls = cursor.execute(session)
        #input_query = "SELECT * FROM " + i
        input_query = "SELECT DISTINCT BASE_MONTH as month_ FROM " + "exr_base_v" + " ORDER BY BASE_MONTH DESC"
        dim = pd.read_sql(input_query, conn)
        print("Captured Distinct Month List ")
        for j in dim['MONTH_']:
            #& (dim['MONTH_']<=202501)
            extract_query = "SELECT * FROM " + i + " WHERE BASE_MONTH = " + str(j)
            print("Running for month ",j,'table ',i)
            header = True
            for chunk in pd.read_sql(extract_query, conn, chunksize=100000):
                print("Started writing..")
                chunk.to_csv(output_file_path_csv + str(j) + '_' + str(i).lower() + '.txt',sep='\t', escapechar='\t', index=False, header= header, mode='a')
                header = False
            df = pd.read_table(output_file_path_csv +  str(j) + '_' + str(i).lower() + '.txt',delimiter='\t',encoding='ISO-8859-1',low_memory=False,error_bad_lines=False)
            table = pa.Table.from_pandas(df)
            pq.write_table(table, output_file_path_parquet + str(j) + '_' + str(i).lower() + '.parquet')
            print("Paraquet and txt File Created for " + i ,j)


input_file_list = ['exr_base_v']
output_file_path_csv = "C:\\distribution_dashboard\\data\oracle_export\\txt_files_freq\\"
output_file_path_parquet = "C:\\distribution_dashboard\\data\\oracle_export\\parquet_files\\"
ts_oracle_export(output_file_path_csv,output_file_path_parquet)
#dd = pd.read_csv("C:/distribution_dashboard/data/oracle_export/txt_files_freq/202209_gbudget_base_monthly_v_x.txt",sep='\t')


#['gisls_process_daily_v_x'] gibb_process_daily_v_x
# pending 202104 'gibb_process_daily_v_x', disti # 201912 below years pending -'giorder_base_daily_v_x',
# input = "C:\distribution_dashboard\data\oracle_export\parquet_files/202201_exr_base_v.parquet"
# Parquet code for testing
#dd = pd.read_parquet(input,engine='pyarrow')

