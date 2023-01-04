import pandas as pd
import pyarrow as pa
import numpy as np
from pyarrow import parquet as pq
import random
import itertools
from itertools import chain, combinations
# For PoP Data

input_file_path = "C:\\distribution_dashboard\\data\\outputs\\table\\gmds_sales\\"
input_file_name = "20221029_gmds_sales.txt"
output_file_path = "C:\\distribution_dashboard\\data\\outputs\\table\\adhocs\\"
input_cols_dir_path = "C:\\distribution_dashboard\\data\\inputs\\gmds_sales\\"
input_cols_dir_file_name = "gmds_input_col_dir.csv"
output_file_path_parquet = "C:\\distribution_dashboard\\data\\oracle_export\\parquet_files\\"
output_file_path_csv = "C:\\distribution_dashboard\\data\\oracle_export\\txt_files\\"
output_file_name = "20221208_gmds_sales"


col_names_dir = pd.read_csv(input_cols_dir_path + input_cols_dir_file_name, sep=',')
col_names = col_names_dir['field_name'].to_list()
list_str_cols = col_names_dir.loc[(col_names_dir['data_type'] == 'str')]['field_name'].to_list()
dict_dtypes = {x: 'str' for x in list_str_cols}

df = pd.read_table(input_file_path+input_file_name,sep='\\t',encoding='ISO-8859-1',low_memory=False)
df.columns = col_names
df.to_csv(output_file_path_csv+output_file_name+'.txt',sep='\\t',index=False)
# Read to parquet file
table = pa.Table.from_pandas(df)
pq.write_table(table, output_file_path_parquet+output_file_name+'.parquet')

df_par = pd.read_parquet(output_file_path_parquet+output_file_name+'.parquet',engine='pyarrow')
df_par.head()
df_par.columns

# For PoS and PoP sales

def txt_parquet(input_file_path:str,input_file_name:str,output_file_path_parquet:str,output_file_name:str):
    col_names = ["base_month", "item_code", "invoice_date",
                 "sap_customer", "customer_name", "bg",
                 "bg_name", "cbu", "cbu_name", "sbu", "sbu_name",
                 "e-cust_name", "e-cust_country", "e-cust_zip_code",
                 "sales_qty", "sales_amount", "sales_price",
                 "to_paired_qty", "to_paired", "pur_price",
                 "gm_abs._calc.", "gm_%", "past_12_month_sales_amt", "past_12_month_sales_qty"]
    lst_str_cols = ["base_month", "item_code", "invoice_date",
                    "sap_customer", "customer_name", "bg",
                    "bg_name", "cbu", "cbu_name", "sbu", "sbu_name",
                    "e-cust_name", "e-cust_country", "e-cust_zip_code"]

    dict_dtypes = dict_dtypes = {x: 'str' for x in lst_str_cols}
    df = pd.read_table(input_file_path+input_file_name, header=None,delimiter='\t',encoding='ISO-8859-1',names = col_names,dtype= dict_dtypes,error_bad_lines=False)
    df.columns = col_names
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_file_path_parquet + output_file_name + '.parquet',)
    print("Converted txt file to parquer file")


input_file_path = "C:\\distribution_dashboard\\data\\inputs\\pos_sales\\"
input_file_name = "pos_sales.txt"
output_file_path_parquet = "C:\\distribution_dashboard\\data\\oracle_export\\parquet_files\\"
output_file_name = "20221221_pos_sales"

txt_parquet(input_file_path,input_file_name,output_file_path,output_file_name)


# Check QA
check_pos = pd.read_parquet(output_file_path_parquet+'20221221_pos_sales'+'.parquet')
# Find the columns with unique values for every row
unique_columns = check_pos.columns[check_pos.apply(lambda x: x.nunique() == check_pos.shape[0])]
# Print the columns that can act as a primary key
print(unique_columns)

check_pos.columns


# Get a list of all possible combinations of columns
column_combinations = list(itertools.combinations(check_pos.columns, 3))  # for combinations of 2 columns

# Loop through each combination and check if it acts as a primary key
for combination in column_combinations:
    if not check_pos[list(combination)].duplicated().any():
        print(f'The group of columns {combination} acts as a primary key')


lst_str_cols.extend(['sales_qty','sales_price'])

# Different function to automate n combination selection
def key_options(items):
    return chain.from_iterable(combinations(items, r) for r in range(1, len(items)+1) )

df = pd.read_parquet(output_file_path_parquet+'20221221_pos_sales'+'.parquet')
df = df.loc[0:500000,lst_str_cols]
# iterate over all combos of headings, excluding ID for brevity
for candidate in key_options(list(df)[1:]):
    deduped = df.drop_duplicates(candidate)

    if len(deduped.index) == len(df.index):
        print (','.join(candidate))


p_key = df.groupby(['item_code','invoice_date','sap_customer','e-cust_name','e-cust_country','e-cust_zip_code'])['sap_customer'].count()


# Function for masking numerical data
def masking_field(input_df,output_file_path_parquet,output_file_name):
    noise = [random.uniform(0.01, 0.99) for _ in range(len(input_df))]
    for var_name in input_df.columns:
        if (input_df[var_name].dtype) != 'object':
            input_df[var_name] = input_df[var_name] * noise
    df = input_df
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_file_path_parquet + output_file_name + '.parquet', )
    print("Data Masking completed.")

input_df = check_pos
output_file_path_parquet = "C:\\distribution_dashboard\\data\\oracle_export\\parquet_files\\"
output_file_name = '20221222_pos_sales'
masking_field(input_df,output_file_path_parquet,output_file_name)

# Function to convert txt files to parquet
def txt_parquet(input_file_path:str,input_file_name:str,output_file_path_parquet:str,output_file_name:str):
    df = pd.read_table(input_file_path+input_file_name, header=None,delimiter='\t',encoding='ISO-8859-1',low_memory=False,error_bad_lines=False)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_file_path_parquet + output_file_name + '.parquet',)
    print("Converted txt file to parquet file")

