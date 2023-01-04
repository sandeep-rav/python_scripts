import numpy as np
import pandas as pd
from src.python.etl.user_defined_fn import UserDefinedFn
import datetime
import warnings
warnings.filterwarnings("ignore")
from src.python.etl.user_defined_fn import UserDefinedFn
time_stamp = datetime.datetime.now().strftime('%Y%m%d')

# Function for Shipped history  and Open data preparation
def open_order_ship_processor(input_file_path:str,input_file_name:str,
                              input_col_dir_path,input_col_dir_file_name,
                              output_file_path:str,output_file_name:str,table_name:str):
    cols_directory = pd.read_csv(input_col_dir_path + input_col_dir_file_name, encoding='UTF-8')
    req_table = cols_directory.loc[(cols_directory['table_name']) == table_name,:]
    input_field_list = req_table['input_field_name'].to_list()
    list_str_cols = cols_directory.loc[(cols_directory['table_name'] == table_name) &
                                       (cols_directory['data_type'] == 'str'), 'input_field_name'].to_list()
    dict_dtypes = {x: 'str' for x in list_str_cols}
    df = pd.read_csv(input_file_path + input_file_name,usecols=input_field_list,dtype=dict_dtypes, encoding='UTF-8')
    tableau_field_list = req_table['tableau_visual_field_name'].to_list()
    variable_directory = {i: j for i, j in zip(input_field_list, tableau_field_list) if i in input_field_list}
    df = df.rename(columns=variable_directory)
    df = df[tableau_field_list]
    df.to_csv(output_file_path+output_file_name,sep=',', header=True, index=False)
    print("Required fields extracted.")


# Declare the arguments to the function for open order
input_file_path="C:\\distribution_dashboard\\data\\inputs\\open_order\\latest\\"
input_file_name="melchioni_open_orders.csv"
input_col_dir_path="C:\\distribution_dashboard\\data\\inputs\\"
input_col_dir_file_name="ship_history_open_order_rt_test_directory.csv"
output_file_path="C:\\distribution_dashboard\\data\\outputs\\table\\order_intake\\open_order\\"
output_file_name = time_stamp+"_open_order_melchioni.txt"
table_name = "open_order"

# Call the function for open order
open_order_ship_processor(input_file_path,input_file_name,
                          input_col_dir_path,input_col_dir_file_name,
                          output_file_path,output_file_name,table_name)

# Declare the arguments to the function for shipped history
input_file_path="C:\\distribution_dashboard\\data\\inputs\\ship_history\\latest\\"
input_file_name="melchioni_shipped_history.csv"
input_col_dir_path="C:\\distribution_dashboard\\data\\inputs\\"
input_col_dir_file_name="ship_history_open_order_rt_test_directory.csv"
output_file_path="C:\\distribution_dashboard\\data\\outputs\\table\\order_intake\\shipped_history\\"
output_file_name= time_stamp + "_shipped_history_melchioni.txt"
table_name = "shipped_history"

# Call the function
open_order_ship_processor(input_file_path,input_file_name,
                          input_col_dir_path,input_col_dir_file_name,
                          output_file_path,output_file_name,table_name)

# RT Preparation for Shipped History and Open Order
def open_order_ship_combiner(input_file_path_l:str,input_file_path_r:str,
                             input_file_name_l:str,input_file_name_r:str,
                             output_file_path:str,output_file_name:str,
                             input_col_dir_path:str,input_col_dir_file_name:str):
    #for open orders dtypes
    cols_directory = pd.read_csv(input_col_dir_path + input_col_dir_file_name, encoding='UTF-8')
    list_str_cols_open = cols_directory.loc[(cols_directory['table_name'] == 'open_order') &
                                       (cols_directory['data_type'] == 'str'), 'tableau_visual_field_name'].to_list()
    list_int_cols_open = cols_directory.loc[(cols_directory['table_name'] == 'open_order') &
                                       (cols_directory['data_type'] == 'int'), 'tableau_visual_field_name'].to_list()
    list_int_cols_ship = cols_directory.loc[(cols_directory['table_name'] == 'shipped_history') &
                                      (cols_directory['data_type'] == 'int'), 'tableau_visual_field_name'].to_list()
    dict_dtypes_open = {x: 'str' for x in list_str_cols_open}
    list_str_cols_shipped = cols_directory.loc[(cols_directory['table_name'] == 'shipped_history') &
                                       (cols_directory['data_type'] == 'str'), 'tableau_visual_field_name'].to_list()
    dict_dtypes_shipped = {x: 'str' for x in list_str_cols_shipped}
    open_df = pd.read_table(input_file_path_l + input_file_name_l, delimiter=',',dtype=dict_dtypes_shipped, encoding='ISO-8859-1')
    #open_df[list_int_cols_open] = open_df[list_int_cols_open].apply(pd.to_numeric,errors='coerce')
    ship_df = pd.read_table(input_file_path_r + input_file_name_r, delimiter=',',dtype=dict_dtypes_shipped, encoding='ISO-8859-1')
    #ship_df[list_int_cols_ship] = ship_df[list_int_cols_ship].apply(pd.to_numeric, errors='coerce')
    open_df.dropna(thresh=2, inplace=True)
    open_df.dropna(thresh=2,inplace=True)
    # For shipped history
    for i in set(open_df.columns).difference(set(ship_df.columns)):
        ship_df[i] = np.nan
    # For open orders
    for i in set(ship_df.columns).difference(set(open_df.columns)):
        open_df[i] = np.nan
    open_df['source'] = 'open order'
    ship_df['source'] = 'shipped history'
    final_df = pd.concat([open_df, ship_df], axis=0)
    final_df = final_df.fillna(np.nan)
    final_df = final_df.replace("//",np.nan)
    final_df = final_df.replace("00/00/00",np.nan)
    # Epcos Part Number Cleaning
    final_df['Original Item Code'] = final_df['Original Item Code'].apply(UserDefinedFn.epcos_cleaning)
    #final_df = final_df.loc[(final_df['Actual GI/Confirmed Delivery Date']!="00/00/00"),]
    #final_df = final_df.loc[(final_df['Actual GI/Confirmed Delivery Date'] != "//"),]
    #final_df['Actual GI/Confirmed Delivery Date'].unique()
    final_df['Time Period'] = pd.to_datetime(final_df['Actual GI/Confirmed Delivery Date']).dt.strftime('%Y/%m')
    final_df.to_csv(output_file_path+output_file_name,sep='\t', header=True, index=False)
    print("Combined file of Open Order and Ship History Created.")

# Declare the arguments to the function
input_file_path_l = "C:\\distribution_dashboard\\data\\outputs\\table\\order_intake\\open_order\\"
input_file_path_r = "C:\\distribution_dashboard\\data\\outputs\\table\\order_intake\\shipped_history\\"
input_file_name_l = '20221223' + "_open_order_melchioni.txt"
input_file_name_r = '20221223' + "_shipped_history_melchioni.txt"
output_file_path = "C:\\distribution_dashboard\\data\outputs\\table\\order_intake\\"
output_file_name = '20221214' + "_combined_open_order_ship_history.txt"
input_col_dir_path="C:\\distribution_dashboard\\data\\inputs\\"
input_col_dir_file_name="ship_history_open_order_rt_test_directory.csv"

# Call the function
open_order_ship_combiner(input_file_path_l,input_file_path_r,
                         input_file_name_l,input_file_name_r,
                         output_file_path,output_file_name,
                         input_col_dir_path,input_col_dir_file_name)


#final_df[final_df['Shipped Quantity']>0]

# convert the csv files to .txt file
input_file_path="C:\\distribution_dashboard\\data\\inputs\\ship_history\\latest\\"
input_file_name="melchioni_shipped_history.csv"
df = pd.read_csv(input_file_path + input_file_name, encoding='UTF-8')
df.columns
output_file_path="C:\\distribution_dashboard\\data\\inputs\\ship_history\\latest\\"
output_file_name="melchioni_shipped_history.txt"
df.to_csv(input_file_path+output_file_name,sep=',', header=True,index=False)
df.shape
output_file_path, output_file_name
df = pd.read_csv(output_file_path + output_file_name,sep='\t', encoding='UTF-8')



# Data Refresh function
# Defining the function for data refresh activity of open order and ship history
def data_refresh_osh(master_dataset_path:str,master_dataset_file_name:str,
                 new_dataset_path:str,new_dataset_file_name:str,
                 output_path:str,output_file_name:str,
                 previous_refresh_date:str,max_date:str):
    master_dataset = pd.read_table(master_dataset_path + master_dataset_file_name, delimiter='\t',encoding='ISO-8859-1')
    new_dataset = pd.read_table(new_dataset_path + new_dataset_file_name, delimiter='\t', encoding='ISO-8859-1')
    master_dataset = pd.concat([master_dataset,new_dataset],axis=0)
    print("New set of data points added")



# Declare the arguments and call the function
master_dataset_path = "C:\\distribution_dashboard\\data\outputs\\table\\order_intake\\"
master_dataset_file_name = ""
new_dataset_path = "C:\\distribution_dashboard\\data\outputs\\table\\order_intake\\"
new_dataset_file_name = time_stamp + "_combined_open_order_ship_history.txt"
output_path = "C:\\distribution_dashboard\\data\\outputs\\table\\gmds_sales\\"
output_file_name = '20221029' + "_gmds_sales.txt"

data_refresh_osh(master_dataset_path,master_dataset_file_name,new_dataset_path,new_dataset_file_name,output_path,output_file_name)


# Defining gmds_tabular_generator function for Tabular data generation
def osh_tabular_generator(master_dataset_path:str,master_dataset_file_name,
                           output_file_path:str,output_file_name:str):
    df = pd.read_table(master_dataset_path + master_dataset_file_name,delimiter='\t',encoding='ISO-8859-1')
    start_date = datetime.date.today()-timedelta(days=366)
    cur_date = datetime.date.today()
    final_df = df.loc[((pd.to_datetime(df['Process Date']) >= pd.to_datetime(start_date))
                       & (pd.to_datetime(df['Process Date']) <= pd.to_datetime(cur_date))),:]
    final_df.to_csv(output_file_path + output_file_name, sep='\t', header=True, index=False)


# Declare the arguments and call the function
master_dataset_path = "C:\\distribution_dashboard\\data\\outputs\\table\\gmds_sales\\"
master_dataset_file_name = "20221029_gmds_sales.txt"
output_file_path = "C:\\distribution_dashboard\\data\\outputs\\table\\gmds_sales\\"
output_file_name = "tabular_views' + '_gmds_sales.txt"

# Calling the function
osh_tabular_generator(master_dataset_path,master_dataset_file_name,output_file_path,output_file_name)


open_df
