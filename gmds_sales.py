# Importing the required packages
from src.python.etl.user_defined_fn import UserDefinedFn
import pandas as pd
import numpy as np
import warnings
import time
import datetime
from datetime import timedelta
warnings.filterwarnings('ignore')
#time_stamp = datetime.datetime.today().strftime('%Y%m%d')
time_stamp = '20221029'

start = time.time()


# Defining the GMDS data processing function for RT Generation
def gmds_data_processor(input_path:str,output_path:str,
                        input_file_name:str,input_cols_dir_path:str,
                        input_cols_dir_file_name:str,output_file_name:str,
                        master_field_path:str,master_field_file_name:str,
                        master_field_table_name:str):
    """
    This function reads the output extracted from GMDS source
    in chunks based on time range specifies and does part cleaning
    and finally create a reporting table
    """
    col_names_dir = pd.read_csv(input_cols_dir_path + input_cols_dir_file_name, sep=',')
    col_names = col_names_dir['field_name'].to_list()
    list_str_cols = col_names_dir.loc[(col_names_dir['data_type'] == 'str')]['field_name'].to_list()
    list_measure_vars = col_names_dir.loc[(col_names_dir['data_type'] == 'int')]['field_name'].to_list()
    n_rows = 500000
    dict_dtypes = {x: 'str' for x in list_str_cols}

    for i in range(8):
        skip_rows = i * n_rows + 1 if i != 0 else 0
        dataset = pd.read_table(input_path + input_file_name, delimiter=',', encoding='ISO-8859-1', header=1,
                                dtype=dict_dtypes, skiprows=skip_rows, nrows=n_rows, names=col_names)  # , nrows=50000
        dataset = dataset.loc[((dataset['base_month'].astype('int') >= 202004) &
                           (dataset['base_month'].astype('int') <= 202412))]
        dataset['item_code'] = dataset['item_code'].apply(UserDefinedFn.epcos_cleaning)
        dataset['process_date'] = pd.to_datetime(dataset['process_date'].astype(str).str[4:6] + '/' +
                                                 dataset['process_date'].astype(str).str[6:] + '/' +
                                                 dataset['process_date'].astype(str).str[:4]).dt.strftime("%m/%d/%Y")
        dataset[list_str_cols] = dataset[list_str_cols].fillna('NULL')
        dataset = dataset.groupby(list_str_cols, as_index=False)[list_measure_vars].sum()
        # dataset.to_csv(output_path + 'PoP_Data_' + str(i) + '.csv', index=False)
        print(col_names)
        dataset = dataset[col_names]
        dataset.to_csv(output_path + 'PoP_Data_Monthly_level_' + str(i) + '.csv', sep=',', index=False)

    # ads = dataset.loc[(dataset['region1_name_e'] == 'EUROPE')].reset_index(drop=True)
    # dataset.to_csv(output_path + 'PoP_InScope_Data_' + str(i) + '.csv', sep=',', index=False)
    # summary['base_month_derived'] = pd.to_datetime(
    # summary['process_date'].astype(str).str[0:2] + '/' + summary['process_date'].astype(str).str[-4:]).dt.strftime("%m/%Y")
    dict_dtypes = {x: 'str' for x in list_str_cols}
    pop_monthly_data = pd.DataFrame()
    for i in range(8):
        # i = 1
        print('Appending_file_' + str(i))
        path_with_file_details = output_path + 'PoP_Data_Monthly_level_' + str(i) + '.csv'
        dataset = pd.read_table(path_with_file_details, header=None, delimiter=',', encoding='ISO-8859-1',
                                names=col_names, dtype=dict_dtypes, skiprows=1)
        print(col_names)
        pop_monthly_data = pd.concat([pop_monthly_data, dataset], ignore_index=True,axis=0)
    print("file_Created Successfully and started appending")
    print(pop_monthly_data.shape)
    master_field_df = pd.read_csv(master_field_path + master_field_file_name, sep=',', encoding="utf-8")
    master_field_df = master_field_df.loc[
        master_field_df['table_name'] == master_field_table_name, ['input_field_name', 'tableau_visual_field_name']]
    input_field_list = master_field_df['input_field_name'].to_list()
    tableau_field = master_field_df['tableau_visual_field_name'].to_list()
    req_cols = [x for x in pop_monthly_data.columns.to_list() if x in input_field_list]
    data_revised = pop_monthly_data[req_cols]
    variable_directory = {i: j for i, j in zip(input_field_list, tableau_field) if i in req_cols}
    pop_monthly_data = data_revised.rename(columns=variable_directory)
    pop_monthly_data = pop_monthly_data[tableau_field]
    pop_monthly_data.to_csv(output_path + output_file_name, sep='\t', header=True, index=False)
    print("file_Created Successfully and started appending")

    end = time.time()
    print("The execution time is", str(np.round((end - start)/60, 2)) + ' minutes')


print("Execution started...")

# Declare the arguments and call the function
input_path = "C:\\distribution_dashboard\\data\\inputs\\gmds_sales\\"
input_file_name = time_stamp + "_gmds_sales.txt"
output_path = "C:\\distribution_dashboard\\data\\outputs\\table\\gmds_sales\\"
input_cols_dir_path = "C:\\distribution_dashboard\\data\\inputs\\gmds_sales\\"
input_cols_dir_file_name = "gmds_input_col_dir.csv"
output_file_name = time_stamp + "_gmds_sales.txt"
master_field_path = "C:\\distribution_dashboard\\data\\inputs\\"
master_field_table_name = "gmds"
master_field_file_name = "revised_master_field_temp.csv"

# Calling gmds_data_processor function
gmds_data_processor(input_path, output_path,input_file_name,
                    input_cols_dir_path, input_cols_dir_file_name,
                    output_file_name,master_field_path,
                    master_field_file_name,master_field_table_name)


# Defining the function for data refresh activity of GMDS
def data_refresh_gmds(master_dataset_path:str,master_dataset_file_name:str,
                 new_dataset_path:str,new_dataset_file_name:str,
                 output_path:str,output_file_name:str,
                 previous_refresh_date:str,max_date:str):
    """
    This function helps in data refresh activities bases on specified time period
    from the last refresh to the latest refresh date relies on output of previous
    function
    """
    master_dataset = pd.read_table(master_dataset_path + master_dataset_file_name,delimiter='\t',encoding='ISO-8859-1')
    new_dataset = pd.read_table(new_dataset_path + new_dataset_file_name,delimiter='\t',encoding='ISO-8859-1')
    records = new_dataset.loc[((pd.to_datetime(new_dataset['Process Date']) >= pd.to_datetime(previous_refresh_date)) & (pd.to_datetime(new_dataset['Process Date']) <= pd.to_datetime(max_date))), :]
    master_dataset.loc[((pd.to_datetime(master_dataset['Process Date']) >= pd.to_datetime(previous_refresh_date)) & (pd.to_datetime(master_dataset['Process Date']) <= pd.to_datetime(max_date))), :] = records
    master_dataset.to_csv(output_path + output_file_name, sep='\t',header=True,index=False)
    print("New set of data added till " + time_stamp)


# Declare the arguments and call the function
master_dataset_path = "C:\\distribution_dashboard\\data\\outputs\\table\\gmds_sales\\"
master_dataset_file_name = "20221029_gmds_sales.txt"
new_dataset_path = "C:\\distribution_dashboard\\data\\outputs\\table\\gmds_sales\\"
new_dataset_file_name = time_stamp + "_gmds_sales.txt"
output_path = "C:\\distribution_dashboard\\data\\outputs\\table\\gmds_sales\\"
output_file_name = '20221029' + "_gmds_sales.txt"

data_refresh_gmds(master_dataset_path,master_dataset_file_name,new_dataset_path,new_dataset_file_name,output_path,output_file_name)


# Defining gmds_tabular_generator function for Tabular data generation
def gmds_tabular_generator(master_dataset_path:str,master_dataset_file_name,
                           output_file_path:str,output_file_name:str):
    """"
    This function takes processed GMDS file and returns the last 1 year of data
    for tabular view requirements
    """
    df = pd.read_table(master_dataset_path + master_dataset_file_name,delimiter='\t',encoding='ISO-8859-1')
    start_date = datetime.date.today()-timedelta(days=366)
    cur_date = datetime.date.today()
   # df = df.loc[(df['Term']>125)]
    final_df = df.loc[((pd.to_datetime(df['Process Date']) >= pd.to_datetime(start_date))
                       & (pd.to_datetime(df['Process Date']) <= pd.to_datetime(cur_date))),:]
    final_df.to_csv(output_file_path + output_file_name, sep='\t', header=True, index=False)


# Declare the arguments and call the function
master_dataset_path = "C:\\distribution_dashboard\\data\\outputs\\table\\gmds_sales\\"
master_dataset_file_name = "20221029_gmds_sales.txt"
output_file_path = "C:\\distribution_dashboard\\data\\outputs\\table\\gmds_sales\\"
output_file_name = "tabular_views' + '_gmds_sales.txt"

# Calling the function
gmds_tabular_generator(master_dataset_path,master_dataset_file_name,output_file_path,output_file_name)
