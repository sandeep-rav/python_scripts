# Import the packages and functions
import pandas as pd
import glob,os
from datetime import date
from src.python.etl.user_defined_fn import UserDefinedFn
import warnings
warnings.filterwarnings("ignore")
# timestamp
time_stamp = date.today().strftime('%Y%m%d')

# Defining the function for Excel to csv conversion
def excel_to_csv_converter(input_files_directory : str,output_file_path:str,master_file_name:str,time_stamp = date.today().strftime('%Y%m%d')):
    for xls_file in glob.glob(os.path.join(input_files_directory, "*.xls*")):
        print("Converting xlsx files..")
        data_xls = pd.read_excel(xls_file, index_col=None)
        csv_file = os.path.splitext(xls_file)[0] + ".csv"
        data_xls.to_csv(csv_file, encoding='utf-8', index=False)
    print("Converting to csv files completed.")
    print("Combining the csv files activity started..")
    csvs = [i for i in glob.glob(os.path.join(input_files_directory, "*.csv*"))]
    combined_csv = pd.concat([pd.read_csv(f, sep=",", encoding='ISO-8859-1') for f in csvs], axis=0)
    combined_csv.reset_index(drop=True,inplace= True)
    combined_csv.to_csv(output_file_path+time_stamp+master_file_name,index = False)
    print("Function executed successfully!")

# Submit the args of the function
input_files_directory = 'C:\\distribution_dashboard\\data\\inputs\\rqs\\latest\\raw_files\\'
output_file_path = 'C:\\distribution_dashboard\\data\\inputs\\rqs\\latest\processed_files\\'
master_file_name = 'rqs_data.csv'

# Call and Execute the function
excel_to_csv_converter(input_files_directory,output_file_path,master_file_name)


# Call and Execute the hierarchy function for bringing in product and customer hierarchy to the target dataset
def hierarchy_retriever(input_path_l:str,input_path_r:str,
                        output_path:str,output_file_name:str,
                        dataset_l:str,dataset_r:str,columns_l:list,
                        columns_r:list,join_type:str):
    l = pd.read_csv(input_path_l + dataset_l + '.csv')
    r = pd.read_csv(input_path_r + dataset_r + '.csv')
    combined = pd.merge(left = l, right = r, how = join_type, left_on = columns_l, right_on = columns_r)
    combined.to_csv(output_path + output_file_name + '.csv',index = False)
    print("Hierarchy columns retrieved.")


input_path_l = 'C:\\distribution_dashboard\\data\\inputs\\rqs\\latest\\processed_files\\'
input_path_r = 'C:\\distribution_dashboard\\data\\inputs\\hierarchy_files\\product_hierarchy\\'
output_path = 'C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\'
output_file_name = 'RQS_revised'
dataset_l = '20221018rqs_data'
dataset_r = 'hierarchy_revised'
columns_l = ['TDK Part No.']
columns_r = ['Item Code']
join_type = 'left'

hierarchy_retriever(input_path_l,input_path_r,output_path,output_file_name,dataset_l,dataset_r,columns_l,columns_r,
                    join_type)

# Reporting Table generation
def rt_generator(input_path : str, output_path : str,master_field_path : str,master_field_file_name : str,
                 input_file_name : str,output_file_name : str,table_name : str):
    master_field_df = pd.read_csv(master_field_path + master_field_file_name,encoding = "utf-8")
    input_data = pd.read_csv(input_path + input_file_name,sep = ',',encoding = "ISO-8859-1")
    master_field_df = master_field_df.loc[master_field_df['table_name'] == table_name, ['input_field_name', 'tableau_visual_field_name']]
    input_field_list = master_field_df['input_field_name'].to_list()
    tableau_field = master_field_df['tableau_visual_field_name'].to_list()
    req_cols = [x for x in input_data.columns.to_list() if x in input_field_list]
    data_revised = input_data[req_cols]
    variable_directory = {i: j for i, j in zip(input_field_list, tableau_field) if i in req_cols}
    data_revised['TDK Part No.'] = data_revised['TDK Part No.'].apply(UserDefinedFn.epcos_cleaning)
    data_revised = data_revised.rename(columns=variable_directory)
    data_revised = data_revised[tableau_field]
    data_revised.to_csv(output_path + output_file_name + '.csv',sep = ',',index = False,header= True)
    print(data_revised.shape)


input_path = 'C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\'
output_path = 'C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\'
master_field_path = 'C:\\distribution_dashboard\\data\\inputs\\'
master_field_file_name = 'revised_master_field_temp.csv'
input_file_name = 'RQS_revised.csv'
output_file_name = 'RT_RQS_'
table_name = 'rqs'

# Call the Function
rt_generator(input_path, output_path, master_field_path,
             master_field_file_name, input_file_name, output_file_name, table_name)





