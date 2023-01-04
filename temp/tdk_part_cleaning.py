import pandas as pd
import warnings
warnings.filterwarnings("ignore")

# Reading the GMDS file
input_file_path = 'C://distribution_dashboard//data//inputs//gmds_sales//'
input_file_name = '20221029_gmds_sales.txt'
# target = pd.read_table(input_file_path + input_file_name,sep=',',encoding='ISO-8859-1')


# Defining tdk_item_gmds_cleaning function for TDK part cleaning of GMDS only
def tdk_item_gmds_cleaning(input_file_name_:str, input_file_path_:str, input_price_list_path_:str, input_price_list_file_name_:str):
    """
    This function creates a cross-reference file relying on price
    list.This will be used as a reference for cleaning activity
    """
    tdk_price_list = pd.read_csv(input_price_list_path_ + input_price_list_file_name_)
    reference_file = tdk_price_list.iloc[:, [4, 3]]
    reference_file.columns = ['reference_code', 'cleaned_code']
    tdk_dict = {}
    for i, j in zip(reference_file['reference_code'], reference_file['cleaned_code']):
        tdk_dict[i] = j
    target = pd.read_table(input_file_path_ + input_file_name_, sep=',', encoding='ISO-8859-1')
    target['item_name'] = target['item_name'].map(tdk_dict).fillna(target['item_name'])
    tdk_df = target.loc[-(target['item_code'].str.startswith('B') == True),['item_code', 'item_name']]
    tdk_df = tdk_df[-tdk_df.duplicated()]
    tdk_df_dic = {}
    for i, j in zip(tdk_df['item_code'], tdk_df['item_name']):
        tdk_df_dic[i] = j
    target['item_code'] = target['item_code'].map(tdk_df_dic).fillna(target['item_code'])
    return target


# Declare the arguments and call the function
input_price_list_path = 'C://distribution_dashboard//data//inputs//price_list//'
input_price_list_file_name = 'tdk_price_list.csv'
cleaned_df = tdk_item_gmds_cleaning(input_file_name,input_file_path,input_price_list_path,input_price_list_file_name)


# Calling the function
def tdk_item_cleaning(input_file_name_:str, input_file_path_:str, input_price_list_path_:str, input_price_list_file_name_:str):
    """
    This function creates a cross-reference file relying on price
    list.This will be used as a reference for cleaning activity
    """
    tdk_price_list = pd.read_csv(input_price_list_path_ + input_price_list_file_name_)
    reference_file = tdk_price_list.iloc[:, [4, 3]]
    reference_file.columns = ['reference_code', 'cleaned_code']
    tdk_dict = {}
    for i, j in zip(reference_file['reference_code'], reference_file['cleaned_code']):
        tdk_dict[i] = j
    target = pd.read_table(input_file_path_ + input_file_name_, sep=',', encoding='ISO-8859-1')
    target['item_name'] = target['item_name'].map(tdk_dict).fillna(target['item_name'])
    return target
