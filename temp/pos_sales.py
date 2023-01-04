#Importing all packages and functions
import pandas as pd
import time
from src.python.etl.user_defined_fn import UserDefinedFn
# Read the text file
col_names = ["base_month","item_code","invoice_date",
             "sap_customer","customer_name","bg",
             "bg_name","cbu","cbu_name","sbu","sbu_name",
             "e-cust_name","e-cust_country","e-cust_zip_code",
             "sales_qty","sales_amount","sales_price",
             "to_paired_qty","to_paired","pur_price",
             "gm_abs._calc.","gm_%","past_12_month_sales_amt","past_12_month_sales_qty"]

len(col_names)

lst_str_cols = ["base_month","item_code","invoice_date",
                "sap_customer","customer_name","bg",
                "bg_name","cbu","cbu_name","sbu","sbu_name",
                "e-cust_name","e-cust_country","e-cust_zip_code"]

dict_dtypes = dict_dtypes = {x: 'str' for x in lst_str_cols}

lst_num_cols = ["sales_qty","sales_amount",
                "sales_price","to_paired_qty","to_paired",
                "pur_price","gm_abs._calc.","gm_%",
                "past_12_month_sales_amt","past_12_month_sales_qty"]

input_df_path = "C:\\distribution_dashboard\\data\\inputs\\pos_sales\\"
output_df_path = "C:\\distribution_dashboard\\data\\outputs\\"
input_master_field_path = "C:\\distribution_dashboard\\data\\inputs"

# Merging Activity for PoS Data
dict_dtypes = dict_dtypes = {x: 'str' for x in lst_str_cols}
region_df = pd.read_excel(input_df_path + '/end_cust_country_codes.xlsx')
region_df['region_code'] = pd.Series(region_df['Region(Allocated)'].str.split(":", expand=True)[0])
region_df['region_name'] = pd.Series(region_df['Region(Allocated)'].str.split(":", expand=True)[1])
dataset = pd.read_table(input_df_path + '/pos_sales.txt', header=None,delimiter='\t',encoding='ISO-8859-1',names = col_names,dtype= dict_dtypes,error_bad_lines=False)
dataset['cleaned_part_number'] = dataset['item_code'].apply(UserDefinedFn.epcos_cleaning)
dataset = pd.merge(dataset, region_df, how='left', left_on=['e-cust_country'], right_on=['E-Cust Country'])
print(dataset.shape)
dataset.to_csv(input_df_path+"revised_pos.txt")
print("done")

# Creating a revised file
input_df_path = "C:\\distribution_dashboard\\data\\inputs\\pos_sales\\"
output_df_path = "C:\\distribution_dashboard\\data\\outputs\\"
input_master_field_path = "C:\\distribution_dashboard\\data\\inputs"

# Choosing the columns
col_names = list(dataset.columns)
lst_str_cols = list(dataset.select_dtypes(include='object').columns)
lst_num_cols = list(dataset.select_dtypes(include='float64').columns)
initial_date_cut_off = '20200101'


def rt_generator(input_df_path:str,output_df_path:str,
                 input_master_field_path:str,str_cols:list,
                 headers:list, group_by_vars:list, measure_vars: list):
    dict_dtypes = {x: 'str' for x in lst_str_cols}
    pos_sales_revised = pd.read_table(input_df_path+'/revised_pos.txt',sep=',',encoding='ISO-8859-1',names = headers,dtype= dict_dtypes,skiprows=1)
    pos_sales_revised = pos_sales_revised.loc[(pd.to_datetime(dataset['invoice_date'],format = "%Y%m%d")>pd.to_datetime(initial_date_cut_off))] #Time filtering
    pos_sales_revised = pos_sales_revised.groupby(str_cols, as_index=False)[measure_vars].sum()
    master_field_df = pd.read_csv(input_master_field_path+'/revised_master_field_name.csv')
    master_field_df = master_field_df.loc[master_field_df['table_name'] == 'pos_sales',['output_field_name','tableau_visual_field_name']]
    output_field_list = master_field_df['output_field_name'].to_list()
    tableau_field = master_field_df['tableau_visual_field_name'].to_list()
    req_cols = [x for x in pos_sales_revised.columns.to_list() if x in output_field_list]
    pos_sales_revised = pos_sales_revised[req_cols]
    variable_directory = {i: j for i, j in zip(output_field_list, tableau_field) if i in req_cols}
    print(variable_directory)
    pos_sales_revised = pos_sales_revised.rename(columns = variable_directory)
    pos_sales_revised['Year'] = pd.to_datetime(pos_sales_revised['Time Period'], format='%Y%m').dt.year
    #pos_sales_revised['Year Classifier'] = pos_sales_revised['Year'].apply(lambda x: 'Current Year' if pos_sales_revised['Year'] - 2022 == 0 else pos_sales_revised['Year'] - 2022 ==1 "Previous Year" )
    print("RT Table Generation started...")
    pos_sales_revised.to_csv(output_df_path +  '/table/20220803_pos_sales_rt.txt',sep = '\t', header = True, index=False)
    print(pos_sales_revised.shape)
    print("RT Table Generation Completed Successfully!!")
    return pos_sales_revised


st = time.time()
final_df = rt_generator(input_df_path,output_df_path,input_master_field_path,lst_str_cols,col_names,lst_str_cols,lst_num_cols)
et = time.time()
print("The number of seconds to execute the program" , str(round(et-st)) + ' seconds')


