import os
import pandas as pd
import glob
import json


# Python program to read JSON File

mapping_keys_list = ['mfrPartNo','price','priceUnit','targetPrice',
'quantity', 'leadTime','totalPrice', 'totalTargetPrice','mdq',
'mpq','moq','customerPartNo','id']

# Defining the function to process the JSON file


def json_file_processor(input_path : str,input_file_name : str ,output_path : str,
                     output_file_name : str,parent_mapping_key : str,
                     key_list_mapping : list, prefix : str):
    # Opening JSON file
    f = open(input_path+"/"+input_file_name+".json","r")
    # Loading JSON file to object
    data = json.load(f)
    # Creating pandas dataframe on JSON object
    data = pd.json_normalize(data, parent_mapping_key, key_list_mapping,
                             record_prefix = prefix, errors ='ignore')
    # Writing the file to the specified path
    data.to_csv(output_path + "/" + output_file_name  + ".csv", index=False)
    print("Created the CSV file")
    # return data

# Providing the inputs here


input_path = 'C:/distribution_dashboard/data/inputs/rqs/sftp_files/'
input_file_name = 'rqs_quotes_all'
output_path = 'C:/distribution_dashboard/data/outputs/table/rqs'
output_file_name = '20220803_rqs_converted'
key_list_mapping = mapping_keys_list
parent_mapping_key = 'items'
prefix = 'items_'


# Running the function
json_file_processor(input_path,input_file_name,output_path,output_file_name,parent_mapping_key,key_list_mapping,prefix)


# Json Processing for DP rfqs


def json_nested_file_processor(input_path : str,output_path : str, input_file_name: str,output_file_name: str,col_to_drop : str,nested_col: str):
    dp = open(input_path+"/"+input_file_name+".json","r")
    dp_load = json.load(dp)
    dp = pd.json_normalize(dp_load)
    meta_fields = dp.columns.to_list()
    final_df = pd.json_normalize(dp_load, record_path=[nested_col],
                                 meta=meta_fields, errors='ignore')
    final_df.drop(columns=[col_to_drop], inplace=True)
    cleaned_cols = [i.replace('.','_') if i.find('.') > 0 else i for i in final_df.columns.to_list()]
    final_df.columns = cleaned_cols
    final_df.to_csv(output_path+"/"+output_file_name+".csv", index=False)

#-------------------------------------------------------------------------------------------------#

# For RQS formatted file


def json_file_processor(input_path : str,output_path : str, input_file_name: str,output_file_name: str,col_to_drop : str,nested_col: str):
    dp = open(input_path+"/"+input_file_name+".json","r")
    dp_load = json.load(dp)
    dp = pd.json_normalize(dp_load)
    meta_fields = dp.columns.to_list()
    final_df = pd.json_normalize(dp_load, record_path=[nested_col],
                                 meta=meta_fields, errors='ignore')
    final_df.drop(columns=[col_to_drop], inplace=True)
    final_df.to_csv(output_path + "/" + output_file_name + ".csv", index=False)


input_path = 'C:/distribution_dashboard/data/inputs/rqs'
input_file_name = '20220803_dp_quotes_formatted'
output_path = 'C:/distribution_dashboard/data/outputs/table/rqs'
output_file_name = '20220803_rqs_converted'

json_file_processor(input_path,output_path,input_file_name,output_file_name,'items','items')

# for the second set of files

dp = open("C:/distribution_dashboard/data/inputs/rqs/20220803_dp_rfqs_formatted.json", "r")
dp_load = json.load(dp)
#dp = pd.json_normalize(dp_load)
#meta_fields = dp.columns.to_list()
final_df = pd.json_normalize(dp_load)
#final_df.drop(columns=['lineItems'],inplace=True)
#cleaned_cols = [ i.replace(".","_") if i.find('.') > 0 else i  for i in final_df.columns.to_list() ]
#final_df.columns = cleaned_cols
#final_df.to_csv('C:/distribution_dashboard/data/outputs/table/rqs/20220803_dp_rfqs_formatted.csv',index = False)

lneitems_list = final_df['lineItems']
line_items = pd.json_normalize(final_df['lineItems'])

#-------------------------------------------------------------------------------------------------#

# Json Processing for DP quotes
dp = open("C:/distribution_dashboard/data/inputs/rqs/20220803_dp_quotes_formatted.json", "r")
dp_load = json.load(dp)
dp = pd.json_normalize(dp_load)
# lineitems = pd.json_normalize(dp)
meta_fields = dp.drop(columns='lineItems').columns.to_list()
final_df = pd.json_normalize(dp_load,record_path = ['lineItems'],
                             meta = meta_fields)
final_df.drop(columns=['lineItems'],inplace=True)
cleaned_cols = [ i.replace(".","_") if i.find('.') > 0 else i  for i in final_df.columns.to_list() ]
final_df.columns = cleaned_cols
final_df.to_csv('C:/distribution_dashboard/data/outputs/table/rqs/20220803_dp_quotes_formatted.csv',index = False)

# JSON file processing structure

data = {
    "number": "",
    "date": "18.08.2012",
    "name": "Test",
    "locations": [
        {
            "name": "Spital am Pyhrn Bahnhof",
            "arrTime": "06:30",
            "depTime": "06:32",
            "track": "12"
        },
        {
            "name": "Windischgarsten Bahnhof",
            "arrTime": "06:37",
            "depTime": "06:40",
            "track": "9"
        },
        {
            "name": "Linz/Donau Hbf",
            "arrTime": "08:24",
            "depTime": "08:30",
            "track": "18"
        }
    ],
    "list_2": [
        {
            "name": "Spital am Pyhrn Bahnhof",
            "arrTime": "06:30",
            "depTime": "06:32"
        },
        {
            "name": "Windischgarsten Bahnhof",
            "arrTime": "06:37",
            "depTime": "06:40"
        },
        {
            "name": "Linz/Donau Hbf",
            "arrTime": "08:24",
            "depTime": "08:30"
        }
    ]


}

df_location = pd.json_normalize(data, 'locations', ['date', 'number', 'name'], record_prefix='locations_')
df_test = pd.json_normalize(data, 'list_2', ['date', 'number', 'name'], record_prefix='test_')

# Current Working Section 11:34 AM

# Code for rqs

data = open("C:/distribution_dashboard/data/inputs/rqs/rqs_quotes_formatted.json", "r")
data = json.load(data)
df_rqs = pd.json_normalize(data, 'items', ['createdAt', 'businessGroup', 'tdkReference',
                                           'customerReference','customerName','customerIsDistributor',
                                           'customerSapNumber','customerMainCustomerGroup','projectName',
                                           'drStatus','posCustomerName','isBudgetary',
                                           'lastDeliveryAt','quoteValidFrom','quoteValidUntil','currency'], record_prefix='items_')
df_rqs = df_rqs.to_csv("C:/distribution_dashboard/data/outputs/table/rqs/20220803_df_rqs.csv",index=False)

# Code for rqs ends

# Code for dp_rfqs

data = open("C:/distribution_dashboard/data/inputs/rqs/20220803_dp_rfqs_formatted.json", "r")
data = json.load(data)
df_dp_rfqs_line_items = pd.json_normalize(data, 'lineItems', ['id', 'reference', 'issued',
                                               'currency'], record_prefix='lineItems_',errors='ignore')
dp = open("C:/distribution_dashboard/data/inputs/rqs/20220803_dp_rfqs_formatted.json", "r")
dp_load = json.load(dp)
dp_elements = pd.json_normalize(dp_load)
cleaned_cols = [ i.replace(".","_") if i.find('.') > 0 else i  for i in dp_elements.columns.to_list() ]
dp_elements.columns = cleaned_cols
dp_elements.drop(columns = ['lineItems'],axis=1,inplace=True)


common_cols = list(set(dp_elements).intersection(set(df_dp_rfqs_line_items)))

joined_dp_rfqs = pd.merge(dp_elements,df_dp_rfqs_line_items,left_on = ['currency', 'id', 'issued', 'reference'],
         right_on=['currency', 'id', 'issued', 'reference'],suffixes=('_x', '_y'),how = 'left')

joined_dp_rfqs.to_csv("C:/distribution_dashboard/data/outputs/table/rqs/20220803_dp_rfqs_formatted.csv",index=False)

# Code for dp_rfqs ends

# Code for dp_quotes
dp = open("C:/distribution_dashboard/data/inputs/rqs/20220803_dp_quotes_formatted.json", "r")
dp_load = json.load(dp)
dp_elements = pd.json_normalize(dp_load)
cleaned_cols = [ i.replace(".","_") if i.find('.') > 0 else i  for i in dp_elements.columns.to_list() ]
dp_elements.columns = cleaned_cols
dp_elements.drop(columns = ['lineItems'],axis=1,inplace=True)

# For lineitems

data = open("C:/distribution_dashboard/data/inputs/rqs/20220803_dp_quotes_formatted.json", "r")
data = json.load(data)
df_dp_quotes_line_items = pd.json_normalize(data,'lineItems',['id','rfqId','tdkReference','validFrom','validUntil',
                                                     'lastDelivery','isBudgetary','salutation','currency',
                                                     ],errors='ignore',record_prefix='lineItems_')

common_keys = list(set(dp_elements).intersection(set(df_dp_quotes_line_items)))

joined_dp_quotes = pd.merge(dp_elements,df_dp_quotes_line_items,
                            left_on =common_keys,
                            right_on=common_keys,
                            suffixes=('_x', '_y'),how = 'left')

joined_dp_quotes.to_csv("C:/distribution_dashboard/data/outputs/table/rqs/20220803_dp_quotes_formatted.csv",index=False)
# Code for dp_quotes ends

# ['currency','id','isBudgetary','lastDelivery','rfqId','salutation','tdkReference', 'validFrom','validUntil']

# Function to read rfqs files
# Review change directory without
# Column names to be changed to csv or JSON format
# Separate function for consolidating the files
# Revise the input and output paths to be short

main_directory = os.chdir('C:/distribution_dashboard/data/inputs/rqs/sftp_files')  # variable for directory change
# List the files in the directory
files_list = os.listdir(main_directory)
file_ending_name = 'rqs_quotes.json'
file_name_iterator = [i for i in files_list if i.endswith(file_ending_name)]  # variable here to replace
len(file_name_iterator)


# RQS_File_processor_function


def rqs_file_processor(input_path: str,output_path: str,file_name: str,prefix: str):
    main_directory = os.chdir(input_path)
    # List the files in the directory
    files_list = os.listdir(main_directory)
    file_ending_name = file_name
    file_name_iterator = [i for i in files_list if i.endswith(file_ending_name)]
    for i in range(len(file_name_iterator)):
        data = open( input_path + file_name_iterator[i], "r")
        data = json.load(data)
        df_rqs = pd.json_normalize(data, 'items',
                                   ['createdAt', 'businessGroup', 'tdkReference',
                                    'customerReference', 'customerName', 'customerIsDistributor',
                                    'customerSapNumber', 'customerMainCustomerGroup', 'projectName',
                                    'drStatus', 'posCustomerName', 'isBudgetary',
                                    'lastDeliveryAt', 'quoteValidFrom', 'quoteValidUntil', 'currency'],
                                   record_prefix=prefix)
        df_rqs.to_csv(output_path + file_name_iterator[i].replace(".json", ".csv"),index=False)
        print("converting file: "+str(i))
    print("Files Successfully converted to csv")
    data = []
    for csvfile in glob.glob(os.path.join(output_path, "*.csv")):
        df = pd.read_csv(csvfile, encoding="utf-8", delimiter=",")
        data.append(df)
    data = pd.concat(data, ignore_index=True)
    data.to_csv(output_path + "consolidated_df_rqs.csv", index=False)
    print("Consolidated file created successfully!")

# Declare the arguments to the function


input_path = "C:/distribution_dashboard/data/inputs/rqs/sftp_files/"
output_path = "C:/distribution_dashboard/data/outputs/table/rqs/df_rqs/"
file_name = "rqs_quotes.json"
prefix = 'items_'
# Run the function
rqs_file_processor(input_path, output_path, file_name, prefix)


# Code for dp_rfqs
# dp_rfqs_processor functions


def dp_file_processor(input_path: str,output_path: str,file_name: str,output_file_name: str,prefix: str,ind_cols : list):
    main_directory = os.chdir(input_path)
    # List the files in the directory
    files_list = os.listdir(main_directory)
    file_ending_name = file_name
    file_name_iterator = [i for i in files_list if i.endswith(file_ending_name)]
    for i in range(len(file_name_iterator)):
        data = open(input_path + file_name_iterator[i], "r")
        data = json.load(data)
        df_dp_rfqs_line_items = pd.json_normalize(data, 'lineItems', ind_cols, record_prefix = prefix ,errors= 'ignore')
        dp = open( input_path + file_name_iterator[i], "r")
        dp_load = json.load(dp)
        dp_elements = pd.json_normalize(dp_load)
        cleaned_cols = [i.replace(".","_") if i.find('.') > 0 else i  for i in dp_elements.columns.to_list() ]
        dp_elements.columns = cleaned_cols
        dp_elements.drop(columns = ['lineItems'],axis=1,inplace=True)
        common_cols = list(set(dp_elements).intersection(set(df_dp_rfqs_line_items)))
        joined_dp_rfqs = pd.merge(dp_elements,df_dp_rfqs_line_items,left_on = common_cols,right_on = common_cols,suffixes=('_x', '_y'),how = 'left')
        joined_dp_rfqs.to_csv(output_path + file_name_iterator[i].replace(".json", ".csv"),index=False)
        print("converting file: "+str(i))
    print("Files Successfully converted to csv")
    data = []
    for csvfile in glob.glob(os.path.join(output_path, "*.csv")):
        df = pd.read_csv(csvfile, encoding="utf-8", delimiter=",")
        data.append(df)
    data = pd.concat(data, ignore_index=True)
    data.to_csv(output_path + output_file_name+".csv", index=False)
    print("Consolidated file created successfully!")

# Declare the arguments to the function


input_path = "C:/distribution_dashboard/data/inputs/rqs/sftp_files/"
output_path = "C:/distribution_dashboard/data/outputs/table/rqs/dp_rfqs/"
ind_cols = ['id', 'reference', 'issued','currency']
file_name = "dp_rfqs.json"
output_file_name = "consolidated_dp_rfqs"
prefix = 'lineItems_'
# Run the function
dp_file_processor(input_path, output_path, file_name,output_file_name, prefix,ind_cols)

# Code for dp_rfqs ends

# Code for dp_quotes
# Declare the arguments to the function
ind_cols =['id','rfqId','tdkReference','validFrom','validUntil',
           'lastDelivery','isBudgetary','salutation','currency',]
input_path = "C:/distribution_dashboard/data/inputs/rqs/sftp_files/"
output_path = "C:/distribution_dashboard/data/outputs/table/rqs/dp_quotes/"
file_name = "dp_quotes.json"
output_file_name = "consolidated_dp_quotes"
prefix = 'lineItems_'

# Run the function
dp_file_processor(input_path, output_path, file_name,output_file_name,prefix,ind_cols)


# Code for dp_quotes ends


# Reading the consolidated files

input_file_path = 'C:/distribution_dashboard/data/outputs/table/rqs/dp_rfqs'
file_name = 'consolidated_df_rqs.csv'
rqs = pd.read_csv(input_file_path + "/" + file_name,encoding="utf-8", delimiter=",")
dp_quotes = pd.read_csv(input_file_path + "/" + 'consolidated_dp_quotes.csv',encoding="utf-8", delimiter=",")
dp_rfqs = pd.read_csv(input_file_path + "/" + 'consolidated_dp_rfqs.csv',encoding="utf-8", delimiter=",")

rqs.shape
dp_quotes.shape
dp_rfqs.shape

req_cols = list(set(dp_rfqs.columns).difference(set(dp_quotes.columns)))
req_cols.extend(['lineItems_rfqScaleItemId'])

# req_cols.extend(['id','lineItems_rfqScaleItemId'])


len(list(set(dp_rfqs['lineItems_rfqScaleItemId']).intersection(set(dp_quotes['lineItems_rfqScaleItemId']))))
# Take the primary keys

quotes_rfqs_merged = pd.merge(dp_quotes,dp_rfqs[req_cols],left_on= ['lineItems_rfqScaleItemId']
         ,right_on= ['lineItems_rfqScaleItemId'],how='left')

quotes_rfqs_merged.to_csv("C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\quotes_rfqs_merged.csv",index= False)

# Merging with rqs.columns
req_cols = list(set(quotes_rfqs_merged.columns).difference(set(rqs.columns)))
req_cols
dp_quotes.columns
len(list(set(dp_rfqs['id']).intersection(set(rqs['items_id']))))

quotes_rfqs_rqs_merged = pd.merge(rqs,quotes_rfqs_merged[req_cols],left_on= ['items_id']
         ,right_on= ['id'],how='left')

quotes_rfqs_rqs_merged.to_csv("C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\quotes_rfqs_rqs_merged.csv",index= False)


# Workspace for Testing pls delete after run

gmds = pd.read_table('C:\\distribution_dashboard\\data\\outputs\\table\\gmds_sales\\20220728_GMDS_Monthly.txt')
len(pd.unique(gmds['Main Customer Group Name']))
cust_hierarchy = gmds[['Main Customer Group Name','PoP Customer Name']]
cust_hierarchy.duplicated().sum()
cust_hierarchy = cust_hierarchy[-cust_hierarchy.duplicated()]
cust_hierarchy.to_csv('C:\\distribution_dashboard\\data\\outputs\\table\\gmds_sales\\cust_hierarchy.csv',index = False)

input_path

f = open(input_path + "/rqs_quotes_all.json")

# returns JSON object as
# a dictionary
data = json.load(f)
data
# Iterating through the json
# list
for i in data['emp_details']:
    print(i)

# Closing file
f.close()

data[2]