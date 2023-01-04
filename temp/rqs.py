import os
import pandas as pd
import glob
import json
from src.python.etl.user_defined_fn import UserDefinedFn
from datetime import date
time_stamp = date.today().strftime("%Y%m%d")


# Function for rqs_quotes processing - this function reads rqs_quotes json files and combines them to a consolidated one
def rqs_file_processor(input_file_path: str,output_file_path: str,
                       input_file_name: str,output_file_name:str,
                       prefix: str,req_output_cols:list):
    # List the files in the directory
    files_list = os.listdir(os.chdir(input_file_path))
    file_ending_name = input_file_name
    file_name_iterator = [i for i in files_list if i.endswith(file_ending_name)]
    # Iterating over the available fines and loading them to JSON function
    for i in range(len(file_name_iterator)):
        data = open(input_file_path + file_name_iterator[i], "r")
        data = json.load(data)
        df_rqs = pd.json_normalize(data, 'items',
                                   ['additionalNotes','amount','applicant','bgApprovalDate','bgApprover','calendarYear',
                                    'contactPerson','csCode','currency','businessGroup','createdAt','customerReference',
                                    'customerName','customerNameInternal','customerNameOfficial','customerIsDistributor',
                                    'customerSapNumber','customerMainCustomerGroup','drActivities','drDetails','drStatus',
                                    'drReference','drStartDate','drEndDate','incoTerms','incoTermsPlace','industrySegment',
                                    'isBudgetary','lastDeliveryAt','legalNotices','mail','noOrderReason','orderStatus',
                                    'osCode','paymentTerms','projectName','posCustomerName','posCountry','quotationCategory',
                                    'quotationPeriod','quotationType','rfqIssueDate','isRunningBiz','salesApprover1Date',
                                    'salesApprover2Date','salesApprover1','salesApprover2','salesDistrict','salesRegion',
                                    'salutation', 'sopYear','sopMonth', 'specification','tdkTerm','tdkReference','url',
                                    'quoteValidFrom','quoteValidUntil'], record_prefix=prefix,errors='ignore')
        # Combining all the resultant csv files to a single one
        df_rqs.to_csv(output_file_path + file_name_iterator[i].replace(".json", ".csv"), index=False)
        print("converting file: "+str(i))
    print("Files Successfully converted to csv")
    data = []
    for csvfile in glob.glob(os.path.join(output_file_path, "*.csv")):
        df = pd.read_csv(csvfile, encoding="utf-8", delimiter=",")
        data.append(df)
    data = pd.concat(data, ignore_index=True)
    data.to_csv(output_file_path + output_file_name,sep=',',header=True,index=False,columns=req_output_cols)
    print("Consolidated file created successfully!")


# Declare the arguments to the function
input_file_path = "C:/distribution_dashboard/data/inputs/rqs/sftp_files/"
output_file_path = "C:/distribution_dashboard/data/outputs/table/rqs/df_rqs/"
input_file_name = "rqs_quotes.json"
output_file_name = time_stamp + "_consolidated_rqs_quotes.txt"
prefix = 'items_'
req_output_cols = ['tdkReference','rfqIssueDate','createdAt','quoteValidFrom','quoteValidUntil',
                   'items_quantity', 'customerSapNumber','customerNameInternal','tdkTerm',
                   'salesRegion','salesDistrict','osCode','csCode','customerIsDistributor',
                   'posCustomerName','posCountry','items_mfrPartNo','amount','drStatus',
                   'items_leadTime','items_price','industrySegment']
# Run the function
rqs_file_processor(input_file_path, output_file_path, input_file_name,
                   output_file_name,prefix,req_output_cols)

# Function that converts the both JSON dp_rfqs files and dp_quotes to csv files
def dp_file_processor(input_file_path: str, output_file_path: str, input_file_name: str,
                      output_file_name: str, prefix: str, ind_cols: list,req_fields=list):
    # List the files in the directory
    files_list = os.listdir(os.chdir(input_file_path))
    file_ending_name = input_file_name
    file_name_iterator = [i for i in files_list if i.endswith(file_ending_name)]
    # Iterating over the available fines and loading them to JSON function
    for i in range(len(file_name_iterator)):
        data = open(input_file_path + file_name_iterator[i], "r")
        data = json.load(data)
        df_dp_rfqs_line_items = pd.json_normalize(data, 'lineItems', ind_cols, record_prefix=prefix, errors='ignore')
        dp = open(input_file_path + file_name_iterator[i], "r")
        dp_load = json.load(dp)
        dp_elements = pd.json_normalize(dp_load)
        cleaned_cols = [i.replace(".","_") if i.find('.') > 0 else i for i in dp_elements.columns.to_list() ]
        dp_elements.columns = cleaned_cols
        dp_elements.drop(columns=['lineItems'],axis=1,inplace=True)
        common_cols = list(set(dp_elements).intersection(set(df_dp_rfqs_line_items)))
        joined_dp_rfqs = pd.merge(dp_elements,df_dp_rfqs_line_items,left_on = common_cols,right_on=common_cols,suffixes=('_x', '_y'),how='left')
        joined_dp_rfqs.to_csv(output_file_path + file_name_iterator[i].replace(".json", ".csv"), index=False)
        print("converting file: " + str(i))
    print("Files Successfully converted to csv")
    # Combining all the resultant csv files to a single one
    data = []
    for csvfile in glob.glob(os.path.join(output_file_path, "*.csv")):
        df = pd.read_csv(csvfile, encoding="utf-8", delimiter=",")
        data.append(df)
    data = pd.concat(data, ignore_index=True)
    data.to_csv(output_file_path + output_file_name, index=False,columns=req_fields)
    print("Consolidated file created successfully!")


# Declare the arguments to the function
input_file_path = "C:/distribution_dashboard/data/inputs/rqs/sftp_files/"
output_file_path = "C:/distribution_dashboard/data/outputs/table/rqs/dp_rfqs/"
ind_cols = ['id', 'reference', 'issued','currency']
input_file_name = "dp_rfqs.json"
output_file_name = time_stamp + "_consolidated_dp_rfqs.txt"
prefix = 'lineItems_'
req_fields = ['id', 'lineItems_mfrPartNo', 'application_type', 'lineItems_branch','issued']


# Run the function for dp_rfqs
dp_file_processor(input_file_path, output_file_path,
                  input_file_name,output_file_name,
                  prefix,ind_cols,req_fields)


# Running the above function for dp_quotes
# Declare the arguments to the function
ind_cols =['id','rfqId','tdkReference','validFrom','validUntil',
           'lastDelivery','isBudgetary','salutation','currency',]
input_file_path = "C:/distribution_dashboard/data/inputs/rqs/sftp_files/"
output_file_path = "C:/distribution_dashboard/data/outputs/table/rqs/dp_quotes/"
input_file_name = "dp_quotes.json"
output_file_name = time_stamp + "_consolidated_dp_quotes.txt"
prefix = 'lineItems_'
req_fields = ['rfqId', 'tdkReference', 'lineItems_mfrPartNo']

# Run the function
dp_file_processor(input_file_path, output_file_path,
                  input_file_name,output_file_name,
                  prefix,ind_cols,req_fields)

# Function for combining dp_quotes and dp_rfqs
def dp_files_combiner(input_file_path_a:str,input_file_path_b:str,
                       input_file_name_a:str,input_file_name_b:str,
                       output_file_path:str,output_file_name:str):
    dp_rfqs = pd.read_csv(input_file_path_a + input_file_name_a, sep=',')
    dp_quotes = pd.read_csv(input_file_path_b + input_file_name_b, sep=',')
    dp_rfqs = pd.merge(left=dp_rfqs, right=dp_quotes, left_on=['id', 'lineItems_mfrPartNo'],right_on=['rfqId', 'lineItems_mfrPartNo'], how='left')
    dp_rfqs.to_csv(output_file_path+output_file_name,sep=',',header=True,index=False)
    print("Combining dp_quotes and dp_rfqs is completed")


input_file_path_a = "C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\dp_rfqs\\"
input_file_path_b = "C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\dp_quotes\\"
input_file_name_a = time_stamp + "_consolidated_dp_rfqs.txt"
input_file_name_b = time_stamp + "_consolidated_dp_quotes.txt"
output_file_path = "C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\dp_rfqs\\"
output_file_name = time_stamp + "_dp_file.txt"

# Run the function
dp_files_combiner(input_file_path_a,input_file_path_b,
                  input_file_name_a,input_file_name_b,
                  output_file_path,output_file_name)


# Function for combining the data sources of dp with rqs system
def rqs_data_processor(input_file_path_a:str,input_file_path_b:str,
                       input_file_name_a:str,input_file_name_b:str,
                       output_file_path:str,output_file_name:str):
    df_rqs = pd.read_csv(input_file_path_a + input_file_name_a, delimiter=',')
    dp_rfqs = pd.read_csv(input_file_path_b + input_file_name_b, delimiter=',')
    df_rqs['items_mfrPartNo'] = df_rqs['items_mfrPartNo'].apply(UserDefinedFn.epcos_cleaning)
    dp_rfqs['lineItems_mfrPartNo'] = dp_rfqs['lineItems_mfrPartNo'].apply(UserDefinedFn.epcos_cleaning)
    df_rqs = pd.merge(left=df_rqs, right=dp_rfqs, left_on=['tdkReference','items_mfrPartNo'],right_on=['tdkReference','lineItems_mfrPartNo'], how='left')
    date_cols = [x for x in df_rqs.columns.to_list() if 'Date' in x]
    date_cols.extend(['issued', 'createdAt', 'quoteValidFrom', 'quoteValidUntil'])
    df_rqs_dates = df_rqs[date_cols]
    df_rqs_dates = df_rqs_dates.astype({i: 'object' for i in date_cols})
    df_rqs_dates = df_rqs_dates.fillna("NA")
    df_rqs_dates = df_rqs_dates.apply(lambda x: pd.to_datetime(x, errors='coerce'))
    df_rqs_dates = df_rqs_dates.apply(lambda x: x.dt.strftime('%Y/%m/%d'))
    df_rqs[date_cols] = df_rqs_dates
    df_rqs.to_csv(output_file_path + output_file_name,sep=',',header=True,index=False)
    print("RQS data merging completed!")


# Run the function
input_file_path_a = "C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\df_rqs\\"
input_file_path_b = "C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\dp_rfqs\\"
input_file_name_a = time_stamp + '_consolidated_rqs_quotes.txt'
input_file_name_b = time_stamp + '_dp_file.txt'
output_file_path = "C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\"
output_file_name = time_stamp + "_rqs_data.txt"

# Call the Function
rqs_data_processor(input_file_path_a, input_file_path_b,input_file_name_a, input_file_name_b,output_file_path,output_file_name)


# Defining the hierarchy retriever function
def hierarchy_retriever(input_file_path:str,input_file_name:str,
                        hierarchy_file_path:str,hierarchy_file_name:str,
                        output_file_path:str,output_file_name:str):
    product_hierarchy = pd.read_csv(hierarchy_file_path + hierarchy_file_name, delimiter=',', encoding='ISO-8859-1')
    df_rqs = pd.read_csv(input_file_path + input_file_name, delimiter=',')
    df_rqs_final = pd.merge(left=df_rqs, right=product_hierarchy, left_on='items_mfrPartNo',
                            right_on='Item Code')
    df_rqs_final.to_csv(output_file_path + output_file_name,sep=',',header=True, index=False)
    print("product hierarchy variables added!")


# Define the arguments of the function
input_file_path = "C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\"
input_file_name = time_stamp+"_rqs_data.txt"
hierarchy_file_path = "C:\\distribution_dashboard\\data\\inputs\\hierarchy_files\\product_hierarchy\\"
hierarchy_file_name = "product_hierarchy_revised.csv"
output_file_path = "C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\"
output_file_name = time_stamp + "master_rqs_data.txt"

# Call the function
hierarchy_retriever(input_file_path,input_file_name,hierarchy_file_path,hierarchy_file_name,output_file_path,output_file_name)


# Function for generating the reporting table
def rt_generator(input_file_path:str,input_file_name:str,
                 master_field_path:str,master_field_file_name:str,
                 master_field_table_name:str, output_file_path:str,
                 output_file_name:str):
    df_rqs = pd.read_csv(input_file_path+input_file_name, delimiter=',', encoding='ISO-8859-1')
    master_field_df = pd.read_csv(master_field_path + master_field_file_name, sep=',', encoding="utf-8")
    master_field_df = master_field_df.loc[master_field_df['table_name'] == master_field_table_name,['input_field_name', 'tableau_visual_field_name']]
    input_field_list = master_field_df['input_field_name'].to_list()
    tableau_field = master_field_df['tableau_visual_field_name'].to_list()
    req_cols = [x for x in df_rqs.columns.to_list() if x in input_field_list]
    data_revised = df_rqs[req_cols]
    variable_directory = {i: j for i, j in zip(input_field_list, tableau_field) if i in req_cols}
    df = data_revised.rename(columns=variable_directory)
    #df = df[tableau_field]
    df.to_csv(output_file_path+output_file_name, sep='\t',header=True, index=False)
    print("RT RQS created successfully!")

# Declare the arguments to the function
input_file_path = "C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\"
input_file_name = time_stamp + "_rqs_data.txt"
master_field_path = "C:\\distribution_dashboard\\data\\inputs\\"
master_field_file_name = "revised_master_field_temp.csv"
master_field_table_name = "rqs"
output_file_path = "C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\"
output_file_name = time_stamp + "_master_rqs_data.txt"

# Run the function

rt_generator(input_file_path,input_file_name,
                 master_field_path,master_field_file_name,
                 master_field_table_name, output_file_path,
                 output_file_name)

# Function for combining the master data with the latest data increments
def rqs_data_refresh(input_file_path_:str,input_file_name_a:str,
                  input_file_name_b:str,output_file_path_:str,
                  output_file_name_:str):
    master_df = pd.read_csv(input_file_path_ + input_file_name_a, delimiter=',', encoding='ISO-8859-1')
    new_df = pd.read_csv(input_file_path_ + input_file_name_b, delimiter=',', encoding='ISO-8859-1')
    master_df_rqs = pd.concat([master_df,new_df],axis=0)
    master_df_rqs.to_csv(output_file_path_ + output_file_name_,sep=',',encoding='ISO-8859-1',index=False)
    print("combining the new data and old data completed!")





