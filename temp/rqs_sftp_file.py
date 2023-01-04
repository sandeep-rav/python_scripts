import os
import pandas as pd
import glob
import json
from src.python.etl.user_defined_fn import UserDefinedFn
from datetime import date
time_stamp = date.today().strftime("%Y%m%d")


# Function reading and processing rqs_quotes.json files
def rqs_file_processor(input_path: str, output_path: str, input_file_name: str,
                       output_file_name_:str,prefix_: str):
    files_list = os.listdir(os.chdir(input_path))
    file_ending_name = input_file_name
    file_name_iterator = [i for i in files_list if i.endswith(file_ending_name)]
    for i in range(len(file_name_iterator)):
        data = open(input_path + file_name_iterator[i], "r")
        data = json.load(data)
        df_rqs = pd.json_normalize(data, 'items',
                                   ['additionalNotes','amount','applicant','bgApprovalDate','bgApprover','calendarYear',
                                    'contactPerson','csCode','currency','businessGroup','createdAt','customerReference',
                                    'customerName','customerNameInternal','customerNameOfficial','customerIsDistributor',
                                    'customerSapNumber','customerMainCustomerGroup','drActivities','drDetails','drStatus',
                                    'drReference', 'drStartDate','drEndDate','incoTerms','incoTermsPlace','industrySegment', 
                                    'isBudgetary','lastDeliveryAt','legalNotices','mail','noOrderReason','orderStatus',
                                    'osCode','paymentTerms','projectName','posCustomerName','posCountry','quotationCategory', 
                                    'quotationPeriod','quotationType','rfqIssueDate','isRunningBiz','salesApprover1Date',
                                    'salesApprover2Date','salesApprover1','salesApprover2','salesDistrict','salesRegion',
                                    'salutation','sopYear','sopMonth','specification','tdkTerm','tdkReference','url',
                                    'quoteValidFrom','quoteValidUntil'], record_prefix=prefix_)
        df_rqs.to_csv(output_path_ + file_name_iterator[i].replace(".json", ".csv"), index=False)
        print("converting file: "+str(i))
    print("Files Successfully converted to csv")
    data = []
    for csvfile in glob.glob(os.path.join(output_path, "*.csv")):
        df = pd.read_csv(csvfile, encoding="utf-8", delimiter=",")
        data.append(df)
    data = pd.concat(data, ignore_index=True)
    data.to_csv(output_path + output_file_name, index=False)
    print("Consolidated file created successfully!")


# Declare the arguments to the function
input_path = "C:\\distribution_dashboard\\data\\inputs\\rqs\\sftp_files\\"
output_path = "C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\df_rqs\\"
input_file_name = "rqs_quotes.json"
output_file_name = time_stamp + "_consolidated_rqs_quotes.csv"
prefix = 'items_'

# Call the function
rqs_file_processor(input_path, output_path, input_file_name,
                   output_file_name,prefix)


# Function for reading and processing dp_rfqs.json files
def dp_file_processor(input_path:str, output_path:str, input_file_name:str,
                      output_file_name:str, prefix_: str, ind_cols :list):
    # List the files in the directory
    files_list = os.listdir(os.chdir(input_path))
    file_ending_name = input_file_name
    file_name_iterator = [i for i in files_list if i.endswith(file_ending_name)]
    for i in range(len(file_name_iterator)):
        data = open(input_path + file_name_iterator[i], "r")
        data = json.load(data)
        df_dp_rfqs_line_items = pd.json_normalize(data, 'lineItems', ind_cols, record_prefix=prefix_, errors='ignore')
        dp = open(input_path + file_name_iterator[i], "r")
        dp_load = json.load(dp)
        dp_elements = pd.json_normalize(dp_load)
        cleaned_cols = [i.replace(".","_") if i.find('.') > 0 else i for i in dp_elements.columns.to_list() ]
        dp_elements.columns = cleaned_cols
        dp_elements.drop(columns=['lineItems'],axis=1,inplace=True)
        common_cols = list(set(dp_elements).intersection(set(df_dp_rfqs_line_items)))
        joined_dp_rfqs = pd.merge(dp_elements,df_dp_rfqs_line_items,left_on = common_cols,right_on=common_cols,suffixes=('_x', '_y'),how='left')
        joined_dp_rfqs.to_csv(output_path + file_name_iterator[i].replace(".json", ".csv"), index=False)
        print("converting file: " + str(i))
    print("Files Successfully converted to csv")
    data = []
    for csvfile in glob.glob(os.path.join(output_path, "*.csv")):
        df = pd.read_csv(csvfile, encoding="utf-8", delimiter=",")
        data.append(df)
    data = pd.concat(data, ignore_index=True)
    data.to_csv(output_path + output_file_name + ".csv", index=False)
    print("Consolidated file created successfully!")


# Declare the arguments to the function
input_path = "C:\\distribution_dashboard\\data\\inputs\\rqs\\sftp_files\\"
output_path = "C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\dp_rfqs\\"
ind_cols = ['id', 'reference', 'issued','currency']
input_file_name = "dp_rfqs.json"
output_file_name = time_stamp + "_consolidated_dp_rfqs"
prefix = 'lineItems_'

# Call the function for processed dp_rfqs
dp_file_processor(input_path, output_path, input_file_name,output_file_name, prefix,ind_cols)


# Combining df_rqs and dp_rfqs for extracting coumns from dp_rfqs
def rqs_dp_rfqs_merging(input_file_path_rqs:str,input_file_name_rqs:str,
                        input_file_path_dp_rqs:str,input_file_name_dp_rqs:str,
                        joining_fields_l:list,joining_fields_r:list,
                        output_file_path:str,output_file_name:str):
    df_rqs = pd.read_csv(input_file_path_rqs + input_file_name_rqs,delimiter=',',encoding='ISO-8859-1')
    dp_rfqs = pd.read_csv(input_file_path_dp_rqs + input_file_name_dp_rqs,delimiter=',',encoding='ISO-8859-1')
    dp_rfqs = dp_rfqs[joining_fields]
    # these are additional fields that are needed from dp_rfqs
    df_rqs = pd.merge(left=df_rqs, right=dp_rfqs, left_on=joining_fields_l,
                      right_on=joining_fields_r,how='left')
    date_cols = [x for x in df_rqs.columns.to_list() if 'Date' in x]
    date_cols.extend(['issued', 'createdAt', 'lastDeliveryAt', 'quoteValidFrom', 'quoteValidUntil'])
    df_rqs_dates = df_rqs[date_cols]
    df_rqs_dates = df_rqs_dates.astype({i: 'object' for i in date_cols})
    df_rqs_dates = df_rqs_dates.fillna("NA")
    df_rqs_dates = df_rqs_dates.apply(lambda x: pd.to_datetime(x, errors='coerce'))
    df_rqs_dates = df_rqs_dates.apply(lambda x: x.dt.strftime('%Y/%m/%d'))
    df_rqs[date_cols] = df_rqs_dates
    df_rqs.to_csv(output_file_path+output_file_name,sep='\t',index=False,header= True)
    print("Merging Activity Completed!")

# Declare the arguments of the function
input_file_path_rqs = "C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\df_rqs\\"
input_file_name_rqs = time_stamp + "consolidated_rqs_quotes.txt"
input_file_path_dp_rqs = "C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\dp_rfqs\\"
input_file_name_dp_rqs = time_stamp + "consolidated_dp_rfqs.txt"
joining_fields_l = ['items_mfrPartNo']
joining_fields_r = ['lineItems_mfrPartNo']
output_file_path = "C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\"
output_file_name = "all_quotes.txt"

# Call the function
rqs_dp_rfqs_merging(input_file_path_rqs,input_file_name_rqs,
                    input_file_path_dp_rqs,input_file_name_dp_rqs,
                    joining_fields_l,joining_fields_r,
                    output_file_path,output_file_name)


# Call the hierarchy retriever function
def hierarchy_retriever(input_path_l : str,input_path_r : str,
                        output_path: str,output_file_name :str,dataset_l : str,
                        dataset_r :str,columns_l : list,
                        columns_r : list,join_type : str):
    l = pd.read_table(input_path_l + dataset_l + '.csv',  delimiter='\t', encoding='ISO-8859-1')
    r = pd.read_csv(input_path_r + dataset_r + '.csv', encoding='ISO-8859-1')
    r = r[['BG Name', 'CBU','CBU Name','SBU','SBU Name','Bdgt Item Grp','Bdgt Item Grp Name', 'Item Code']]
    combined = pd.merge(left=l, right=r, how=join_type, left_on=columns_l, right_on=columns_r)
    combined.to_csv(output_path + output_file_name ,sep = '\t',index=False)
    print("Hierarchy columns retrieved.")

input_path_l = "C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\"
input_path_r = "C:\\distribution_dashboard\\data\\inputs\\hierarchy_files\\product_hierarchy\\"
output_path = "C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\"
output_file_name = time_stamp + 'all_quotes.txt'
dataset_l = "all_quotes.txt"
dataset_r = 'product_hierarchy_revised'
columns_l = ['items_mfrPartNo']
columns_r = ['Item Code']
join_type = 'left'

hierarchy_retriever(input_path_l,input_path_r,output_path,output_file_name,dataset_l,
                    dataset_r,columns_l,columns_r,join_type)


# Reporting Table generation
def rt_generator(input_path : str, output_path : str,master_field_path : str,master_field_file_name : str,
                 input_file_name : str,output_file_name : str,table_name : str):
    master_field_df = pd.read_csv(master_field_path + master_field_file_name,encoding = "utf-8")
    input_data = pd.read_table(input_path + input_file_name, delimiter='\t', encoding='ISO-8859-1')
    master_field_df = master_field_df.loc[master_field_df['table_name'] == table_name, ['input_field_name', 'tableau_visual_field_name']]
    input_field_list = master_field_df['input_field_name'].to_list()
    tableau_field = master_field_df['tableau_visual_field_name'].to_list()
    req_cols = [x for x in input_data.columns.to_list() if x in input_field_list]
    data_revised = input_data[req_cols]
    variable_directory = {i: j for i, j in zip(input_field_list, tableau_field) if i in req_cols}
    data_revised = data_revised.rename(columns=variable_directory)
    data_revised = data_revised[tableau_field]
    data_revised.to_csv(output_path + output_file_name,sep = '\t',index = False,header= True)
    print(data_revised.shape)

# Declare the arguments to the function
input_path = 'C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\'
output_path = 'C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\'
master_field_path = 'C:\\distribution_dashboard\\data\\inputs\\'
master_field_file_name = 'revised_master_field_temp.csv'
input_file_name = time_stamp + 'all_quotes.txt'
output_file_name = time_stamp + 'rqs_data.txt'
table_name = 'rqs'

# Call the Function
rt_generator(input_path, output_path, master_field_path,
             master_field_file_name, input_file_name, output_file_name, table_name)


# data refresh function to combine new data points with master file
def data_refresh_rqs(master_dataset_path:str,master_dataset_file_name:str,
                     new_dataset_path:str,new_dataset_file_name:str,
                     output_path:str,output_file_name:str):
    master_dataset = pd.read_table(master_dataset_path + master_dataset_file_name,
                                   delimiter='\t',encoding='ISO-8859-1')
    new_dataset = pd.read_table(new_dataset_path + new_dataset_file_name,
                                delimiter='\t', encoding='ISO-8859-1')
    master_dataset = pd.concat([master_dataset,new_dataset],axis=0)
    master_dataset.to_csv(output_path + output_file_name, sep='\t', header=True, index=False)
    print("Data Refresh till"+ time_stamp)

# Declare the arguments of the function
master_dataset_path = "C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\"
master_dataset_file_name = ""
new_dataset_path = "C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\"
new_dataset_file_name = time_stamp + 'rqs_data.csv'
output_path = "C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\"
output_file_name = 'all_quotes.txt'

# Call the Function
data_refresh_rqs(master_dataset_path,master_dataset_file_name,
                 new_dataset_path,new_dataset_file_name,
                 output_path,output_file_name)

# Defining rqs_tabular_generator function for Tabular data generation
def rqs_tabular_generator(master_dataset_path:str,master_dataset_file_name,
                           output_file_path:str,output_file_name:str):
    """"
    This function takes processed GMDS file and returns the last 1 year of data
    for tabular view requirements
    """
    df = pd.read_table(master_dataset_path + master_dataset_file_name, delimiter='\t', encoding='ISO-8859-1')
    start_date = datetime.date.today()-timedelta(days=366)
    cur_date = datetime.date.today()
    final_df = df.loc[((pd.to_datetime(df['Process Date']) >= pd.to_datetime(start_date))
                       & (pd.to_datetime(df['Process Date']) <= pd.to_datetime(cur_date))),:]
    final_df.to_csv(output_file_path + output_file_name, sep='\t', header=True, index=False)


# Declare the arguments and call the function
master_dataset_path = "C:\\distribution_dashboard\\data\\outputs\\table\\gmds_sales\\"
master_dataset_file_name = "all_quotes.txt"
output_file_path = "C:\\distribution_dashboard\\data\\outputs\\table\\gmds_sales\\"
output_file_name = "tabular_views" + "all_quotes.txt"

# Calling the function
rqs_tabular_generator(master_dataset_path,master_dataset_file_name,output_file_path,output_file_name)

