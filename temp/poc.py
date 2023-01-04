import pandas as pd
import json

col_names = ["base_month","process_date","term","div_sub","div_sub_name_e","plan_grp","plan_grp_name_e",
             "plan_sum","plan_sum_name_e","plan_itgrp","plan_itgrp_name_e",
             "item_code","item_name","sao_group","sao_group_name_e","ovs_cust_name",
             "sap_customer_no","disty_type","disty_type_name","budget_amt","budget_qty",
             "budget_price","result_amt","result_qty","result_price","co_amt",
             "co_qty","co_price","fi_amt","fi_qty","fi_price","bok_amt","bok_qty",
             "bok_price","country_code","zip_code","region1","region1_name_e",
             "region3","region3_name_e","region5","region5_name_e","territory_code",
             "territory_name","district_code","district_name"]

lst_str_cols = ["base_month","process_date","term","div_sub","div_sub_name_e","plan_grp","plan_grp_name_e",
             "plan_sum","plan_sum_name_e","plan_itgrp","plan_itgrp_name_e",
             "item_code","item_name","sao_group","sao_group_name_e","ovs_cust_name",
             "sap_customer_no","disty_type","disty_type_name","country_code","zip_code","region1","region1_name_e",
             "region3","region3_name_e","region5","region5_name_e","territory_code",
             "territory_name","district_code","district_name"]

list_measure_vars = ["budget_price", "budget_amt", "budget_qty",
                     "result_price", "result_amt", "result_qty",
                     "co_price", "co_amt", "co_qty","fi_price", "fi_amt",
                     "fi_qty", "bok_price", "bok_amt",
                     "bok_qty"]

cat_dtypes = {x: 'str' for x in lst_str_cols}
num_dtypes = {x: 'int' for x in list_measure_vars}
cat_dtypes.update(num_dtypes)

col_dir = pd.DataFrame(columns=['field_name','data_type'])
col_dir['field_name'] = pd.Series(col_names)
col_dir['data_type'] = col_dir['field_name'].map(cat_dtypes)



col_dir.to_csv('C:\distribution_dashboard\data\inputs\gmds_sales\gmds_input_col_dir.csv',sep =',',
                                   encoding='ISO-8859-1',index=False)


import cx_Oracle
import csv
import time


cx_Oracle.init_oracle_client(lib_dir='C:/distribution_dashboard/src/python/etl/instantclient-basic-windows.x64-21.3.0.0.0/instantclient_21_3')

dsn = cx_Oracle.makedsn(
    'gmdx.tdk.biz',
    '1521',
    service_name='gmds_srv'
)
conn = cx_Oracle.connect(
    user='ORA_DM',
    password='B4u9Srj4h',
    dsn=dsn
)
cursor = conn.cursor()
begin = time.time()

cursor.execute('''
SELECT SAO_GROUP,SUM(SLS_AMT) AS AMT_
FROM GISLS_PROCESS_DAILY_V_X  
WHERE SAO_GROUP = 451
GROUP BY SAO_GROUP''');
print("hello")

for j in cursor:
    print(j)


# Append to the master file
col_names_dir = pd.read_csv(input_cols_dir_path + input_cols_dir_file_name, sep=',')
col_names = col_names_dir['field_name'].to_list()
list_str_cols = col_names_dir.loc[(col_names_dir['data_type'] == 'str')]['field_name'].to_list()
list_measure_vars = col_names_dir.loc[(col_names_dir['data_type'] == 'int')]['field_name'].to_list()
dict_dtypes = {x: 'str' for x in list_str_cols}
master_dataset_file_name = "20221029gmds_sales.txt"
master_dataset_path_name = "C:\\distribution_dashboard\\data\\outputs\\table\\gmds_sales\\"
#new_dataset = pd.read_table(output_path + output_file_name, delimiter='\t', dtype=dict_dtypes, names=col_names,encoding='ISO-8859-1')
master_dataset = pd.read_table(master_dataset_path_name + master_dataset_file_name, delimiter='\t', dtype=dict_dtypes, names=col_names, encoding='ISO-8859-1')
master_dataset['Process Date'] = pd.to_datetime(master_dataset['Process Date'])
master_dataset = master_dataset.loc[-(master_dataset['Process Date']>="20221104"), ]
#dd = pd.concat([master_dataset,new_dataset], axis=0).head()

md = master_dataset.head()

# Part Cleaning Algorithm for Epcos

rqs = pd.read_csv("C://distribution_dashboard//data//inputs//rqs//latest//processed_files//20221018rqs_data.csv"
            , delimiter=',', encoding='ISO-8859-1')

rqs.head()

rqs.columns

rqs['TDK Part No.'].apply(lambda x: str(x)[0])

rqs['TDK Part No.']


def epcos_cleaning(items):
    if str(items)[0] == 'B':
        clean_code = items.replace(' ', '0')
        final_code = clean_code[:15]
    else:
        final_code = items

    return final_code


len_char = []

for i in rqs['TDK Part No.']:
    if str(i)[0] == 'B' and len(i) == 18:
        len_char.append(i)
print(set(len_char))
len_char

rqs['TDK Part No.'][0]

# make the part numbers to number 18 with additional space for extra string literals
# Impute the spaces with Zeros
# Cut the item code to 15 then done ;-)
str(len_char[0]).ljust(18,"0")


rqs['TDK Part No.'].str.ljust(18," ")


len_char = []

for i in rqs['TDK Part No.']:
    if str(i)[0] == 'B':
        len_char.append(str(i).ljust(18," ").replace(" ","0")[:15])
len(len_char[0])

print(len_char)

len(len_char[0])


a = 'CGA2B1X7R1A224KT0Y0E'

a[:15]

type(a)

a.startswith("CGA")

if a.startswith("CGA"):
    print(a[:len(a)-5])
else:
    print("False")


# df_rqs

input_path = 'C:\\distribution_dashboard\\data\\inputs\\rqs\\sftp_files\\20221104_rqs_quotes.json'

data = open(input_path, "r")
data = json.load(data)
data[0]
prefix = 'items_'
df_rqs = pd.json_normalize(data, 'items',
                                   ['additionalNotes', 'amount', 'applicant',
                                    'bgApprovalDate', 'bgApprover', 'calendarYear',
                                    'contactPerson', 'csCode', 'currency',
                                    'businessGroup', 'createdAt', 'customerReference',
                                    'customerName', 'customerNameInternal', 'customerNameOfficial',
                                    'customerIsDistributor','customerSapNumber','customerMainCustomerGroup',
                                    'drActivities','drDetails','drStatus','drReference','drStartDate',
                                    'drEndDate','incoTerms','incoTermsPlace','industrySegment','isBudgetary',
                                    'lastDeliveryAt','legalNotices','mail','noOrderReason','orderStatus',
                                    'osCode','paymentTerms','projectName','posCustomerName','posCountry',
                                    'quotationCategory','quotationPeriod','quotationType','rfqIssueDate',
                                    'isRunningBiz','salesApprover1Date','salesApprover2Date','salesApprover1',
                                    'salesApprover2','salesDistrict','salesRegion','salutation','sopYear',
                                    'sopMonth','specification','tdkTerm','tdkReference','url','quoteValidFrom',
                                    'quoteValidUntil'], record_prefix=prefix)

df_rqs.to_csv("C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\20221115_rqs_quotes.csv")
ind_cols = ['id', 'reference', 'issued','currency']


ind_cols = ['id','rfqId','tdkReference','validFrom','validUntil',
            'lastDelivery','isBudgetary','salutation','currency',]
prefix = 'lineItems_'
input_path = "C:\\distribution_dashboard\\data\\inputs\\rqs\\sftp_files\\20221115_dp_quotes.json"
data = open(input_path, "r")
data = json.load(data)
data[0]
dp_quotes = pd.json_normalize(data, 'lineItems', ind_cols, record_prefix=prefix, errors='ignore')
dp_quotes.head()



dp_quotes.to_csv("C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\20221115_dp_quotes.csv")


input_path = "C:\\distribution_dashboard\\data\\inputs\\rqs\\sftp_files"

f = open(input_path + "\\rqs_quotes_all.json")

# returns JSON object as
# a dictionary
data = json.load(f)
data

input_path = "C:/distribution_dashboard/data/inputs/rqs/sftp_files/"
output_path = "C:/distribution_dashboard/data/outputs/table/rqs/df_rqs/"
file_name = "rqs_quotes_all.json"
prefix = 'items_'


data = open(input_path + file_name, "r")
data = json.load(data)
df_rqs = pd.json_normalize(data, 'items',
                           ['additionalNotes', 'amount', 'applicant',
                            'bgApprovalDate', 'bgApprover', 'calendarYear',
                            'contactPerson', 'csCode', 'currency',
                            'businessGroup', 'createdAt', 'customerReference',
                            'customerName', 'customerNameInternal', 'customerNameOfficial',
                            'customerIsDistributor', 'customerSapNumber', 'customerMainCustomerGroup',
                            'drActivities', 'drDetails', 'drStatus', 'drReference', 'drStartDate',
                            'drEndDate', 'incoTerms', 'incoTermsPlace', 'industrySegment', 'isBudgetary',
                            'lastDeliveryAt', 'legalNotices', 'mail', 'noOrderReason', 'orderStatus',
                            'osCode', 'paymentTerms', 'projectName', 'posCustomerName', 'posCountry',
                            'quotationCategory', 'quotationPeriod', 'quotationType', 'rfqIssueDate',
                            'isRunningBiz', 'salesApprover1Date', 'salesApprover2Date', 'salesApprover1',
                            'salesApprover2', 'salesDistrict', 'salesRegion', 'salutation', 'sopYear',
                            'sopMonth', 'specification', 'tdkTerm', 'tdkReference', 'url', 'quoteValidFrom',
                            'quoteValidUntil'], record_prefix=prefix,
                           errors="ignore")

# Declare the arguments to the function

# Run the function
df_rqs['createdAt'].head()


df_rqs.to_csv("C://distribution_dashboard//data//outputs//table//rqs//all_quotes.csv",
              index=False)

# Read the TDK price list file

tdk_list = pd.read_csv("C://distribution_dashboard//data//inputs//price_list//tdk_price_list.csv")
tdk_list = tdk_list[['ORDERING CODE\nCustomer Part Nr.','\nTDK Item Description']]
tdk_list.columns = ['ordering_code','item_description']
tdk_list = tdk_list[['item_description','ordering_code']]
tdk_list.head()

l1 = ["eat", "sleep", "repeat"]
for i,j in enumerate(l1,"geek"):
    print(i)

tdk_dict = {}

for i,j in zip(tdk_list['item_description'],tdk_list['ordering_code']):
    tdk_dict[i]=j
print("Part Number Dictionary Created!")

tdk_dict.get('C2012X7R1V475KT000S')

for i in tdk_list['item_description']:
    tdk_dict.get(i)
    print(i)

df_target = test[['Original Item Code']]
df_target.set_index('Original Item Code',inplace=True)
df_target
df_target.head()


# Read the GMDS file
# Read the Price list file
def tdk_cleaning(ref_x,x):
    clean_code = x.map(ref_x).fillna(x)
    return clean_code


input_path = "C://distribution_dashboard//data//inputs//poc//"
input_file_name = "tdk_parts.csv"

target = pd.read_table(input_path + input_file_name,sep=',',encoding='ISO-8859-1')
tdk_price_list = pd.read_csv("C://distribution_dashboard//data//inputs//price_list//tdk_price_list.csv")
reference_file = tdk_price_list.iloc[:,[4,3]]
reference_file.columns = ['reference_code','cleaned_code']
reference_file.head()

tdk_dict = {}
for i,j in zip(reference_file['reference_code'],reference_file['cleaned_code']):
    tdk_dict[i] = j


target['old_code'] = target['item_name']
target['item_name'] = target['item_name'].map(tdk_dict).fillna(target['old_code'])
target = target[['old_code','item_name']]
set(target['item_name']).intersection(set(target['old_code']))

# Read the TDK Item_name file
target.to_csv("C:\\distribution_dashboard\\data\\outputs\\table\\test\\tdk_cleaned.csv",
              sep=',',encoding='ISO-8859-1',index = False)


count = 5
while count<=10:
    print("Hurray passed the loop!!")
    if count == 6:
        break
    count += 1


# Merging the data sources dp-quotes and dp_rfqs

input_file_path_a = "C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\dp_quotes\\"
input_file_path_b = "C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\dp_rfqs\\"
input_file_name_a = "consolidated_dp_quotes.csv"
input_file_name_b = "20221125_consolidated_dp_rfqs.csv"


dp_quotes = pd.read_csv(input_file_path_a+input_file_name_a,sep=',',usecols=['rfqId','tdkReference','lineItems_mfrPartNo'])
dp_rfqs = pd.read_csv(input_file_path_b+input_file_name_b,sep=',',usecols=['id','lineItems_mfrPartNo','application_type','lineItems_branch'])

dp_rfqs = pd.merge(left=dp_rfqs,right=dp_quotes,left_on=['id','lineItems_mfrPartNo'],right_on=['rfqId','lineItems_mfrPartNo'],how='left')

# Merge with df_rqs_quotes

rqs = pd.read_csv("C:\\distribution_dashboard\\data\\outputs\\table\\rqs\\df_rqs\\20221125_consolidated_rqs_quotes.csv")

rqs = pd.merge(left=rqs,right=dp_rfqs,left_on=['tdkReference','items_mfrPartNo'],right_on=['tdkReference','lineItems_mfrPartNo'])

rqs.application_type.isnull().sum()

input_file_path = "C:\\oracle_export\\"
input_file_name = "20221217_region.txt"

df = pd.read_csv(input_file_path+input_file_name,sep=',',encoding='ISO-8859-1',skipfooter=1,engine='python')
df.shape