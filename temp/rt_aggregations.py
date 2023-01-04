# Firstly convert all the excel files to csv files
# File extraction should be done using alias
# read the file with only columns from merging_datasets
# concatenate all the relevant datasets
# Highlight the columns that are missing
# Create a interim dataset with the field names from the 2nd column of the merging_datasets
# Create tableau input file and save it with the column names of 3rd column of the merging_datasets

# Importing the required packages

import pandas as pd
import os
import glob
from datetime import datetime

# Accessing the current path
os.getcwd()


# Accessing the desired folder and converting the files to csvs

def rt_generator(source_path: str, master_fields_path: str, master_fields_name: str, interim_path: str,
                 outputs_path: str,
                 table_name):
    os.chdir(source_path)
    file_details = table_name
    final_file_name = datetime.now().strftime("%Y%m%d") + file_details

    #csvs = [i for i in glob.glob('*.{}'.format("csv"))]
    #combined_csv = pd.concat([pd.read_csv(f,sep = ",",encoding='ISO-8859-1') for f in csvs])
    #combined_csv = combined_csv.reset_index(drop=True)
    combined_csv.to_csv(outputs_path + "/" + final_file_name + '_' + 'combined' + '.csv', index=False,encoding='ISO-8859-1')
    print("Combined file created successfully!!")

    os.chdir(master_fields_path)
    master = pd.read_excel(master_fields_path + '/' + master_fields_name)
    combined = combined_csv.copy()
    directory = master.loc[(master['table_name'] == table_name), :]

    cols = []
    for i in directory[directory.columns[1]]:
        if i in combined.columns.to_list():
            cols.append(i)

    refined_combined = combined[cols]
    # Extract the unique records
    refined_combined = refined_combined[-refined_combined.duplicated()]
    interim = refined_combined.copy()
    interim.columns = directory[directory[directory.columns[1]].isin(cols)].iloc[:, 2].to_list()
    tableau_input = refined_combined.copy()
    tableau_input.columns = directory[directory[directory.columns[1]].isin(cols)].iloc[:, 3].to_list()
    os.chdir(interim_path)
    interim.to_csv(interim_path + "/" + final_file_name + '_' + 'interim_' + '.csv', index=False,encoding='ISO-8859-1')
    # refined_combined.to_csv(source_path + "/" + final_file_name + '_' + 'refined_combined' + '.csv', index=False)
    os.chdir(outputs_path)
    tableau_input.to_csv(outputs_path + "/" + final_file_name + '_' + 'tableau_input' + '.csv', index=False,encoding='ISO-8859-1')
    print("Done!")


# For RQS
#Generate the non-existing columns

RQS_2020 = pd.read_csv('C:\distribution_dashboard\data\inputs\RQS\RQS_2020.csv',encoding='ISO-8859-1')
RQS_2021 = pd.read_csv('C:\distribution_dashboard\data\inputs\RQS\RQS_2021.csv',encoding='ISO-8859-1')
RQS_2019 = pd.read_csv('C:\distribution_dashboard\data\inputs\RQS\RQS_2019.csv',encoding='ISO-8859-1')

RQS_2019.columns[0]

l = []
for i in RQS_2021.columns:
    if i not in RQS_2021.columns:
        RQS_2020.drop(columns=[i],axis =1 ,inplace= True)
RQS_2020_2021 = pd.concat([RQS_2021,RQS_2020],axis=0)
print(l)


RQS_2020_2021['Product Order Number'] = 'O1234567890'
RQS_2020_2021['Product Line Order Number'] = "aaa"
RQS_2020_2021['POP Customer Purchase Order']= 'O1234567890'
RQS_2020_2021['Original Order Quantity']= 0000
RQS_2020_2021['Part Description'] = 'avbgh abjk'
RQS_2020_2021['Original Balance Due']  = 0000
RQS_2020_2021['Shipped Quantity']= 0000
RQS_2020_2021['Delivery ETA']= '01/01/2000'
RQS_2020_2021['Country Of Origin']= 'abcde'
RQS_2020_2021['Delivery Note and Tracking Details']= 'comments'
RQS_2020_2021QS = RQS_2020_2021.to_csv('C:\distribution_dashboard\data\inputs\RQS\RQS_2020_2021.csv',encoding='ISO-8859-1',index= False)

RQS
rt_generator("C:/distribution_dashboard/data/inputs/rqs",
             "C:/distribution_dashboard/data/inputs",
             "revised_master_field_temp.csv",
             "C:/distribution_dashboard/data/interim",
             "C:/distribution_dashboard/data/outputs/table",
             "rqs"
             )


# For price_list

rt_generator("C:/distribution_dashboard/data/inputs/price_list",
             'C:/distribution_dashboard/data/inputs',
             'master_field_name_file.xlsb',
             'C:/distribution_dashboard/data/interim',
             'C:/distribution_dashboard/data/outputs/table',
             "price_list"
             )
rt_generator("C:/distribution_dashboard/data/inputs/lead_time",
             'C:/distribution_dashboard/data/inputs',
             'master_field_name_file.xlsb',
             'C:/distribution_dashboard/data/interim',
             'C:/distribution_dashboard/data/outputs/table',
             "lead_time"
             )

os.listdir()

# For Pos and Pos inventory rt_generation

rt_generator("C:/distribution_dashboard/data/inputs/pos_sales",
             "C:/distribution_dashboard/data/inputs",
             "revised_master_field_name.xlsb",
             "C:/distribution_dashboard/data/interim",
             "C:/distribution_dashboard/data/outputs/table",
             "pos_sales"
             )

# For GMDS

rt_generator("C:/distribution_dashboard/data/inputs/gmds_sales",
             "C:/distribution_dashboard/data/inputs",
             "revised_master_field_name.xlsb",
             "C:/distribution_dashboard/data/interim",
             "C:/distribution_dashboard/data/outputs/table",
             "gmds"
             )




##Adding missing columns
missing_cols = ['RO','Currency','Product Order Number']
gmds = pd.read_table("C:/distribution_dashboard/data/inputs/gmds_sales/RT_GMDS_PoS_Sales_Inventory_Monthly.txt",sep="\t")
pos_sales = pd.read_table("C:/distribution_dashboard/data/inputs/pos_sales/pos_sales.txt",sep="\t")
gmds['RO'] = 0000
gmds['Currency'] = "aaa"
gmds['Product Order Number']= 'B1234567890'
gmds.to_csv("C:/distribution_dashboard/data/inputs/gmds_sales/RT_GMDS_PoS_Sales_Inventory_Monthly.csv",index=False,encoding='ISO-8859-1')

# For GMDS and Pos joined file
rt_generator("C:/distribution_dashboard/data/inputs/gmds_sales",
             "C:/distribution_dashboard/data/inputs",
             "master_field_name_file.xlsb",
             "C:/distribution_dashboard/data/interim",
             "C:/distribution_dashboard/data/outputs/table",
             "gmds")

##Adding missing columns

missing_cols = ['Product Order Number','Product Line Order Number','Original Order Quantity',
                'POP Customer Purchase Order','Part Description','Order Original Quantity',
                'Shipped Quantity','Delivery ETA','Country Of Origin','Delivery Note and Tracking Details']

rqs = pd.read_csv("C:/distribution_dashboard/data/inputs/RQS/RT_RQS_NEW.csv",sep = ",",encoding='ISO-8859-1')
rqs['Product Order Number'] = 'O1234567890'
rqs['Product Line Order Number'] = "aaa"
rqs['POP Customer Purchase Order']= 'O1234567890'
rqs['Original Order Quantity']= 0000
rqs['Part Description'] = 'avbgh abjk'
rqs['Original Balance Due']  = 0000
rqs['Shipped Quantity']= 0000
rqs['Delivery ETA']= '01/01/2000'
rqs['Country Of Origin']= 'abcde'
rqs['Delivery Note and Tracking Details']= 'comments'
rqs.to_csv("C:/distribution_dashboard/data/inputs/RQS/RT_RQS_new_refined.csv",index=False,encoding='ISO-8859-1')

# joining the region information to PoS data
# Read the files
codes = pd.read_excel('C:/distribution_dashboard/data/inputs/pos_sales/end_cust_country_codes.xlsx')
codes['Region code'] = pd.Series(codes['Region(Allocated)'].str.split(":",expand = True)[0])
codes['Region'] = pd.Series(codes['Region(Allocated)'].str.split(":",expand = True)[1])
pos = pd.read_csv('C:/distribution_dashboard/data/inputs/pos_sales/pos_sales_revised.csv',encoding='ISO-8859-1')
pos_sales_revised = pd.merge(pos,codes,how = 'left',left_on = ['E-Cust Country'],right_on = ['E-Cust Country'])
pos_sales_revised.to_csv('C:/distribution_dashboard/data/inputs/pos_sales/pos_sales.csv',index=False,encoding='ISO-8859-1')

# For GMDS and Pos joined file
rt_generator("C:/distribution_dashboard/data/inputs/rqs",
             "C:/distribution_dashboard/data/inputs",
             "master_field_name_file.xlsb",
             "C:/distribution_dashboard/data/interim",
             "C:/distribution_dashboard/data/outputs/table",
             "rqs")

# For PoS file
rt_generator("C:/distribution_dashboard/data/inputs/pos_sales",
             "C:/distribution_dashboard/data/inputs",
             "master_field_name_file.xlsb",
             "C:/distribution_dashboard/data/interim",
             "C:/distribution_dashboard/data/outputs/table",
             "pos_sales")

#Merging Activity on price list and lead time

price_list = pd.read_csv("20220405price_list_tableau_input.csv",encoding='ISO-8859-1')
lead_time = pd.read_csv("20220405lead_time_tableau_input.csv",encoding='ISO-8859-1')
price_list.head()
lead_time.head()
data = price_list.merge(lead_time,left_on= ['Item Code','Budget Item Group','BG'],right_on = ['Item Code','Budget Item Group','BG']
                        ,how= 'left')
timestamp = datetime.now().strftime("%Y%m%d")
data.to_csv(timestamp + "price_list_lead_time_tableau_input.csv",encoding='ISO-8859-1',index= False)

# Please ignore the code below this is written for some testing activity
os.listdir()
dataframe = pd.read_csv('RQS_2019.csv',encoding='ISO-8859-1')

dataframe.shape

dataframe.columns
import numpy as np

quote_id = dataframe.groupby(['OS Code', 'TDK Quotation Reference No.', 'TDK Part No.', 'Quotation Status']).agg(
    {'TDK Quotation Reference No.': lambda x: x.count(), 'Quotation Status': lambda x: x.count(),

     "OS Code": lambda x: x.nunique()}).rename(columns={'TDK Quotation Reference No.': 'TDK Quote No Freq',
                                                        'OS Code': 'OS Code Freq',
                                                        'Quotation Status': 'Quotation Status Freq '
                                                        })
quote_id = quote_id.reset_index()
quote_id.to_csv('table/quote_analysis.csv', index=False)

# Use  rt_generator for lead time and price list files concatenation and aggregation

# Merging activity

#Combining Epcos and TDK lead time files
#Rename the variables to establish similarity
#Epcos

# Join PoS and PoS inv for Pos Pos Inv reporting table

os.chdir("C:\distribution_dashboard\data\inputs\pos_sales")
os.listdir()
pos_inv = pd.read_csv("POS Stock 202108.csv",encoding='ISO-8859-1')
pos = pd.read_csv("POS.csv",encoding='ISO-8859-1')
pos_inv_com = pos.merge(pos_inv,left_on= ['BG','BG Name', 'Customer Name', 'Base Month', 'Item Code', 'SAP Customer'],
          right_on=['BG', 'BG Name','Customer Name', 'Base Month', 'Item Code', 'SAP Customer'],how= 'left')

pos_inv_com.to_csv("pos_inv_com.csv",encoding='ISO-8859-1',index= False)

# Get the zip codes through joins

zip = pd.read_csv("zip_codes.csv",encoding='ISO-8859-1')
pos_inv_zip = pos_inv_com.merge(zip,
                  left_on= ['Customer Name', 'Base Month', 'E-Cust Name', 'E-Cust Country',
        'Item Code', 'BG', 'BG Name'],
                  right_on= ['Customer Name', 'Base Month', 'E-Cust Name', 'E-Cust Country',
        'Item Code', 'BG', 'BG Name'],how = 'left')
pos_inv_zip.to_csv("pos_inv_zip.csv",encoding='ISO-8859-1',index= False)