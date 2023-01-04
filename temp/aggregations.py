# Import the packages
import glob
from datetime import datetime
import pandas as pd
import os


# Accessing the current path
os.getcwd()

# Flow of the working
# Import the txt file
# read the file and correct the formats
# Group the required variables_kw

os.chdir("C:\distribution_dashboard\data\inputs\RT_Inputs")
data = pd.read_table("RT_GMDS_PoS_Sales_Inventory_Monthly.txt",sep="\t")
data['Main Customer Grp Name']= data['Main Customer Grp Name'].fillna("None")
data['Year'] = pd.to_datetime(data['Base Month']).dt.year
data.columns
agg_data = pd.DataFrame(data.groupby(['Country','Main Customer Grp Name','Customer Name','Year'])['POS Sales Amt'].sum()).reset_index()
agg_data.to_csv("C:/distribution_dashboard/data/outputs/RT_outputs/RT_Network.csv",encoding="ISO-8859-1",index=False)
data.to_csv("C:/distribution_dashboard/data/outputs/RT_outputs/PoS_GMDS.csv",encoding="ISO-8859-1",index=False)

# PoS File
os.chdir("C:/distribution_dashboard/data")
pos = pd.read_csv("C:/distribution_dashboard/data/inputs/pos_sales/pos_sales.txt",sep="\t", encoding="ISO-8859-1",error_bad_lines=False,header=None)
variable_names= pd.read_csv("C:/distribution_dashboard/data/inputs/pos_sales/definitions/column_definitions.csv",sep=",", encoding="ISO-8859-1")
variable_names=variable_names.iloc[:,0].to_list()
pos.columns = variable_names
pos['Currency']= 'NA'
pos['Base Month']= pd.to_datetime(pos['Base Month'],format="%Y%m")
pos['Invoice Date'] = pd.to_datetime(pos['Invoice Date'],format="%Y%m%d")
pos.to_csv("C:/distribution_dashboard/data/inputs/pos_sales/pos_sales_revised.csv",sep=",", encoding="ISO-8859-1",index=False)




