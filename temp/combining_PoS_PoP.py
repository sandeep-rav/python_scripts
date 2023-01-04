import gc

import pandas as pd
from src.python.etl.user_defined_fn import UserDefinedFn
import random

path = 'C:/project/distributionDashboard'
input_path = path + '/data/input'
output_path = path + '/data/output'

### POS INVENTORY

col_names = ["base_month",	"base_month_derived",
              "bg", "bg_name", "cbu",	"cbu_name",	"sbu",	"sbu_name", "original_item_code", "item_code",
              "sap_customer",	"customer_name",
              "inventory_qty",	"inventory_amt", "inventory_price",	"free_stock_qty", "free_stock_amt"]

lst_str_cols = ["base_month",	"base_month_derived",
              "bg", "bg_name", "cbu",	"cbu_name",	"sbu",	"sbu_name", "original_item_code", "item_code",
              "sap_customer",	"customer_name"]
path_with_file_details = '{0}/{1}.{2}'.format(output_path, 'PoS_Inventory', 'txt')

dict_dtypes = {x: 'str' for x in lst_str_cols}
pos_inventory = pd.read_table(path_with_file_details,  header=None, delimiter='\t', encoding='ISO-8859-1', names=col_names, dtype=dict_dtypes) # , nrows=50000

# inv_by_bg_month = pos_inventory.groupby(["bg","base_month"],as_index=False).agg(inventory_amt = ('inventory_amt','sum'), free_stock_amt = ('free_stock_amt','sum'))
# len(pos_inventory)
### POS SALES

col_names = ["base_month",	"base_month_derived",
              "bg", "bg_name", "cbu",	"cbu_name",	"sbu",	"sbu_name", "original_item_code", "item_code",
              "sap_customer",	"customer_name", "e_cust_name", "e_cust_country", "e_cust_zip_code",
              "pos_sales_qty",	"pos_sales_amt", "pos_sales_price"]

lst_str_cols = ["base_month",	"base_month_derived",
                "bg", "bg_name", "cbu",	"cbu_name",	"sbu",	"sbu_name", "original_item_code", "item_code",
                "sap_customer",	"customer_name", "e_cust_name", "e_cust_country", "e_cust_zip_code"]

path_with_file_details = '{0}/{1}.{2}'.format(output_path, 'PoS_Sales', 'txt')

dict_dtypes = {x: 'str' for x in lst_str_cols}
pos_sales = pd.read_table(path_with_file_details,  header=None, delimiter='\t', encoding='ISO-8859-1', names=col_names, dtype=dict_dtypes) # , nrows=50000
# pos_by_bg_month = pos_sales.groupby(["bg","base_month"],as_index=False).agg(pos_sales_amt = ('pos_sales_amt','sum'))
# len(pos_sales)

pos_joining_cols = ["base_month", "base_month_derived", "bg", "bg_name", "cbu", "cbu_name", "sbu", "sbu_name", "original_item_code", "item_code", "sap_customer", "customer_name"]

pos_sales_inventory = pd.merge(left=pos_sales[pos_joining_cols + ["e_cust_name", "e_cust_country", "e_cust_zip_code", "pos_sales_qty", "pos_sales_amt",	"pos_sales_price"]],
                               right=pos_inventory[pos_joining_cols + ["inventory_qty", "inventory_amt", "inventory_price", "free_stock_qty", "free_stock_amt"]],
                               how='left', on=pos_joining_cols, left_index=False, right_index=False, sort=True, suffixes=('_x', '_y'), copy=True, indicator=False, validate=None)
# pos_sales_inv_by_bg_month = pos_sales_inventory.groupby(["bg","base_month"],as_index=False).agg(pos_sales_amt = ('pos_sales_amt','sum'), inventory_amt = ('inventory_amt','sum'), free_stock_amt = ('free_stock_amt','sum'))
len(pos_sales_inventory)

product_hierarchy = pd.read_csv(input_path+'/product_hierarchy.csv', header=0)
ph_joining_cols = ["bg", "bg_name",	"cbu",	"cbu_name",	"sbu",	"sbu_name", "original_item_code", "item_code"]
pos_sales_inventory_ph = pd.merge(left=pos_sales_inventory,
                                  right=product_hierarchy[ph_joining_cols+["item_description", "budget_item_grp", "budget_item_grp_name"]],
                                  how='left', on=ph_joining_cols, left_index=False, right_index=False, sort=True, suffixes=('_x', '_y'), copy=True, indicator=False, validate=None)

customer_hierarchy = pd.read_csv(input_path+'/customer_hierarchy.csv', header=0, dtype='str')
ch_joining_cols = ["sap_customer", "customer_name"]
pos_sales_inventory_ph_ch = pd.merge(left=pos_sales_inventory_ph,
                                  right=customer_hierarchy[ch_joining_cols+["main_customer_grp", "main_customer_grp_name"]],
                                  how='left', on=ch_joining_cols, left_index=False, right_index=False, sort=True, suffixes=('_x', '_y'), copy=True, indicator=False, validate=None)
len(pos_sales_inventory_ph_ch)
master_field = pd.read_csv(input_path+'/master_field_name_file.csv')
pos_fields = master_field.loc[master_field['table_name']=='pos_sales_inv'].reset_index(drop=True)
pos_required_fields = pos_fields['output_field_name'].to_numpy()
pos_fields_names = pos_fields['tableau_visual_field_name'].to_numpy()
pos_data = pos_sales_inventory_ph_ch[pos_required_fields].reset_index(drop=True)
pos_data.columns = [pos_fields_names]
pos_data.to_csv(output_path+'/RT_PoS_Sales_Inventory.txt', sep = '\t', header = True, index=False)



# pos_sales_inventory.to_csv(output_path+'/PoS_Sales_Inventory.txt', sep = '\t', header = True, index=False)

### POP MONTHLY

col_names = ["base_month",
             "bg",	"bg_name",	"cbu",	"cbu_name",	"sbu",	"sbu_name",	"budget_item_grp",	"budget_item_grp_name",	"original_item_code", "item_code", "item_description",
             "main_customer_grp", "main_customer_grp_name", "customer_name",	"sap_customer", "disty_type", "disty_type_name",
             "country", "zip_code",
             "pop_budget_amt",	"pop_budget_qty", "pop_sales_amt",	"pop_sales_qty", "pop_co_amt",	"pop_co_qty", "pop_fi_amt",	"pop_fi_qty", "pop_booking_bb_amt",	"pop_booking_bb_qty", "base_month_derived"]

lst_str_cols = ["base_month",
                "bg",	"bg_name",	"cbu",	"cbu_name",	"sbu",	"sbu_name",	"budget_item_grp",	"budget_item_grp_name",	"original_item_code", "item_code", "item_description",
                "main_customer_grp", "main_customer_grp_name", "customer_name",	"sap_customer", "country", "zip_code"]

path_with_file_details = '{0}/{1}.{2}'.format(output_path, 'PoP_Monthly_Data', 'txt')

dict_dtypes = {x: 'str' for x in lst_str_cols}
pop_monthly_data_all = pd.read_table(path_with_file_details,  header=None, delimiter='\t', encoding='ISO-8859-1', names=col_names, dtype=dict_dtypes) # , nrows=50000
pop_monthly_data = pop_monthly_data_all.loc[(pd.to_datetime(pop_monthly_data_all['base_month_derived'])>='04/2021')&(pd.to_datetime(pop_monthly_data_all['base_month_derived'])<'05/2022')&(pop_monthly_data_all['disty_type']=='DV')].reset_index(drop=True)
del(pop_monthly_data_all)
gc.collect()

pop_pos_joining_cols = ["base_month", "base_month_derived",
                        "bg",	"bg_name",	"budget_item_grp",	"budget_item_grp_name",	"cbu",	"cbu_name",	"sbu",	"sbu_name",	"original_item_code", "item_code", "item_description",
                        "main_customer_grp", "main_customer_grp_name", "sap_customer", "customer_name"]


# hier_vars_check = ["base_month","bg", "bg_name",	"cbu",	"cbu_name",	"sbu",	"sbu_name",	"budget_item_grp",	"budget_item_grp_name",	"original_item_code", "item_code", "item_description",
#                    "main_customer_grp", "main_customer_grp_name", "sap_customer", "customer_name"]
# hier_vars_check = ["base_month","item_code","sap_customer"]

# pos_distinct_dimensions = pos_sales_inventory_ph_ch.groupby(hier_vars_check, as_index=False).size()
# pop_distinct_dimensions = pop_monthly_data.groupby(hier_vars_check, as_index=False).size()
# pop_pos_common_dimension_rows = pd.merge(left=pop_distinct_dimensions, right=pos_distinct_dimensions, how='inner', on=hier_vars_check,
#                                          left_index=False, right_index=False, sort=True, suffixes=('_x', '_y'), copy=True, indicator=False, validate=None)
# len(pop_pos_common_dimension_rows)
# del(pos_distinct_dimensions, pop_distinct_dimensions, pop_pos_common_dimension_rows)
# gc.collect()

pop_pos_sales_inventory = pd.merge(left=pop_monthly_data, right=pos_sales_inventory_ph_ch,
                               how='left', on=pop_pos_joining_cols, left_index=False, right_index=False, sort=True, suffixes=('_x', '_y'), copy=True, indicator=False, validate=None)
len(pop_pos_sales_inventory)
del(pop_monthly_data, pos_sales_inventory_ph_ch)
gc.collect()
pop_pos_sales_inventory.dtypes
# noise = [random.uniform(0.01,0.99) for _ in range(len(pop_pos_sales_inventory))]
# pop_pos_sales_inventory["pop_budget_amt"]=pop_pos_sales_inventory["pop_budget_amt"] * noise
# pop_pos_sales_inventory["pop_budget_qty"]=pop_pos_sales_inventory["pop_budget_qty"] * noise
# pop_pos_sales_inventory["pop_sales_amt"]=pop_pos_sales_inventory["pop_sales_amt"] * noise
# pop_pos_sales_inventory["pop_sales_qty"]=pop_pos_sales_inventory["pop_sales_qty"] * noise
# pop_pos_sales_inventory["pop_co_amt"]=pop_pos_sales_inventory["pop_co_amt"] * noise
# pop_pos_sales_inventory["pop_co_qty"]=pop_pos_sales_inventory["pop_co_qty"] * noise
# pop_pos_sales_inventory["pop_fi_amt"]=pop_pos_sales_inventory["pop_fi_amt"] * noise
# pop_pos_sales_inventory["pop_fi_qty"]=pop_pos_sales_inventory["pop_fi_qty"] * noise
# pop_pos_sales_inventory["pop_booking_bb_amt"]=pop_pos_sales_inventory["pop_booking_bb_amt"] * noise
# pop_pos_sales_inventory["pop_booking_bb_qty"]=pop_pos_sales_inventory["pop_booking_bb_qty"] * noise
# pop_pos_sales_inventory["pos_sales_qty"]=pop_pos_sales_inventory["pos_sales_qty"] * noise
# pop_pos_sales_inventory["pos_sales_amt"]=pop_pos_sales_inventory["pos_sales_amt"] * noise
# pop_pos_sales_inventory["pos_sales_price"]=pop_pos_sales_inventory["pos_sales_price"] * noise
# pop_pos_sales_inventory["inventory_qty"]=pop_pos_sales_inventory["inventory_qty"] * noise
# pop_pos_sales_inventory["inventory_amt"]=pop_pos_sales_inventory["inventory_amt"] * noise
# pop_pos_sales_inventory["inventory_price"]=pop_pos_sales_inventory["inventory_price"] * noise
# pop_pos_sales_inventory["free_stock_qty"]=pop_pos_sales_inventory["free_stock_qty"] * noise
# pop_pos_sales_inventory["free_stock_amt"]=pop_pos_sales_inventory["free_stock_amt"] * noise


master_field = pd.read_csv(input_path+'/master_field_name_file.csv')
pop_pos_fields = master_field.loc[master_field['table_name']=='gmds_pos_sales_inv'].reset_index(drop=True)
pop_pos_required_fields = pop_pos_fields['output_field_name'].to_numpy()
pop_pos_fields_names = pop_pos_fields['tableau_visual_field_name'].to_numpy()
pop_pos_data = pop_pos_sales_inventory[pop_pos_required_fields].reset_index(drop=True)
pop_pos_data.columns = [pop_pos_fields_names]
pop_pos_data.to_csv(output_path+'/RT_GMDS_PoS_Sales_Inventory_Monthly.txt', sep = '\t', header = True, index=False)

pop_pos_sales_inventory_big_lvl = pop_pos_sales_inventory.groupby(["base_month",'base_month_derived', 'country', 'zip_code',
                                                                   "bg", "bg_name",	"cbu",	"cbu_name",	"sbu",	"sbu_name",	"budget_item_grp",	"budget_item_grp_name",
                                                                   "main_customer_grp", "main_customer_grp_name", "sap_customer", "customer_name"],as_index=False)[["pop_budget_amt",	"pop_budget_qty", "pop_sales_amt",	"pop_sales_qty", "pop_co_amt",	"pop_co_qty", "pop_fi_amt",	"pop_fi_qty", "pop_booking_bb_amt",	"pop_booking_bb_qty",
                        "pos_sales_qty",	"pos_sales_amt", "inventory_qty",	"inventory_amt", "free_stock_qty", "free_stock_amt"]].sum()

len(pop_pos_sales_inventory_big_lvl)
pop_pos_fields = master_field.loc[master_field['table_name']=='gmds_pos_sales_inv_big_lvl'].reset_index(drop=True)
pop_pos_required_fields = pop_pos_fields['output_field_name'].to_numpy()
pop_pos_fields_names = pop_pos_fields['tableau_visual_field_name'].to_numpy()
pop_pos_data = pop_pos_sales_inventory_big_lvl[pop_pos_required_fields].reset_index(drop=True)
pop_pos_data.columns = [pop_pos_fields_names]
pop_pos_data.to_csv(output_path+'/RT_GMDS_PoS_Sales_Inventory_Monthly_BIG.txt', sep = '\t', header = True, index=False)


pop_pos_sales_inventory.to_csv(output_path+'/PoP_PoS_Monthly_Data.txt', sep = '\t', header = True, index=False)
len(pop_pos_sales_inventory)



