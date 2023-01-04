# Import the packages
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
import csv


# Function to read the TSV file
def read_tsv (input_path:str,input_file_name:str,output_path:str,output_file_name:str,input_col_directory_path:str,dir_name:str,dir_table_name:str,n_rows:int):
    directory = pd.read_csv(input_col_directory_path + dir_name, sep=',', encoding="ISO-8859-1")
    cols = directory.loc[directory['table_name'] == dir_table_name, 'colnames'].to_list()
    for i in range(1):
        # i = 1
        print(f"Creating file - {i}..")
        skip_rows = i * n_rows + 1 if i != 0 else 0
        dataset = pd.read_csv(input_path + input_file_name, header=None, delimiter='\t', encoding='ISO-8859-1',
                              skiprows=skip_rows,names=cols, nrows=n_rows, error_bad_lines=False)
        dataset.to_csv(output_path + output_file_name + str(i) + '.csv', index=False)
    print("Csvs Created!!")


# Submitting the arguments
input_path = "C:\\distribution_dashboard\\data\\inputs\\open_order\\"
input_file_name = "open_order_20221212.tsv"
output_path = "C:\\distribution_dashboard\\data\\outputs\\table\\order_intake\\open_order\\"
output_file_name = 'open_order_20221212_.csv'
input_col_directory_path = "C:\\distribution_dashboard\\data\\inputs\\open_order\\"
dir_name = "order_intake_ship_history_column_directory.csv"
dir_table_name = "open_order"
n_rows = 300000

# Call the function and Execute
read_tsv(input_path,input_file_name,output_path,output_file_name,input_col_directory_path,dir_name,dir_table_name,n_rows)






