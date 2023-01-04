import pandas as pd
import json
import random
import pyarrow as pa
from pyarrow import parquet as pq


class UserDefinedFn:

    def epcos_cleaning(items):
        if str(items)[0] == 'B':
            clean_code = items.replace(' ', '0')
            final_code = clean_code[:15]
        else:
            final_code = items

        return final_code

    def total_summary(input_df: pd.DataFrame, group_by_vars: list, measure_vars: list) -> pd.DataFrame:
        return input_df.groupby(group_by_vars, as_index=False)[measure_vars].apply(lambda x: x.sum())  # .agg(sum=(measure_vars, 'sum'))


class ReadFilesFromDisk:

    def __init__(self, path_to_file: str):
        self.path = path_to_file
        self.orient_type = 'records'

    def read_files_from_disk(self, name_file: str, extn_file: str) -> pd.DataFrame:
        path_with_file_details = '{0}/{1}.{2}'.format(self.path, name_file, extn_file)

        if extn_file == 'csv':
            output = pd.read_csv(path_with_file_details, encoding='ISO-8859-1')
        elif extn_file == 'json':
            output = json.loads(pd.read_json(path_with_file_details, orient=self.orient_type).to_json())
        else:
            output = "none"
        return output

    # Function for masking numerical data
    def masking_field(input_df, output_file_path_parquet, output_file_name):
        noise = [random.uniform(0.01, 0.99) for _ in range(len(input_df))]
        for var_name in input_df.columns:
            if (input_df[var_name].dtype) != 'object':
                input_df[var_name] = input_df[var_name] * noise
        df = input_df
        table = pa.Table.from_pandas(df)
        pq.write_table(table, output_file_path_parquet + output_file_name + '.parquet', )
        print("Data Masking completed.")