import pandas as pd
import json


class UserDefinedFn:

    def epcos_cleaning(items):
        if str(items)[0] == 'B':
            clean_code = items.ljust(18," ").replace(" ","0")
            final_code = clean_code[:15]
        else:
            final_code = items

        return final_code

    def tdk_cleaning(items):
        if str(items).startswith("CGA"):
            clean_code = items[:len(items)-5]
            final_code = clean_code
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