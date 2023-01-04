import pandas as pd
import pyarrow as pa
import numpy as np
from pyarrow import parquet as pq
import random
import itertools
from itertools import chain, combinations


output_file_path_parquet = ''
lst_str_cols =[]


# Different function to automate n combination selection
def key_options(items):
    return chain.from_iterable(combinations(items, r) for r in range(1, len(items)+1) )


df = pd.read_parquet(output_file_path_parquet+'20221221_pos_sales'+'.parquet')
df = df.loc[0:500000,lst_str_cols]
# iterate over all combinations of headings, excluding ID for brevity
for candidate in key_options(list(df)[1:]):
    deduped = df.drop_duplicates(candidate)

    if len(deduped.index) == len(df.index):
        print (','.join(candidate))

