import pandas as pd
import pyarrow.parquet as pa
import datetime as dt
import os

base_dir = '//Ppmefs01/PPETARHFDASHB01/1758789696/rhf.esh_timesignals/'
print(dt.datetime.now())
for item in os.listdir(base_dir):
    if item.endswith(".parquet"):
        table = pa.read_table(base_dir + item)
        df = table.to_pandas()
        print(f"{item}: {len(df)} rows")

print(dt.datetime.now())