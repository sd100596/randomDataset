import json
from pathlib import Path
import os
import logging
import pandas as pd

path_input = Path(os.environ.get("INPUTS", "/data/inputs"))
path_output = Path(os.environ.get("OUTPUTS", "/data/outputs"))
path_logs = Path(os.environ.get("LOGS", "/data/logs"))
dids = json.loads(os.environ.get("DIDS", '[]'))
did = dids[0]
input_files_path = Path(os.path.join(path_input, did))
input_files = list(input_files_path.iterdir())
first_input = input_files.pop()

path_input_file = first_input 

path_output_file = path_output / 'output1.csv'

with open(path_input_file, 'rb') as fh:
    df = pd.read_csv(fh)

sum_age = df['age'].sum()
sum_height = df['hight'].sum()

sums_df = pd.DataFrame({'age': [sum_age], 'height': [sum_height]})

result = sums_df.to_csv(path_output_file,index=False) 


