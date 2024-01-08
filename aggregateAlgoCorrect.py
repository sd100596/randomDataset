import pandas as pd
from urllib import request
import os
import json
from io import StringIO
from pathlib import Path

path_input = Path(
    os.path.join(os.environ.get("INPUTS", "/data/inputs"), "algoCustomData.json")
)
path_output = Path(os.environ.get("OUTPUTS", "/data/outputs"))
path_logs = Path(os.environ.get("LOGS", "/data/logs"))

path_aggregate_output_file = path_output / "aggregateOutput.csv"

algoCustomData = {}

with open(path_input, "r") as json_file:
    algoCustomData = json.load(json_file)

result_data = algoCustomData["resultUrls"]


def get_data_from_url(url, headers):
    try:
        print(f"url : {url}")
        print("Headers : ")
        print(headers)
        req = request.Request(url, headers=headers)  # Create a request with headers
        response = request.urlopen(req)
        if response.getcode() == 200:
            csv_data = response.read().decode("utf-8")
            df = pd.read_csv(StringIO(csv_data))
            return df
        else:
            return None
    except Exception as e:
        print("Error fetching data from URL:", e)
        return None


data_frames = [
    get_data_from_url(job_data["job_url"], job_data["job_headers"])
    for job_data in result_data
]

if data_frames:
    combined_df = pd.concat(data_frames, ignore_index=True)
    column_averages = combined_df.mean().to_frame().T
    column_averages.columns = combined_df.columns
    column_averages.to_csv(path_aggregate_output_file, index=False)
