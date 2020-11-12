#!home/nifi/nifienv/bin/python
# activate_this = "/Users/hjavaid/devtools/nifi-1.9.0/home/nifi/nifienv/bin/activate"
# exec(open(activate_this).read())
import csv
import json
import sys
from datetime import datetime
from io import StringIO

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import mapping


def tuple_to_list(record):
    if isinstance(record, tuple):
        record = list(record)
        for i, r in enumerate(record):
            record[i] = tuple_to_list(r)
            return record


def geometry_to_record(row):
    row = mapping(row)
    row["coordinates"] = np.asarray(row["coordinates"]).tolist()
    return json.dumps(row)


REMOVE_COLUMNS = [
    "x",
    "y",
    "lat",
    "lon",
    "latitude",
    "longitude",
    "shape_area",
    "shape_length",
]

fields = json.loads(sys.argv[1].replace("'", '"'))["fields"]
result = sys.stdin.read()
results = json.loads(result)

crs = {"init": "EPSG:4326"}

data = gpd.GeoDataFrame.from_features(results["features"], crs=crs).to_crs(epsg=4326)
# data=data.drop(columns=['X','Y'])
drop_columns = [
    column for column in data.columns.values if column.lower() in REMOVE_COLUMNS
]
data = data.drop(drop_columns, axis=1)
data["geometry"] = data["geometry"].apply(lambda x: geometry_to_record(x))
# drop_columns=[c for c in data.columns.values if c.lower() in REMOVE_COLUMNS]
# data=data.drop(drop_columns,axis=1)
# map column types
columns = {}
for col in ["TIMESTAMP", "INT", "FLOAT", "TEXT"]:
    columns[col] = [field["id"] for field in fields if field["type"].upper() == col]

# reformat DATE column types
for column in columns["TIMESTAMP"]:
    data[column] = data[column].apply(
        lambda x: datetime.utcfromtimestamp(x / 1000).strftime("%Y-%m-%d %H:%M:%S")
        if not pd.isnull(x)
        else x
    )

data = data.replace({pd.np.nan: None}).to_dict(orient="records")

# initialize CSV to hold data since we cannot put back it back in Pandas
csv_string = StringIO()
writer = csv.DictWriter(csv_string, data[0].keys())
writer.writeheader()

# enforce data types
for row in data:
    for column, value in row.items():
        if column in columns["INT"] and value is not None:
            row[column] = int(value)
        elif column in columns["FLOAT"] and value is not None:
            row[column] = float(value)
        elif value:
            row[column] = value.encode("ascii", "ignore").decode()
    writer.writerow(row)

csv_string.seek(0)
sys.stdout.write(csv_string.read())
