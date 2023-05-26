from airflow import DAG
from sustainment.update_data_quality_scores import explanation_codes_logic
import os
import json
import pandas as pd

# init the directory where the data will be written to
current_folder = os.path.dirname(os.path.realpath(__file__))

METADATA_FIELDS = [
    "notes",
    "topics",
    "owner_email",
    "information_url",
]

WEIGHTS_DATASTORE = [0.35, 0.35, 0.15, 0.1, 0.05]
WEIGHTS_FILESTORE = [0.41, 0.41, 0.18]
BINS = {
    "Bronze": 0.6,
    "Silver": 0.8,
    "Gold": 1,
}
DIMENSIONS = [
    "metadata",
    "freshness",
    "accessibility",
    "completeness",
    "usability",
]

with open(current_folder + "/death-registry-statistics-package.json") as file:
    package = json.load(file)["result"]

with open(current_folder + "/death-registry-statistics-datastore.json") as file:
    datastore = json.load(file)["result"]


df = pd.DataFrame(datastore["records"]).drop("_id", axis=1)
content, fields = df, [x for x in datastore["fields"] if x["id"] != "_id"]

def test_attribue_description_check():
    assert explanation_codes_logic.attribue_description_check(fields) == (False, [])

def test_metadata_explanation_code():
    assert explanation_codes_logic.metadata_explanation_code(package, METADATA_FIELDS, fields) == "~metadata_missing:information_url"

def test_usability_explanation_code():
    assert explanation_codes_logic.usability_explanation_code(fields, content) == ""

def test_completeness_explanation_code():
    assert explanation_codes_logic.completeness_explanation_code(content) == ""

def test_prepare_and_normalize_scores():
    packages = [package]
    tmp_dir = current_folder
    raw_explanation_code_path = current_folder + "/raw_scores.parquet"
    filepath = explanation_codes_logic.prepare_and_normalize_scores()
    
    assert os.path.exists(current_folder + "/" + "final_scores_and_codes.parquet")
    



   