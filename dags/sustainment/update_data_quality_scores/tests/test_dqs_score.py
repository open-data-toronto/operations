from airflow import DAG
import os

from sustainment.update_data_quality_scores import dqs_logic, dqs_utils
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


with open(current_folder + "/death-registry-statistics-package.json") as file:
    package = json.load(file)

with open(current_folder + "/death-registry-statistics-datastore.json") as file:
    datastore = json.load(file)

with open(current_folder + "/ttc-routes-and-schedules.json") as file:
    package_filestore = json.load(file)


df = pd.DataFrame(datastore["records"]).drop("_id", axis=1)
content, fields = df, [x for x in datastore["fields"] if x["id"] != "_id"]


def test_valid_parse_datetime():
    date = "2023-05-03T16:51:13.211496"
    assert isinstance(dqs_utils.parse_datetime(date), int)


def test_invalid_parse_datetime():
    date = "2022-05"

    assert dqs_utils.parse_datetime(date) is None


def test_parse_col_name():
    word = "Test Camel_"
    empty_word = ""

    assert dqs_utils.parse_col_name(word) == ["test", "camel"]
    assert dqs_utils.parse_col_name(empty_word) == []


def test_score_metadata_datastore():
    assert dqs_logic.score_metadata(package, METADATA_FIELDS, fields) == 0.875


def test_score_metadata_filestore():
    assert dqs_logic.score_metadata(package_filestore, METADATA_FIELDS, "") == 1


def test_invalid_score_metadata():
    assert dqs_logic.score_metadata("", METADATA_FIELDS) == 0


def test_score_usability():
    assert dqs_logic.score_usability(fields, content) == 1


def test_score_completeness():
    assert dqs_logic.score_completeness(content) == 1
