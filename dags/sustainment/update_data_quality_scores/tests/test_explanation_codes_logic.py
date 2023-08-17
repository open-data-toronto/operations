from airflow import DAG
from sustainment.update_data_quality_scores import explanation_codes_logic, dqs_utils
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


with open(current_folder + "/death-registry-statistics-package.json") as file:
    package = json.load(file)

with open(current_folder + "/death-registry-statistics-datastore.json") as file:
    datastore = json.load(file)

with open(current_folder + "/ttc-routes-and-schedules.json") as file:
    package_filestore = json.load(file)

df = pd.DataFrame(datastore["records"]).drop("_id", axis=1)
content, fields = df, [x for x in datastore["fields"] if x["id"] != "_id"]


def test_attribue_description_check():
    assert dqs_utils.attribue_description_check(fields) == (False, [])


def test_metadata_explanation_code_datastore():
    METADATA_FIELDS = [
        "notes",
        "topics",
        "owner_email",
        "information_url",
    ]
    assert (
        explanation_codes_logic.metadata_explanation_code(
            package, METADATA_FIELDS, fields
        )
        == "~metadata_missing:information_url"
    )


def test_metadata_explanation_code_filestore():
    METADATA_FIELDS = [
        "notes",
        "topics",
        "owner_email",
        "information_url",
    ]
    assert (
        explanation_codes_logic.metadata_explanation_code(
            package_filestore, METADATA_FIELDS
        )
        == ""
    )


def test_usability_explanation_code():
    assert explanation_codes_logic.usability_explanation_code(fields, content) == ""


def test_completeness_explanation_code():
    assert explanation_codes_logic.completeness_explanation_code(content) == ""


def test_calculate_final_scores_and_codes():
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

    df = pd.read_csv(current_folder + "/raw_scores_and_codes.csv").set_index(
        ["package", "resource"], drop=True
    )

    output = explanation_codes_logic.calculate_final_scores_and_codes(
        df, WEIGHTS_DATASTORE, WEIGHTS_FILESTORE, DIMENSIONS, BINS
    )

    category_type = pd.CategoricalDtype(
        categories=["Bronze", "Silver", "Gold"], ordered=True
    )
    expected_output = pd.read_csv(
        current_folder + "/final_scores_and_codes.csv",
        dtype={"grade": category_type},
    )

    # check output expected format
    assert output.shape == (255, 17)

    """
    check if file exactly same, exclude "recorded_at" column,
    since recording time has been changing over time, not eligible for comparison
    """

    assert (
        output.loc[:, output.columns != "recorded_at"].equals(
            expected_output.loc[:, expected_output.columns != "recorded_at"]
        )
        is True
    )
