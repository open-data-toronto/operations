# test_agol_operators - test suite for AGOL operators

import pytest
import requests
import os
import hashlib
import json

from datetime import datetime

from airflow import DAG
from airflow.models import TaskInstance
from utils_operators.agol_operators import AGOLDownloadFileOperator
from utils import agol_utils


#from airflow.operators import HTTPGETToFileOperator

# init the url where the data will come from
file_url = "https://services.arcgis.com/S9th0jAJ7bqgIRjw/arcgis/rest/services/Reported_Crimes_ASR_RC_TBL_001/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json"
file_url_content = agol_utils.get_data(file_url)
file_url_fields = agol_utils.get_fields(file_url)


# init the directory where the data will be written to
current_folder = os.path.dirname(os.path.realpath(__file__))
dir = current_folder + "/tmp_dir"

# init the dag for the tests
dag = DAG(dag_id='anydag', start_date=datetime.now())

# init the Operator and Task Instance
task = AGOLDownloadFileOperator(
        task_id="get_data",
        file_url=file_url,
        dir=dir,
        filename="agol_data.json",
        dag=dag
    )

ti = TaskInstance(task=task, execution_date=datetime.now())

def test_parse_data_from_agol():
    # checks if the operator parses agol data properly
    # and gets more than one record returned
    assert len(task.parse_data_from_agol()) > 1

    # checks if returned data is an array
    assert type(task.parse_data_from_agol()["data"]) == list

def test_execute():
    # checks all keys are present in operator output
    output = task.execute(ti.get_template_context())
    keys = output.keys()
    assert "path" in keys
    assert "last_modified" in keys
    # assert "checksum" in keys


    # checks if data file is created in proper location and contains correct data
    with open( output["path"] ) as f:
        filedata = json.load(f)

    assert filedata == file_url_content

    # checks if fields file is in proper location and contains correct fields
    with open( output["fields_path"] ) as f:
        fields_data = json.load(f)

    assert fields_data == file_url_fields