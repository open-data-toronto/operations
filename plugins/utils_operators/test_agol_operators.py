# test_agol_operators - test suite for AGOL operators

import pytest
import requests
import os
import hashlib
import json

from datetime import datetime

from airflow import DAG
from airflow.models import TaskInstance
from agol_operators import AGOLDownloadFileOperator
from utils import agol_utils

# init the base url, without query parameters, of where the data will come from
file_url = "https://services3.arcgis.com/b9WvedVPoizGfvfD/arcgis/rest/services/COTGEO_CENSUS_NEIGHBORHOOD/FeatureServer/0/"


# init the directory where the data will be written to
current_folder = os.path.dirname(os.path.realpath(__file__))
dir = current_folder + "/tmp_dir"
filename = "agol_data.json"

# make sure there isn't already a file where this test will be writing to
if os.path.exists(dir + "/" + filename):
    os.remove( dir + "/" + filename )

if os.path.exists(dir + "/fields_" + filename):
    os.remove( dir + "/fields_" + filename )

# init the dag for the tests
dag = DAG(dag_id='anydag', start_date=datetime.now())

# init the Operator and Task Instance
task = AGOLDownloadFileOperator(
        task_id="get_data",
        file_url=file_url,
        dir=dir,
        filename=filename,
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
    assert "data_path" in keys
    assert "fields_path" in keys
    assert "last_modified" in keys
    assert "checksum" in keys


    # checks if data file is created in proper location and contains correct data
    with open( output["data_path"] ) as f:
        filedata = json.load(f)
        assert filedata

    # checks if fields file is in proper location and contains correct fields
    with open( output["fields_path"] ) as f:
        fields_data = json.load(f)

    with open( "test_agol_operators_fields.json" ) as f:
        test_fields_data = json.load(f)

    assert fields_data == test_fields_data
    

    # checks if checksums match
    with open( "test_agol_operators_checksum.txt" ) as f:
        file_checksum = f.read()
    
    assert output["checksum"] == file_checksum