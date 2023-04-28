# test_agol_operators - test suite for AGOL operators

import pytest
import requests
import os
import hashlib
import json
import types
import csv

from datetime import datetime

from airflow import DAG
from airflow.models import TaskInstance
from utils_operators.agol_operators import AGOLDownloadFileOperator

# init the base url, without query parameters, of where the data will come from
request_url = "https://services3.arcgis.com/b9WvedVPoizGfvfD/ArcGIS/rest/services/COTGEO_FIRE_FACILITY/FeatureServer/0"


# init the directory where the data will be written to
current_folder = os.path.dirname(os.path.realpath(__file__))
dir = current_folder + "/tmp_dir"
filename = "agol_data.csv"

# make sure there isn't already a file where this test will be writing to
#if os.path.exists(dir + "/" + filename):
#    os.remove( dir + "/" + filename )

# init the dag for the tests
dag = DAG(dag_id='anydag', start_date=datetime.now())

# init the Operator and Task Instance
task = AGOLDownloadFileOperator(
        task_id="get_data",
        request_url=request_url,
        dir=dir,
        filename=filename,
        dag=dag
    )

ti = TaskInstance(task=task, execution_date=datetime.now())

def test_get_fields():
    # checks if the operator parses agol data properly
    # and gets more than one field
    fields = task.get_fields(request_url)

    assert len(fields) > 1

    # checks if returned data is an array of strings
    assert type(fields) == list
    assert all([isinstance(f, str) for f in fields])

def test_agol_generator():
    # checks that making a generator from agol works as expected
    fields = task.get_fields(request_url)

    # checks if generator
    assert isinstance(task.agol_generator(request_url, fields), types.GeneratorType)

    # checks if contents are dicts
    for i in task.agol_generator(request_url, fields):
        assert isinstance(i, dict)

def test_execute():
    # checks all keys are present in operator output
    output = task.execute(ti.get_template_context())
    
    keys = output.keys()
    assert "data_path" in keys
    assert "needs_update" in keys
    


    # checks if data file contains correct data
    with open(output["data_path"]) as f:
        filedata = csv.reader(f)

        # check record count
        res = requests.get(request_url + "/query?where=1%3D1&returnCountOnly=true&f=pjson")
        correct_count = json.loads(res.text)["count"]
        test_count = sum(1 for row in filedata) - 1 # exclude header

        assert correct_count == test_count
