# test_http_to_file_operators - test suite for http to file operators

import pytest
import requests
import os

from datetime import datetime

from airflow import DAG
from airflow.models import TaskInstance
from utils_operators.file_operators import DownloadFileOperator

# init the url where the data will come from
file_url = "https://contrib.wp.intra.prod-toronto.ca/app_content/tpp_measures/"
file_url_content = requests.get(file_url).text

# init the directory where the data will be written to
current_folder = os.path.dirname(os.path.realpath(__file__))
dir = current_folder + "/tmp_dir"
filename = "file_data.json"

# make sure there isn't already a file where this test will be writing to
if os.path.exists(dir + "/" + filename):
    os.remove( dir + "/" + filename )

# init the dag for the tests
dag = DAG(dag_id='anydag', start_date=datetime.now())

# init the Operator and Task Instance
task = DownloadFileOperator(
        task_id="get_data",
        file_url=file_url,
        dir=dir,
        filename=filename,
        dag=dag
    )

ti = TaskInstance(task=task, execution_date=datetime.now())

def test_get_data_from_http_request():
    # checks if the operator makes a simple http get correctly
    assert task.get_data_from_http_request(ti).text == file_url_content
    
def test_execute():
    # checks all keys are present in operator output
    output = task.execute(ti.get_template_context())
    keys = output.keys()
    assert "path" in keys
    assert "last_modified" in keys
    assert "checksum" in keys

    # checks file is created in proper location 
    assert os.path.exists(dir + "/" + filename)

    # checks if checksum is correct
    with open( "/data/operations/tests/plugins/utils_operators/test_file_operators_checksum.txt" ) as f:
        file_checksum = f.read()

    assert file_checksum == output["checksum"]