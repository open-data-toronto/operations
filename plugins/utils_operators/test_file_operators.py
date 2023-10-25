# test_http_to_file_operators - test suite for http to file operators

import pytest
import requests
import os

import datetime

from airflow import DAG
from airflow.models import TaskInstance
from utils_operators.file_operators import DownloadFileOperator

# init the url where the data will come from
file_url = "https://httpbin.org/html"
file_url_content = requests.get(file_url).text

# init the directory where the data will be written to
current_folder = os.path.dirname(os.path.realpath(__file__))
dir = current_folder + "/tmp_dir"
filename = "file_data.json"

# make sure there isn't already a file where this test will be writing to
# if os.path.exists(dir + "/" + filename):
#     os.remove(dir + "/" + filename)

DATA_INTERVAL_START = datetime.datetime(2021, 9, 13)
DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=1)

TEST_DAG_ID = "my_custom_operator_dag"
TEST_TASK_ID = "my_custom_operator_task"


@pytest.fixture()
def dag():
    with DAG(
        dag_id=TEST_DAG_ID,
        schedule_interval="@daily",
        default_args={"start_date": DATA_INTERVAL_START},
    ) as dag:
        task = AGOLDownloadFileOperator(
            task_id=TEST_TASK_ID,
            request_url=request_url,
            dir=dir,
            filename=filename,
            #dag=dag
        )
    return dag
  
def test_execute(dag):
    dagrun = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=DATA_INTERVAL_START,
        data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
        start_date=DATA_INTERVAL_END,
        run_type=DagRunType.MANUAL,
    )
    # checks all keys are present in operator output
    output = task.execute(ti.get_template_context())
    keys = output.keys()
    assert "data_path" in keys
    assert "needs_update" in keys

    # checks file is created in proper location 
    assert os.path.exists(dir + "/" + filename)