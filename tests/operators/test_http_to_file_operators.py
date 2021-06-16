#!python3
# test_http_to_file_operators - test suite for http to file operators

import pytest
import requests
import os

from datetime import datetime

from airflow import DAG
from airflow.models import TaskInstance
from utils_operators.file_operator import DownloadFileOperator

#from airflow.operators import HTTPGETToFileOperator
current_folder = os.path.dirname(os.path.realpath(__file__))

def test_execute():
    file_url = "https://contrib.wp.intra.prod-toronto.ca/app_content/tpp_measures/"
    dir = current_folder + "/tmp_dir"
    
    dag = DAG(dag_id='anydag', start_date=datetime.now())
    
    task = DownloadFileOperator(
        task_id="get_data",
        file_url=file_url,
        dir=dir,
        filename="src_data.json",
        dag=dag
    )

    #ti = TaskInstance(task=task, execution_date=datetime.now())
    print("USER:")
    print( os.environ["HOME"])
    print("TASK INSTANCE TEMPLATE CONTEXT:")
    #print(ti.get_template_context())

    assert task.execute() == "nope"
