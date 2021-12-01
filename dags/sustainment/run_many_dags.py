# run_many_dags.py


import json
import os

from datetime import datetime

from utils import airflow_utils
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator



DEFAULT_ARGS = airflow_utils.get_default_args(
    {
        "owner": "Mackenzie",
        "depends_on_past": False,
        "email": ["mackenzie.nichols4@toronto.ca"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        #"on_failure_callback": task_failure_slack_alert,
        "retries": 0,
        "start_date": datetime(2021, 11, 1, 0, 0, 0)

    })
DESCRIPTION = "Runs multiple input dags"
SCHEDULE = "@once" 
TAGS=["sustainment"]

def runner(ds, **kwargs):
    for dag in kwargs['dag_run'].conf["dags"] :
        print("airflow dags trigger " + dag)
        os.system( "airflow dags trigger " + dag)



with DAG(
    "run_many_dags",
    description = DESCRIPTION,
    default_args = DEFAULT_ARGS,
    schedule_interval = SCHEDULE,
    tags=TAGS
) as dag:

    get_dags = PythonOperator(
        task_id = "get_dags",
        python_callable = runner,
        provide_context = True
    )

    #run_dags = BashOperator(
    #    task_id = "run_dags",
    #    bash_command = "airflow dags trigger {{ dag_run.conf['dags'] }}"
    #)

    get_dags