# stress_test.py
'''This DAG allows parallel execution, and run_ids reporting to Slack, of DAGs'''

import json
import os
import time
import logging
import re

from datetime import datetime

#from utils import airflow_utils
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from utils_operators.slack_operators import task_success_slack_alert, task_failure_slack_alert, GenericSlackOperator

from airflow.models import DagRun, DagBag

DEFAULT_ARGS = {
        "owner": "Mackenzie",
        "depends_on_past": False,
        "email": ["mackenzie.nichols4@toronto.ca"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "on_failure_callback": task_failure_slack_alert,
        "retries": 0,
        "start_date": datetime(2023, 8, 1, 0, 0, 0)
    }
DESCRIPTION = "Runs multiple input dags at once"
SCHEDULE = "@once" 
TAGS=["sustainment"]


def get_dag_ids(ds, **kwargs):
    output = {}

    # if given a list of dag ids, check and return those dag_idss:
    if "dag_ids" in kwargs['dag_run'].conf.keys():
        dag_ids = kwargs['dag_run'].conf["dag_ids"]
        assert isinstance(dag_ids, list), "Input 'dag_ids' object needs to be a list of strings of the names of dags you want to run"

    return {"dag_ids": dag_ids}


def run_dags(ds, **kwargs):

    # init output
    output = {}

    # establish stress test runtime start time
    test_dag_runs = DagRun.find(dag_id="stress_test")
    test_dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    runtime = test_dag_runs[0].execution_date
    
    # for all of the dag_ids collected the other operator ...
    dag_ids = kwargs.pop("ti").xcom_pull(task_ids="get_dag_ids")["dag_ids"]

    for dag_id in dag_ids :
        # trigger each dag based on dag_ids
        logging.info("Running command: airflow dags trigger " + dag_id)
        assert os.system("airflow dags trigger " + dag_id + " &") == 0, dag_id + " was unable to start!"
    
    # wait for dag to finish and then store its result in output
    # we check each dag one at a time. Once it finishes, we stop checking it
    # once all dags are done, we return their results
    while len(dag_ids):        
        for dag_id in dag_ids:
            this_dag_runs = DagRun.find(dag_id=dag_id)
            this_dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
            if this_dag_runs[0].execution_date > runtime:
                this_dag_run = this_dag_runs[0]
            else:
                time.sleep(10)
                continue

            if this_dag_run.state == "running":
                time.sleep(10)
                continue

            elif this_dag_run.state != "running":                
                logging.info(dag_id + " complete: " + this_dag_run.state)
                dag_ids.remove(dag_id)
                output[dag_id] = this_dag_run.state

                if this_dag_run.state == "success":
                    output[dag_id] += " " + str(this_dag_run.end_date - this_dag_run.start_date)
            
    return {"output": output}



with DAG(
    "stress_test",
    description = DESCRIPTION,
    default_args = DEFAULT_ARGS,
    schedule_interval = SCHEDULE,
    tags=TAGS
) as dag:

    dag.doc_md = """
    ### Summary
    This DAG triggers multiple other input DAGs in parallel, and then reports their success or failure in slack.

    ### How To
    When you trigger this DAG through this Airflow UI, specify the DAGs you want triggered in the configuration JSON, like below:

    ```
    {"dag_ids": ["dag_id1", "dag_id2", "dag_id3"]}
    ```

    ```

    There is no limit to the number of DAGs you can trigger
    """

    get_dag_ids = PythonOperator(
        task_id = "get_dag_ids",
        python_callable = get_dag_ids,
        provide_context = True
    )

    run_dags = PythonOperator(
        task_id = "run_dags",
        python_callable = run_dags,
        provide_context = True
    )

    message_slack = GenericSlackOperator(
        task_id = "message_slack",
        message_header = "Stress Test Report",
        message_content_task_id = "run_dags",
        message_content_task_key = "output",
        message_body = ""
    )

    get_dag_ids >> run_dags >> message_slack