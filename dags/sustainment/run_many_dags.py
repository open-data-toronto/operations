# run_many_dags.py


import json
import os
import time
import logging
import re
import yaml
import ckanapi

from datetime import datetime

#from utils import airflow_utils
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from utils_operators.slack_operators import task_success_slack_alert, task_failure_slack_alert, GenericSlackOperator

from airflow.models import DagRun, DagBag, Variable, TaskInstance

DEFAULT_ARGS = {
        "owner": "Mackenzie",
        "depends_on_past": False,
        "email": ["mackenzie.nichols4@toronto.ca"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "on_failure_callback": task_failure_slack_alert,
        "retries": 0,
        "start_date": datetime(2021, 11, 1, 0, 0, 0)
    }
DESCRIPTION = "Runs multiple input dags"
SCHEDULE = "@once" 
TAGS=["sustainment"]


def get_dag_ids(ds, **kwargs):
    output = {}

    # if given a list of operators, then run dags that contain those operators
    if "operators" in kwargs['dag_run'].conf.keys():
        # init dagbag, list of operators to lowercase and empty list of dags to run
        dagbag = DagBag()
        dag_ids = []
        operators = [operator.lower() for operator in kwargs['dag_run'].conf["operators"]]
        assert isinstance(operators, list) and len(operators), "Input 'operators' object needs to be a list of strings of the names of operators you want in the dags to run"

        # for *every* dag in this airflow instance
        for dag in dagbag.dags.values():
            for task in dag.tasks:
                if type(task).__name__.lower() in operators:
                    dag_ids.append(dag.dag_id)
                    continue

    # if given a list of dag ids, check and return those dag_ids:
    if "dag_ids" in kwargs['dag_run'].conf.keys():
        dag_ids = kwargs['dag_run'].conf["dag_ids"]
        assert isinstance(dag_ids, list), "Input 'dag_ids' object needs to be a list of strings of the names of dags you want to run"

    return {"dag_ids": dag_ids}


def run_dags(ds, **kwargs):

    # init output
    output = {}
    # for all of the dag_ids collected the other operator ...
    dag_ids = kwargs.pop("ti").xcom_pull(task_ids="get_dag_ids")["dag_ids"]

    for dag_id in dag_ids :
        # trigger each dag based on dag_ids
        logging.info("Running command: airflow dags trigger " + dag_id)
        assert os.system( "airflow dags trigger " + dag_id) == 0, dag_id + " was unable to start!"

        dag_runs = DagRun.find(dag_id=dag_id)
        dag_runs.sort(key=lambda x: x.execution_date, reverse=True)

        # wait for dag to finish and then store its result in output
        while dag_runs[0].state in ["running", "queued"]:
            time.sleep(5)
            dag_runs = DagRun.find(dag_id=dag_id)
            dag_runs.sort(key=lambda x: x.execution_date, reverse=True)

        # parse task instance metadata
        for ti in dag_runs[0].get_task_instances():
            if ti.task_id.startswith("prepare_update_") and ti.state == "success":
                updated = True
            else:
                updated = False

        logging.info(dag_id + " complete: " + dag_runs[0].state)
        output[dag_id] = {
            "state": dag_runs[0].state, 
            "start_date": dag_runs[0].start_date, 
            "end_date": dag_runs[0].end_date, 
            "updated": updated,

        }

    return {"output": output}


def assess_results(**kwargs):

    # Init CKAN
    ACTIVE_ENV = Variable.get("active_env")
    CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
    CKAN_APIKEY = CKAN_CREDS[ACTIVE_ENV]["apikey"]  #
    CKAN_ADDRESS = CKAN_CREDS[ACTIVE_ENV]["address"]
    CKAN = ckanapi.RemoteCKAN(**CKAN_CREDS[ACTIVE_ENV])

    # init configs folder
    CONFIG_FOLDER = "/data/operations/dags/datasets/files_to_datastore"

    # init output
    message = ""

    # get dag run results
    results = kwargs.pop("ti").xcom_pull(task_ids="run_dags")["output"]

    # loop through each result from each run DAG and get more details
    for dag_id, result in results.items():

        # get YAML config info
        with open(CONFIG_FOLDER + "/" + dag_id + ".yaml", "r") as f:
            config = yaml.load(f, yaml.SafeLoader)

        # get package info
        package = CKAN.action.package_show(name_or_id=dag_id)


        # find resource from YAML config in CKAN package
        message += "\n*{}*: {} in {}".format(package["name"], result["state"], str(result["end_date"] - result["start_date"]))
        for r_conf_name, r_conf in config[dag_id]["resources"].items():      
            for r in package["resources"]:                
                if r_conf_name == r["name"]:
                    message += "\n\t\t   {}:".format(r["name"])
                    
                    # get whether data was updated or not
                    updated = False
                    
                    if result["updated"]:
                        updated = True

                        # get inserted records and fields
                        datastore_resource = CKAN.action.datastore_search(resource_id=r["id"])
                        record_count = datastore_resource["total"]
                        field_count = len(datastore_resource["fields"]) - 1 # dont count _id field
                        cache_file_count = len(r["datastore_cache"].keys())

                        message += """
                                Records inserted: {}
                                Fields inserted: {}
                                Files cached: {}
                        """.format(record_count, field_count, cache_file_count)
                    
                    if not updated:
                        message += "\n\t\t   No update occurred"

    return {"output": message}
                    
with DAG(
    "run_many_dags",
    description = DESCRIPTION,
    default_args = DEFAULT_ARGS,
    schedule_interval = SCHEDULE,
    tags=TAGS
) as dag:

    dag.doc_md = """
    ### Summary
    This DAG triggers multiple other input DAGs one by one, and then reports their success or failure in slack.

    ### How To
    When you trigger this DAG through this Airflow UI, specify the DAGs you want triggered in the configuration JSON, like below:

    ```
    {"dag_ids": ["dag_id1", "dag_id2", "dag_id3"]}
    ```

    You can also run any dag containing a particular operator, like below:
    
    ```
    {"operators": ["operator1", "operator2", "operator3"]}
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

    assess_results = PythonOperator(
        task_id = "assess_results",
        python_callable = assess_results,
        provide_context = True
    )

    message_slack = GenericSlackOperator(
        task_id = "message_slack",
        message_header = "Test DAG Run Report",
        message_content_task_id = "assess_results",
        message_content_task_key = "output",
        message_body = ""
    )

    get_dag_ids >> run_dags >> assess_results >> message_slack