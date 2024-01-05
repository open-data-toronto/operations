'''Receives input YAML dag_ids, runs and assesses dag run results'''

import json
import os
import time
import logging
import re
import yaml
import ckanapi

from utils import misc_utils
from airflow.models import DagRun, DagBag, Variable, TaskInstance

CONFIG_FOLDER = "/data/operations/dags/datasets/files_to_datastore"
CKAN = misc_utils.connect_to_ckan()

def get_dag_ids(**kwargs):
    yamls = os.listdir()

    # if given a list of dag ids, check and return those dag_ids:
    if "dag_ids" in kwargs['dag_run'].conf.keys():
        dag_ids = kwargs['dag_run'].conf.get("dag_ids", None)
        assert dag_ids, "Missing input 'dag_ids'"
        assert isinstance(dag_ids, list), "Input 'dag_ids' object needs to be a list of strings of the names of YAML dags you want to test"
        for dag_id in dag_ids:
            assert f"{dag_id}.yaml" in yamls, "Input 'dag_id' must be a YAML DAG"

    return {"dag_ids": dag_ids}


def run_dag(dag_id, **kwargs):
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

    return dag_runs[0]


def run_dags(dag_ids, **kwargs):
    '''This runs a few tests on an input dag_id:

    - First, it purges its package then runs the DAG to make dataset from scratch
    - Then it runs the DAG again to make sure no update is attempted
    - Then it edits data and runs the DAG to make sure the delta is found
    - Then it spoofs a failure and runs the DAG to check failure protocol
    '''
    for dag_id in dag_ids:
        logging.info(f"Testing {dag_id}...")
        # get metadata from YAML config
        with open(CONFIG_FOLDER + "/" + dag_id + ".yaml", "r") as f:
            config = yaml.load(f, yaml.SafeLoader)

        package_name = list(config.keys())[0]
        resource_count = len(config[package_name]["resources"])

        
        # PURGE PACKAGE, RUN DAG
        # purge package
        CKAN.action.dataset_purge(id = random_name)

        # run DAG and get results
        results = run_dag(dag_id)

        # parse task instance metadata
        for ti in results.get_task_instances():
            if ti.task_id.startswith("prepare_update_") and ti.state == "success":
                updated = True
            else:
                updated = False