'''Receives input YAML dag_ids, runs and assesses dag run results'''

import json
import os
import time
import logging
import re
import yaml
import ckanapi

from datetime import datetime
from utils import misc_utils
from airflow.models import DagRun, DagBag, Variable, TaskInstance
from utils_operators.slack_operators import task_failure_slack_alert, SlackTownCrier
from airflow.decorators import dag, task

CONFIG_FOLDER = "/data/operations/dags/datasets/files_to_datastore"
TMP_DIR = "data/tmp/"
CKAN = misc_utils.connect_to_ckan()

@dag(
    schedule=None,
    on_failure_callback=task_failure_slack_alert,
    start_date=datetime(2023, 11, 1, 0, 0, 0),
    catchup=False,
)
def test_yaml_dags():
    '''
    ### What
    This DAG tests input YAML DAGs by running them many times against this 
    Airflow isntance's associated CKAN instance.

    ### How
    Run this DAG with a config. That config must have this structure:

    `{dag_ids: ["dag_id1", "dag_id2", "dag_id3"]}`
    '''
    
    @task()
    def get_dag_ids(**kwargs):
        yamls = os.listdir(CONFIG_FOLDER)

        # if given a list of dag ids, check and return those dag_ids:
        if "dag_ids" in kwargs['dag_run'].conf.keys():
            dag_ids = kwargs['dag_run'].conf.get("dag_ids", None)
            assert dag_ids, "Missing input 'dag_ids'"
            assert isinstance(dag_ids, list), "Input 'dag_ids' object needs to be a list of strings of the names of YAML dags you want to test"
            for dag_id in dag_ids:
                assert f"{dag_id}.yaml" in yamls, "Input 'dag_id' {dag_id} must be a YAML DAG"

        return {"dag_ids": dag_ids}


    def run_dag(dag_id, **kwargs):
        # trigger each dag based on dag_ids
        logging.info("\tRunning command: airflow dags trigger " + dag_id)
        assert os.system( "airflow dags trigger " + dag_id) == 0, dag_id + " was unable to start!"

        dag_runs = DagRun.find(dag_id=dag_id)
        dag_runs.sort(key=lambda x: x.execution_date, reverse=True)

        # wait for dag to finish and then store its result in output
        while dag_runs[0].state in ["running", "queued"]:
            time.sleep(5)
            dag_runs = DagRun.find(dag_id=dag_id)
            dag_runs.sort(key=lambda x: x.execution_date, reverse=True)

        return dag_runs[0]

    @task()
    def run_dags(dag_ids, **kwargs):
        '''This runs a few tests on an input dag_id:

        - First, it purges its package then runs the DAG to make dataset from scratch
        - Then it runs the DAG again to make sure no update is attempted
        - Then it edits data and runs the DAG to make sure the delta is found
        - Then it spoofs a failure and runs the DAG to check failure protocol
        '''
        output = {}
        for dag_id in dag_ids["dag_ids"]:
            logging.info(f"Testing {dag_id}...")
            output[dag_id] = {}
            # get metadata from YAML config
            with open(CONFIG_FOLDER + "/" + dag_id + ".yaml", "r") as f:
                config = yaml.load(f, yaml.SafeLoader)

            package_name = list(config.keys())[0]
            resource_count = len(config[package_name]["resources"])

            
            ### PURGE PACKAGE, RUN DAG, EXPECT DATASET CREATED
            logging.info(f"\tTesting {dag_id} purge and run...")
            # purge package
            try:        
                CKAN.action.dataset_purge(id = package_name)
            except:
                logging.warning(f"{package_name} was not purged")

            # run DAG and get results
            run = run_dag(dag_id)

            # parse task instance metadata
            results = get_results(run)

            output[dag_id]["test_create_success"] = results["updated"] and results["state"] == "success"


            ### RUN DAG, RUN DAG, EXPECT NO UPDATE TO DATASET
            logging.info(f"\tTesting {dag_id} run with no delta...")
            # run DAG and get results
            run = run_dag(dag_id)

            # parse task instance metadata
            results = get_results(run)

            output[dag_id]["test_no_update_success"] = results["updated"] == False and results["state"] == "success"


            ### EDIT DATA, RUN DAG, EXPECT UPDATE TO DATASET
            logging.info(f"\tTesting {dag_id} run with delta...")
            # edit data stored on airflow server
            tmp_filenames = os.listdir(f"{TMP_DIR}{package_name}")
            for filename in tmp_filenames:
                with open(f"{TMP_DIR}{package_name}/{filename}", "a") as f:
                    f.write("random line of text")
                    f.close()

            # run DAG and get results
            run = run_dag(dag_id)

            # parse task instance metadata
            results = get_results(run)

            output[dag_id]["test_update_success"] = results["updated"] == True and results["state"] == "success"

            ### TODO: RUN DAG, FORCE ERROR, EXPECT FAILURE PROTOCOL TO RUN

        print(output)
        print(type(output))
        return output
                

    def get_results(run):
        updated = False
        failure_protocol = False
        for ti in run.get_task_instances():

            if ti.task_id.startswith("prepare_update_") and ti.state == "success":
                updated = True

            if ti.task_id.startswith("restore_backup_") and ti.state == "success":
                failure_protocol = True
                                

        return {
            "updated": updated,
            "failure_protocol": failure_protocol,
            "state": run.state,
        }

    @task
    def write_to_slack(results):

        # parse results
        failures = {}
        tests_run = 0
        yamls_tested = 0

        for k1,v1 in results.items():
            yamls_tested += 1
            for k2,v2 in results[k1].items():
                tests_run += 1
                if v2 == False:
                    failures[f"{k1} {k2}"] = "failed"

        if len(failures) == 0:
            failures = ":+1: All tests ran successfully"

        body = f"\n\t\t   {tests_run} tests run\n\t\t   {yamls_tested} :yaml:s tested"

        # write to slack
        SlackTownCrier(
            dag_id="test_yaml_dags",
            message_header = "YAML DAGs Test Complete",
            message_content = failures, 
            message_body = body,
        ).announce()


    write_to_slack(run_dags(get_dag_ids()))

test_yaml_dags()