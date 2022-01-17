# pull_etl_configs - goes to FME, NiFi, and Airflow to pull information about its ETLs
# pulls many fields for each ETL:
#   Dataset names
#   Engine (FME, Airflow, NiFi, etc)
#   Schedule (Daily, weekly, cron, ad hoc, etc)
#   ?Schedule Flexibility (Fixed, Flexible)?
#   Config Location (filepath to an artifact detailing this ETL)
#   Logs Location (filepath, email inbox, slack channel where success/failure is logged)
#   OD Owner (responsible for getting broken ETLs fixed)
#   COT Department (responsible for communicating wanted changes to the ETL to OD)
#   ?Date Created?
#   ?Active/Inactive?
#   brief english description


import json
import os
import time
import logging
import re
import requests
import yaml

from datetime import datetime

from utils import airflow_utils
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from utils_operators.slack_operators import task_success_slack_alert, task_failure_slack_alert, GenericSlackOperator

from airflow.models import DagRun, DagBag

DEFAULT_ARGS = airflow_utils.get_default_args(
    {
        "owner": "Mackenzie",
        "depends_on_past": False,
        "email": ["mackenzie.nichols4@toronto.ca"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "on_failure_callback": task_failure_slack_alert,
        "retries": 0,
        "start_date": datetime(2022, 1, 1, 0, 0, 0)
    })

DESCRIPTION = "goes to FME, NiFi, and Airflow to pull information about its ETLs"
SCHEDULE = "@once" 
TAGS=["sustainment"]

# FME
def get_fme_configs(**kwargs):
    fme_url = "http://j2xd0116717:8080/fmerest/v3/schedules?category=OpenData&includeAutomations=false&limit=-1&offset=-1"
    fme_payload={}
    fme_headers = {'Authorization': 'fmetoken token=' + Variable.get("fme_token")}
    try:
        fme_response = json.loads(requests.request("GET", fme_url, headers=fme_headers, data=fme_payload).text)
        print(fme_response)
    except:
        pass

# Airflow
def get_airflow_configs(**kwargs):
    airflow_configs = []
    # Legacy custom DAGs
    dagbag = DagBag()
    for dag in dagbag.dags.values():
        print(dag)
        if "etl_mapping" in dag.default_args.keys():
            for mapping in dag.default_args["etl_mapping"]:
                #print("source: " + mapping["source"])
                #print("target_package_name: " + mapping["target_package_name"])
                #print("target_resource_name: " + mapping["target_resource_name"])
                #print("schedule_interval: " + dag.schedule_interval)
                #print("owner: " + dag.owner)
                #print("dag_id: " + dag.dag_id)
                #print("description: " + dag.description)

                airflow_configs += {
                    "source_data" : mapping["source"],
                    "target_package_name": mapping["target_package_name"],
                    "target_resource_name": mapping["target_resource_name"],
                    "engine": "Airflow",
                    "Schedule" : dag.schedule_interval,
                    #"?Schedule Flexibility (Fixed, Flexible)?" : "",
                    "config_location" : dag.filepath,
                    #"Logs Location (filepath, email inbox, slack channel where success/failure is logged)" : "",
                    "od_owner" : dag.owner,
                    "etl_description": dag.description,
                    #"COT Department (responsible for communicating wanted changes to the ETL to OD)" : "",
                    #"?Date Created?" : "",
                    #"?Active/Inactive?" : "",
                }
        elif dag.dag_id == "upload_remote_files":
            # upload_remote_files
            with open("/data/operations/dags/sustainment/upload_remote_files/config.yaml", "r") as f:
                remote_items_config = yaml.load(f, yaml.SafeLoader)

            for package_name in (remote_items_config.keys()):
                for resource_name in list(remote_items_config[package_name].keys()):
                    resource = remote_items_config[package_name][resource_name]
                    airflow_configs += {
                        "source_data" : resource["url"],
                        "target_package_name": package_name,
                        "target_resource_name": resource_name,
                        "engine": "Airflow",
                        "Schedule" : dag.schedule_interval,
                        #"?Schedule Flexibility (Fixed, Flexible)?" : "",
                        "config_location" : dag.filepath,
                        #"Logs Location (filepath, email inbox, slack channel where success/failure is logged)" : "",
                        "od_owner" : dag.owner,
                        "etl_description": dag.description,
                        #"COT Department (responsible for communicating wanted changes to the ETL to OD)" : "",
                        #"?Date Created?" : "",
                        #"?Active/Inactive?" : "",
                    }
        elif dag.default_args.get("config_folder", False):
            # YAML Jobs
            for config_file in os.listdir( dag.default_args["config_folder"] ):
                if config_file.endswith(".yaml"):

                    # read config file
                    with open(dag.default_args["config_folder"] + "/" + config_file, "r") as f:
                        config = yaml.load(f, yaml.SafeLoader)
                        package_name = list(config.keys())[0]
                        for resource_name in list(config[package_name]["resources"].keys()):
                            airflow_configs += {
                                "source_data" : config[package_name]["resources"][resource_name]["url"],
                                "target_package_name": package_name,
                                "target_resource_name": resource_name,
                                "engine": "Airflow",
                                "Schedule" : dag.schedule_interval,
                                #"?Schedule Flexibility (Fixed, Flexible)?" : "",
                                "config_location" : dag.default_args["config_folder"] + "/" + config_file,
                                #"Logs Location (filepath, email inbox, slack channel where success/failure is logged)" : "",
                                "od_owner" : dag.owner,
                                "etl_description": config[package_name]["excerpt"],
                                #"cot_owner" : config[package_name]["owner_division"],
                                #"?Date Created?" : "",
                                #"?Active/Inactive?" : "",
                            }
                        


# Run DAG
with DAG(
    "pull_etl_configs.py",
    description = DESCRIPTION,
    default_args = DEFAULT_ARGS,
    schedule_interval = SCHEDULE,
    tags=TAGS
) as dag:

    get_fme_configs = PythonOperator(
        task_id = "get_fme_configs",
        python_callable = get_fme_configs,
        provide_context = True
    )

    get_airflow_configs = PythonOperator(
        task_id = "get_airflow_configs",
        python_callable = get_airflow_configs,
        provide_context = True
    )
    get_fme_configs >> get_airflow_configs
