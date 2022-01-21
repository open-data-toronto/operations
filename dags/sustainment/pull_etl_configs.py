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
import requests
import yaml
import ckanapi
import csv

import datetime

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
        "start_date": datetime.datetime(2022, 1, 1, 0, 0, 0)
    })

DESCRIPTION = "goes to FME, NiFi, and Airflow to pull information about its ETLs"
SCHEDULE = "@once" 
TAGS=["sustainment"]

# Init CKAN
ACTIVE_ENV = "prod"#Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN = ckanapi.RemoteCKAN(**CKAN_CREDS[ACTIVE_ENV])

# FME
def get_fme_nifi_configs(**kwargs):
    fme_nifi_configs_url = "http://opendata.toronto.ca/config/fme_nifi_od_etl_inventory.json"
    try:
        fme_nifi_response = json.loads(requests.get(fme_nifi_configs_url).text)
        print(fme_nifi_response)
        return {"output": fme_nifi_response}
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
                
                airflow_configs.append( {
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
                })
        elif dag.dag_id == "upload_remote_files":
            # upload_remote_files
            with open("/data/operations/dags/sustainment/upload_remote_files/config.yaml", "r") as f:
                remote_items_config = yaml.load(f, yaml.SafeLoader)

            for package_name in (remote_items_config.keys()):
                for resource_name in list(remote_items_config[package_name].keys()):
                    resource = remote_items_config[package_name][resource_name]
                    airflow_configs.append( {
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
                    })
        elif dag.default_args.get("config_folder", False):
            # YAML Jobs
            for config_file in os.listdir( dag.default_args["config_folder"] ):
                if config_file.endswith(".yaml") and config_file[:-5] == dag.dag_id:
                    print(config_file)
                    print(dag.dag_id)

                    # read config file
                    with open(dag.default_args["config_folder"] + "/" + config_file, "r") as f:
                        config = yaml.load(f, yaml.SafeLoader)
                        print(config)
                        package_name = list(config.keys())[0]
                        print(package_name)
                        for resource_name in list(config[package_name]["resources"].keys()):
                            print(resource_name)

                            airflow_configs.append( {
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
                            })
    print(airflow_configs)
    return {"output": airflow_configs}
                        
# Combine Configs w ckan resources
def combine_configs(**kwargs):
    # init output
    output = []

    # get configs from other operators
    fme_nifi_configs = kwargs["ti"].xcom_pull(task_ids="get_fme_nifi_configs")["output"]
    airflow_configs = kwargs["ti"].xcom_pull(task_ids="get_airflow_configs")["output"]

    # make a master list of etl configs
    configs = fme_nifi_configs + airflow_configs

    # combine master list of etl configs with ckan resources
    # get packages
    package_names = CKAN.action.package_list()
    for package_name in package_names:
        package = CKAN.action.package_show(name_or_id=package_name)
        if package_name in [ config["target_package_name"] for config in configs ]:
            # for each resource in a package, try to connect it to an etl config
            for resource in package["resources"]:
                for config in configs:
                    if resource["name"] == config["target_resource_name"] and package_name == config["target_package_name"]:
                        print("Matched! " + resource["name"])
                        output.append({
                            "package_id": package["name"],
                            "resource_name": resource["name"],
                            "extract_job": resource["extract_job"],
                            "refresh_rate": package["refresh_rate"],
                            "is_retired": package["is_retired"],
                            "owner_division": package["owner_division"],
                            "owner_unit": package["owner_unit"],
                            "owner_email": package["owner_email"],
                            "information_url": package["information_url"],
                            "datastore_active": resource["datastore_active"],
                            "format": resource["format"],
                            "last_modified": resource["last_modified"] or resource["created"],
                            **config,
                        })
        else:
            for resource in package["resources"]:
                print("No match - adding " + resource["name"])
                output.append({
                        "package_id": package["name"],
                        "resource_name": resource["name"],
                        "extract_job": resource["extract_job"],
                        "refresh_rate": package["refresh_rate"],
                        "is_retired": package["is_retired"],
                        "owner_division": package["owner_division"],
                        "owner_unit": package["owner_unit"],
                        "owner_email": package["owner_email"],
                        "information_url": package["information_url"],
                        "datastore_active": resource["datastore_active"],
                        "format": resource["format"],
                        "last_modified": resource["last_modified"] or resource["created"],
                        **{key: None for key in configs[0].keys()},
                    })

    return {"output": output}

# Assess, for each resource, a course of action
def assign_action(**kwargs):
     # init output
    output = []

    # get configs from other operators
    resource_configs = kwargs["ti"].xcom_pull(task_ids="combine_configs")["output"]

    # loop through resource configs and assign an action
    for config in resource_configs:
        config["snapshot_or_readme"] = False
        config["action"] = "Unassigned"
        # if config is healthy, assign it no action
        if (
            config["datastore_active"] and
            datetime.datetime.strptime(config["last_modified"][:10], "%Y-%m-%d") > datetime.datetime.now() - datetime.timedelta(days = 1.5*365) and
            config["engine"] and 
            config["information_url"] and 
            config["owner_division"] and 
            config["information_url"] and
            not config["is_retired"]
        ):
            config["action"] = "None"

        # if resource is a snapshot, label it as a snapshot
        if (
            '2010' in config["resource_name"] or
            '2011' in config["resource_name"] or
            '2012' in config["resource_name"] or
            '2013' in config["resource_name"] or
            '2014' in config["resource_name"] or
            '2015' in config["resource_name"] or
            '2016' in config["resource_name"] or
            '2017' in config["resource_name"] or
            '2018' in config["resource_name"] or
            '2019' in config["resource_name"] or
            '2020' in config["resource_name"] or
            '2021' in config["resource_name"]
        ):
            config["snapshot_or_readme"] = "Snapshot"
        if config["datastore_active"]:
            # easy-to-parse formats should be put in datastore
            if config["format"].upper() in ["CSV", "XLS", "XLSX", "JSON", "GEOJSON"]:
                config["action"] = "Put in datastore"
            # old resources should be reassessed with the owner
            elif datetime.datetime.strptime(config["last_modified"][:10], "%Y-%m-%d") < datetime.datetime.now() - datetime.timedelta(days = 1.5*365):
                config["action"] = "Work with Data Owner to update"

        # readmes should be labelled as readmes
        elif "readme" in config["resource_name"].lower():
            config["snapshot_or_readme"] = "README"
        # old resources should be reassessed with the owner
        elif datetime.datetime.strptime(config["last_modified"][:10], "%Y-%m-%d") < datetime.datetime.now() - datetime.timedelta(days = 1.5*365):
            config["action"] = "Work with Data Owner to update"

        output.append(config)
    return {"output": output}


# Write Configs
def write_configs(**kwargs):
    configs = kwargs["ti"].xcom_pull(task_ids="assign_action")["output"]
    f = open("/data/tmp/etl_inventory.csv", "w")
    writer = csv.DictWriter( f, fieldnames = list(configs[0].keys()) )
    writer.writeheader()
    for config in configs:
        writer.writerow(config)
    
    f.close()



# Run DAG
with DAG(
    "pull_etl_configs.py",
    description = DESCRIPTION,
    default_args = DEFAULT_ARGS,
    schedule_interval = SCHEDULE,
    tags=TAGS
) as dag:

    get_fme_nifi_configs = PythonOperator(
        task_id = "get_fme_nifi_configs",
        python_callable = get_fme_nifi_configs,
        provide_context = True
    )

    get_airflow_configs = PythonOperator(
        task_id = "get_airflow_configs",
        python_callable = get_airflow_configs,
        provide_context = True
    )

    combine_configs = PythonOperator(
        task_id = "combine_configs",
        python_callable = combine_configs,
        provide_context = True
    )

    assign_action = PythonOperator(
        task_id = "assign_action",
        python_callable = assign_action,
        provide_context = True
    )

    write_configs = PythonOperator(
        task_id = "write_configs",
        python_callable = write_configs,
        provide_context = True
    )

    get_fme_nifi_configs >> get_airflow_configs >> combine_configs >> assign_action >> write_configs
