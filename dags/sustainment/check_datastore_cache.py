# check_datastore_cache.py
"""
Compares datastore cache last update to resource last update for each package in CKAN and, where needed, updates the datastore cache for a whole package
"""
import requests
import logging
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

from utils_operators.slack_operators import task_success_slack_alert, task_failure_slack_alert, GenericSlackOperator
from ckan_operators.package_operator import GetAllPackagesOperator, AssertIdenticalPackagesOperator

from utils import airflow_utils

# standard arguments to be used throughout this DAG
ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN_ADDRESS = CKAN_CREDS[ACTIVE_ENV]["address"]

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
        "start_date": datetime(2022, 5, 4, 0, 0, 0)
    })
    
DESCRIPTION = "For each datastore resource in CKAN, check whether its datastore cache is up-to-date"
SCHEDULE = "0 */3 * * *" # minute 1 at noon and midnight on weekdays
TAGS=["sustainment"]

# custom function to determine whether a package contains a datastore resource that needs its datastore cache updated
def find_outdated_datastore_caches(**kwargs):
    output = []
    packages = kwargs.pop("ti").xcom_pull(task_ids="get_packages")["packages"]

    for package in packages:

        for resource in package["resources"]:
            if resource["datastore_active"] == True:
                comparison_attribute = "last_modified" if resource["last_modified"] else "created"

                # if resource doesnt have a cache date, try caching it
                if not resource.get("datastore_cache_last_update"):
                    output.append( package["name"] )
                    continue

                # if datastore_cache_last_update is earlier than the resource was created or last modified, then run datastore_cache
                if datetime.strptime(resource["datastore_cache_last_update"][:19], "%Y-%m-%dT%H:%M:%S") < datetime.strptime(resource[comparison_attribute][:19], "%Y-%m-%dT%H:%M:%S") :
                    output.append( package["name"] )
                    continue
    # remove dupes
    output = set(output)
    logging.info("Found {} packages to run datastore_cache on: {}".format(str(len(output)), str(output)))
    return {"output": output}

def trigger_datastore_cache(**kwargs):
    output_dict = {
        "2": "successfully cached",
        "5": "failed",
        "4": "failed",
    }
    output = {}
    package_names = kwargs.pop("ti").xcom_pull(task_ids="find_outdated_datastore_caches")["output"]
    headers = {"Authorization": CKAN_CREDS[ACTIVE_ENV]["apikey"]}

    for package_name in package_names:
        logging.info("Starting on " + package_name)
        data = {"package_id": package_name}    
        output[package_name] = output_dict[str(requests.post(CKAN_ADDRESS + "/api/3/action/datastore_cache", headers = headers, data=data).status_code)[0]]
        logging.info("Finished " + package_name)
    if len(output):
        return {"output": output}





with DAG(
    "check_datastore_cache",
    description = DESCRIPTION,
    default_args = DEFAULT_ARGS,
    schedule_interval = SCHEDULE,
    tags=TAGS,
    catchup=False,
) as dag:

    get_packages = GetAllPackagesOperator(
        task_id = "get_packages"
    )

    find_outdated_datastore_caches = PythonOperator(
        task_id = "find_outdated_datastore_caches",
        python_callable = find_outdated_datastore_caches,
    )

    trigger_datastore_cache = PythonOperator(
        task_id = "trigger_datastore_cache",
        python_callable = trigger_datastore_cache,
    )

    message_slack = GenericSlackOperator(
        task_id = "message_slack",
        message_header = "Datastore Cache Check Report",
        message_content_task_id = "trigger_datastore_cache",
        message_content_task_key = "output",
        message_body = ""
    )

    get_packages >> find_outdated_datastore_caches >> trigger_datastore_cache >> message_slack