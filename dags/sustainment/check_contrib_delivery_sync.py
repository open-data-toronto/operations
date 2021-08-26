# check_contrib_delivery_sync.py
"""
This DAG reads packages and resources from CKAN PROD Contrib and Delivery
It then compares them. If they are not identical, an error message is sent via slack.
"""
import json
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

from utils_operators.directory_operator import CreateLocalDirectoryOperator, DeleteLocalDirectoryOperator
from utils_operators.agol_operators import AGOLDownloadFileOperator
from utils_operators.slack_operators import task_success_slack_alert, task_failure_slack_alert, GenericSlackOperator
from ckan_operators.package_operator import GetAllPackagesOperator, AssertIdenticalPackagesOperator

from utils import airflow_utils


ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)

CONTRIB_ADDRESS = "https://ckanadmin0.intra.prod-toronto.ca/"
DELIVERY_ADDRESS = "https://ckan0.cf.opendata.inter.prod-toronto.ca/"

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
        "start_date": datetime(2021, 8, 12, 0, 0, 0)

    })
DESCRIPTION = "Compares CKAN Contrib to Delivery and returns slack message if they aren't identical"
SCHEDULE = "1 0,12 * * 1-5" # minute 1 at noon and midnight on weekdays
TAGS=["sustainment"]


with DAG(
    "check_contrib_delivery_sync",
    description = DESCRIPTION,
    default_args = DEFAULT_ARGS,
    schedule_interval = SCHEDULE,
    tags=TAGS
) as dag:



    get_contrib_packages = GetAllPackagesOperator(
        task_id = "get_contrib_packages",
        address = CONTRIB_ADDRESS,
        apikey = ""
    )

    get_delivery_packages = GetAllPackagesOperator(
        task_id = "get_delivery_packages",
        address = DELIVERY_ADDRESS,
        apikey = ""
    )

    check_if_packages_match = AssertIdenticalPackagesOperator(
        task_id = "check_if_packages_match",
        package_a_task_id = "get_contrib_packages",
        package_a_task_key = "packages",
        package_b_task_id = "get_delivery_packages",
        package_b_task_key = "packages"
    )

    job_success = DummyOperator(
            task_id = "job_success",
            trigger_rule="all_success"
    )

    job_failed = DummyOperator(
        task_id = "job_failed",
        trigger_rule="one_failed"
    )

    slack_failure_message = GenericSlackOperator(
        task_id = "slack_failure_message",
        message_header = "CKAN PROD Contrib and Delivery",
        message_content = ":exclamation: Out of Sync :exclamation:",
        message_body = ""
    )

    slack_success_message = GenericSlackOperator(
        task_id = "slack_success_message",
        message_header = "CKAN PROD Contrib and Delivery",
        message_content = ":+1: In Sync :+1:",
        message_body = ""
    )

    [get_contrib_packages, get_delivery_packages] >> check_if_packages_match
    check_if_packages_match >> job_success >> slack_success_message
    check_if_packages_match >> job_failed >> slack_failure_message