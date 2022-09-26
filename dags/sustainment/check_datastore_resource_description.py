
from datetime import datetime, timedelta
import requests
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from ckan_operators.package_operator import GetCkanPackageListOperator
from ckan_operators.datastore_operator import CheckCkanResourceDescriptionOperator
from utils_operators.slack_operators import GenericSlackOperator, task_failure_slack_alert

# init hardcoded vars for these dags
ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN = CKAN_CREDS[ACTIVE_ENV]["address"]
CKAN_APIKEY = CKAN_CREDS[ACTIVE_ENV]["apikey"]

with DAG(
    'check-datastore-description',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
                "owner": "Yanan",
                "depends_on_past": False,
                "email": "yanan.zhang@toronto.ca",
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 2,
                "retry_delay": 3,
                "on_failure_callback": task_failure_slack_alert,
                "tags": ["sustainment"]
            },
    description='Datastore description check, send notification to slack if description missing',
    schedule_interval="@once",
    start_date=datetime(2022, 9, 26, 0, 0, 0),
) as dag:

    get_package = GetCkanPackageListOperator(
        task_id = "get_package", 
        address=CKAN, 
        apikey=CKAN_APIKEY,
    )
 
    check_resource_description = CheckCkanResourceDescriptionOperator(
        task_id="check_resource_description", 
        input_dag_id="get_package",
        address = CKAN,
    )

    slack_notificaiton = GenericSlackOperator(
            task_id = "slack_notificaiton",
            message_header = "resource description checking",
            message_content_task_id = "check_resource_description",
            message_content_task_key = "package_and_resource",
            message_body = "Resource description MISSING!"
        )
   
    get_package >> check_resource_description >> slack_notificaiton
