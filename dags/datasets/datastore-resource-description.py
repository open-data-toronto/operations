
from datetime import datetime, timedelta
from textwrap import dedent
import requests
import logging

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from ckan_operators.package_operator import GetCkanPackageListOperator, CheckCkanResourceDescriptionOperator, PrintCkanResponseOperator, GetAllPackagesOperator
from utils_operators.slack_operators import GenericSlackOperator

# init hardcoded vars for these dags
ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN = CKAN_CREDS[ACTIVE_ENV]["address"]
CKAN_APIKEY = CKAN_CREDS[ACTIVE_ENV]["apikey"]

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
with DAG(
    'description',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='A simple tutorial DAG',
    schedule_interval="@once",
    start_date=datetime(2021, 1, 1),
) as dag:

    get_package = GetCkanPackageListOperator(
        task_id = "get_package", 
        address=CKAN, 
        apikey=CKAN_APIKEY,
    )
 
    check_resource_description = CheckCkanResourceDescriptionOperator(
        task_id="check_resource_description", 
        input_dag_id="get_package",
        base_url = "https://ckanadmin0.intra.dev-toronto.ca/api/3/action/package_show?id=",
        resource_url = "https://ckanadmin0.intra.dev-toronto.ca/api/3/action/datastore_search?resource_id="
    )

    slack_notificaiton = GenericSlackOperator(
            task_id = "slack_notificaiton",
            message_header = "resource description checking",
            message_content_task_id = "check_resource_description",
            message_content_task_key = "package_and_resource",
            message_body = "Resource description MISSING!"
        )
   
    get_package >> check_resource_description >> slack_notificaiton
