import requests
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from ckan_operators.package_operator import GetCkanPackageListOperator
from utils_operators.slack_operators import GenericSlackOperator, task_failure_slack_alert

# init hardcoded vars for these dags
ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN = CKAN_CREDS[ACTIVE_ENV]["address"]
CKAN_APIKEY = CKAN_CREDS[ACTIVE_ENV]["apikey"]

with DAG(
    'information-url-checker',
    default_args={
                "owner": "Yanan",
                "depends_on_past": False,
                "email": "yanan.zhang@toronto.ca",
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 1,
                "retry_delay": 3,
                "on_failure_callback": task_failure_slack_alert,
                "tags": ["sustainment"]
            },
    description='Check if information_url(Under dataset description section) accessible, send notification to slack on failure urls',
    schedule_interval="@once",
    start_date=datetime(2023, 1, 27, 0, 0, 0),
) as dag:
    
    def information_url_checker(**kwargs):
        #package = kwargs.pop("ti").xcom_pull(task_ids="get_or_create_package")
        ti = kwargs.pop("ti")
        package_list = ti.xcom_pull(task_ids="get_package_list")
        address = kwargs.pop("address")
        
        result_info = {}
        for package_name in package_list:
            url = address + "api/3/action/package_show?id=" + package_name
            response = requests.get(url).json()['result']
            if "information_url" in response.keys():
                information_url = response['information_url']
                try:
                    response = requests.get(information_url)
                    status = response.status_code
                    reason = response.reason
                    logging.info(f"---Status-{status}--Reason--{reason}---{package_name}----{information_url}----------")
                except:
                    logging.info(f"---ERROR: Failed to establish connection---{package_name}----{information_url}")
                    result_info[package_name] = information_url

            else:
                information_url = None
                logging.info("--------No Information URL Found---------")
        
        return {"invalid_information_url": result_info}

    get_package_list = GetCkanPackageListOperator(
        task_id = "get_package_list", 
        address=CKAN, 
        apikey=CKAN_APIKEY,
    )
 
    information_url_checker = PythonOperator(
        task_id="information_url_checker",
        python_callable=information_url_checker,
        op_kwargs={"address": CKAN},
    )
    
    slack_notificaiton = GenericSlackOperator(
        task_id = "slack_notificaiton",
        message_header = "Information_URL checking",
        message_content_task_id = "information_url_checker",
        message_content_task_key = "invalid_information_url",
        message_body = "Information URL!"
    )
    
    get_package_list >> information_url_checker >> slack_notificaiton