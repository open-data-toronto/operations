"""
    This dag checks problematic inforamtion_url on a dataset page, send
    notification to slack on problematic urls
"""

import requests
import logging
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from ckan_operators.package_operator import GetCkanPackageListOperator
from utils_operators.slack_operators import (
    GenericSlackOperator,
    task_failure_slack_alert,
)

# init hardcoded vars for these dags
ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN = CKAN_CREDS[ACTIVE_ENV]["address"]
CKAN_APIKEY = CKAN_CREDS[ACTIVE_ENV]["apikey"]

# List of website we ignore for now,
# since they are accessible but probably not allowed api calls
white_list = ["www.ttc.ca", "www.ttc.ca/"]

with DAG(
    "information-url-checker",
    default_args={
        "owner": "Yanan",
        "depends_on_past": False,
        "email": "yanan.zhang@toronto.ca",
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": 3,
        "on_failure_callback": task_failure_slack_alert,
        "tags": ["sustainment"],
    },
    description=(
        "Check if information_url(Under dataset description section)"
        " accessible, send notification to slack on failure urls"
    ),
    schedule_interval="0 8 * * 0",
    start_date=datetime(2023, 1, 27, 0, 0, 0),
    catchup=False,
) as dag:

    def information_url_checker(**kwargs):
        ti = kwargs.pop("ti")
        package_list = ti.xcom_pull(task_ids="get_package_list")
        address = kwargs.pop("address")

        result_info = {}
        for package_name in package_list:
            url = address + "api/3/action/package_show?id=" + package_name
            response = requests.get(url).json()["result"]
            if "information_url" in response.keys():
                information_url = response["information_url"]
                if "http" in information_url:
                    call = information_url
                else:
                    call = "https://" + information_url
                logging.info(f"{call}")

                # record probametic url
                try:
                    response = requests.get(call)
                    status_code = response.status_code
                    reason = response.reason
                    logging.info(
                        (
                            f"---Status-{status_code}---Reason--{reason}--"
                            f"--{package_name}----{call}----------"
                        )
                    )
                    if 300 <= status_code < 400:
                        result_info[package_name] = (
                            "Redirected URL:  "
                            + str(status_code)
                            + " "
                            + information_url
                        )
                    elif 400 <= status_code < 500:
                        if status_code == 404:
                            result_info[package_name] = (
                                "Page Not Found: "
                                + str(status_code)
                                + " "
                                + information_url
                            )
                        elif call.split("://")[1] not in white_list:
                            result_info[package_name] = (
                                "Client Error: "
                                + str(status_code)
                                + " "
                                + information_url
                            )
                    elif status_code >= 500:
                        result_info[package_name] = "Server Error: "
                        +str(status_code) + "" + information_url

                except requests.ConnectionError:
                    logging.info(
                        (
                            "--ERROR: Failed to establish connection"
                            f"---{package_name}--{call}"
                        )
                    )
                    result_info[package_name] = (
                        "Hmmm… can't reach this page:  " + information_url
                    )

            else:
                information_url = None
                logging.info("--------No Information URL Found---------")

        return {"invalid_information_url": result_info}

    get_package_list = GetCkanPackageListOperator(
        task_id="get_package_list",
        address=CKAN,
        apikey=CKAN_APIKEY,
    )

    information_url_checker = PythonOperator(
        task_id="information_url_checker",
        python_callable=information_url_checker,
        op_kwargs={"address": CKAN},
    )

    slack_notificaiton = GenericSlackOperator(
        task_id="slack_notificaiton",
        message_header="Information_URL checking",
        message_content_task_id="information_url_checker",
        message_content_task_key="invalid_information_url",
        message_body="Information URL!",
    )

    get_package_list >> information_url_checker >> slack_notificaiton
