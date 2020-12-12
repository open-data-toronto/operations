from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from datetime import datetime
from airflow import DAG
import ckanapi
from pathlib import Path
import os
import sys
import logging

sys.path.append(Variable.get("repo_dir"))
from utils import airflow as airflow_utils  # noqa: E402
from utils import ckan as ckan_utils  # noqa: E402

job_settings = {
    "description": "Identifies empty datastore resources and send to Slack",
    "schedule": "5 15,18,21,0,3 * * *",
    "start_date": datetime(2020, 11, 9, 0, 30, 0),
}

job_file = Path(os.path.abspath(__file__))
job_name = job_file.name[:-3]

ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN = ckanapi.RemoteCKAN(**CKAN_CREDS[ACTIVE_ENV])


def send_success_msg(**kwargs):
    msg = kwargs.pop("ti").xcom_pull(task_ids="build_message")
    airflow_utils.message_slack(name=job_name, **msg)


def send_failure_msg(self):
    airflow_utils.message_slack(
        name=job_name,
        message_type="error",
        msg="Job not finished",
    )


def get_record_counts(**kwargs):
    packages = kwargs.pop("ti").xcom_pull(task_ids="get_all_packages")
    datastore_resources = []

    for package in packages:
        for resource in package["resources"]:
            if resource["url_type"] != "datastore":
                continue
            response = CKAN.action.datastore_search(id=resource["id"], limit=0)

            datastore_resources.append(
                {
                    "package_id": package["title"],
                    "resource_id": resource["id"],
                    "resource_name": resource["name"],
                    "extract_job": resource["extract_job"],
                    "row_count": response["total"],
                    "fields": response["fields"],
                }
            )

            logging.info(
                f'{package["name"]}: {resource["name"]} - {response["total"]} records'
            )

    logging.info(f"Identified {len(datastore_resources)} datastore resources")

    return datastore_resources


def filter_empty_resources(**kwargs):
    datastore_resources = kwargs.pop("ti").xcom_pull(task_ids="get_record_counts")

    empties = [r for r in datastore_resources if r["row_count"] == 0]

    logging.info(f"Identified {len(empties)} empty datastore resources")

    return empties


def build_message(**kwargs):
    def pprint_2d_list(matrix):
        s = [[str(e) for e in row] for row in matrix]
        lens = [max(map(len, col)) for col in zip(*s)]
        fmt = "\t".join("{{:{}}}".format(x) for x in lens)
        table = [fmt.format(*row) for row in s]
        return "\n".join(table)

    empties = kwargs.pop("ti").xcom_pull(task_ids="filter_empty_resources")

    empties = sorted(empties, key=lambda i: i["package_id"])

    matrix = [["#", "PACKAGE", "EXTRACT_JOB", "ROW_COUNT"]]

    for i, r in enumerate(empties):
        string = [f"{i+1}."]
        string.extend(
            [r[f] for f in ["package_id", "extract_job", "row_count"] if r[f]]
        )
        matrix.append(string)

    empties = pprint_2d_list(matrix)

    logging.warning(f"Empty resources found:\n```\n{empties}\n```")

    return {
        "message_type": "warning",
        "msg": f"""```{empties}```""",
    }


def are_there_empties(**kwargs):
    empties = kwargs.pop("ti").xcom_pull(task_ids="filter_empty_resources")

    if len(empties) == 0:
        return "no_notification"

    return "build_message"


default_args = airflow_utils.get_default_args(
    {
        "on_failure_callback": send_failure_msg,
        "start_date": job_settings["start_date"],
    }
)


with DAG(
    job_name,
    default_args=default_args,
    description=job_settings["description"],
    schedule_interval=job_settings["schedule"],
    tags=["sustainment"],
    catchup=False,
) as dag:

    packages = PythonOperator(
        task_id="get_all_packages",
        python_callable=ckan_utils.get_all_packages,
        op_args=[CKAN],
    )

    record_counts = PythonOperator(
        task_id="get_record_counts",
        provide_context=True,
        python_callable=get_record_counts,
    )

    empties = PythonOperator(
        task_id="filter_empty_resources",
        provide_context=True,
        python_callable=filter_empty_resources,
    )

    empties_branch = BranchPythonOperator(
        task_id="are_there_empties",
        python_callable=are_there_empties,
        provide_context=True,
    )

    no_notification = DummyOperator(
        task_id="no_notification",
    )

    message = PythonOperator(
        python_callable=build_message,
        task_id="build_message",
        provide_context=True,
    )

    send_notification = PythonOperator(
        task_id="send_notification",
        provide_context=True,
        python_callable=send_success_msg,
    )

    packages >> record_counts >> empties >> empties_branch

    empties_branch >> no_notification

    empties_branch >> message >> send_notification
