from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from airflow.models import Variable
from airflow import DAG
import ckanapi
import logging
from pathlib import Path
import yaml
import os
import sys
from dateutil import parser
import requests
import traceback

sys.path.append(Variable.get("repo_dir"))
from utils import airflow as airflow_utils  # noqa: E402
from utils import ckan as ckan_utils  # noqa: E402

job_settings = {
    "description": "Syncs last modified times between CKAN and files in GCC server",
    "schedule": "@hourly",
    "start_date": datetime(2020, 11, 10, 0, 30, 0),
}

job_file = Path(os.path.abspath(__file__))
job_name = job_file.name[:-3]

active_env = Variable.get("active_env")
ckan_creds = Variable.get("ckan_credentials", deserialize_json=True)
ckan = ckanapi.RemoteCKAN(**ckan_creds[active_env])


def send_success_msg(**kwargs):
    msg = kwargs.pop("ti").xcom_pull(task_ids="build_message")
    airflow_utils.message_slack(
        name=job_name,
        message_type="success",
        msg=msg,
    )


def send_failure_msg(self):
    airflow_utils.message_slack(
        name=job_name,
        message_type="error",
        msg="Job not finished",
    )


def load_remote_files():
    with open(job_file.parent / "config.yaml", "r") as f:
        config = yaml.load(f, yaml.SafeLoader)

    return config


def get_packages_to_sync(**kwargs):
    remote_files = kwargs.pop("ti").xcom_pull(task_ids="load_files")

    all_packages = ckan.action.package_search(rows=10000)["results"]
    to_sync = list(remote_files.keys())

    packages = [p for p in all_packages if p["name"] in to_sync]

    return packages


def sync_resource_timestamps(**kwargs):
    ti = kwargs.pop("ti")
    remote_files = ti.xcom_pull(task_ids="load_files")
    packages = ti.xcom_pull(task_ids="get_packages")

    def sync(package, remote_files):
        files = remote_files[package["name"]]
        resources = package["resources"]

        package_sync_results = []
        for f in files:
            logging.info(f"Attempting: {f}")
            resources_with_url = [r for r in resources if r["url"] == f]

            assert len(resources_with_url) == 1, logging.error(
                f"{package['name']}: No resource for file: {f}"
            )

            resource = resources_with_url[0]

            resource_last_modified = resource["last_modified"]
            if not resource["last_modified"]:
                resource_last_modified = resource["created"]
                logging.info(f"{resource['id']}: No last_modified, using created.")

            resource_last_modified = parser.parse(f"{resource_last_modified} UTC")

            res = requests.head(f)
            file_last_modified = parser.parse(res.headers["Last-Modified"])

            difference_in_seconds = (
                file_last_modified.timestamp() - resource_last_modified.timestamp()
            )

            record = {
                "package_name": package["name"],
                "resource_name": resource["name"],
                "resource_id": resource["id"],
                "file": f,
                "file_last_modified": file_last_modified.strftime("%Y-%m-%dT%H:%M:%S"),
            }

            if difference_in_seconds == 0:
                logging.info(f"Up to date: {resource['id']} | {f}")
                package_sync_results.append({**record, "result": "unchanged"})
                continue

            resp = ckan_utils.update_resource_last_modified(
                ckan=ckan,
                resource_id=resource["id"],
                new_last_modified=file_last_modified,
            )

            logging.info(
                f'{resource["id"]}: Last modified set to {resp["last_modified"]}'
            )

            package_sync_results.append({**record, "result": "synced"})

        return package_sync_results

    sync_results = []
    for package in packages:
        name = package["name"]
        logging.info(name)

        try:
            results = sync(package, remote_files)

        except Exception:
            logging.error(f"{name}:\n{traceback.format_exc()}")
            results = [{"package_name": name, "result": "error"}]

        sync_results.extend(results)

    return sync_results


def build_notification_message(**kwargs):
    sync_results = kwargs.pop("ti").xcom_pull(task_ids="sync_timestamps")
    message_lines = [""]

    for result_type in ["synced", "error", "unchanged"]:
        result_type_resources = [r for r in sync_results if r["result"] == result_type]

        if len(result_type_resources) == 0:
            continue

        lines = []

        for index, r in enumerate(result_type_resources):
            if result_type == "unchanged":
                continue

            lines.append(result_type)

            if result_type == "error":
                lines.append("{}. {} ".format(index + 1, r["package_name"]))
                continue

            lines.append(
                "{}: `{}`".format(
                    r["resource_name"],
                    r["file_last_modified"],
                )
            )

        message_lines.extend(lines)

    notification_msg = "\n".join(message_lines)

    if all(["error" not in notification_msg, "synced" not in notification_msg]):
        return None

    return "\n".join(message_lines)


def return_branch(**kwargs):
    msg = kwargs.pop("ti").xcom_pull(task_ids="build_message")

    if msg is None:
        return "no_need_for_notification"

    return "send_notification"


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

    load_files = PythonOperator(
        task_id="load_files",
        python_callable=load_remote_files,
    )

    get_packages = PythonOperator(
        task_id="get_packages",
        provide_context=True,
        python_callable=get_packages_to_sync,
    )

    sync_timestamps = PythonOperator(
        task_id="sync_timestamps",
        provide_context=True,
        python_callable=sync_resource_timestamps,
    )

    build_message = PythonOperator(
        task_id="build_message",
        provide_context=True,
        python_callable=build_notification_message,
    )

    branching = BranchPythonOperator(
        task_id="branching",
        provide_context=True,
        python_callable=return_branch,
    )

    send_notification = PythonOperator(
        task_id="send_notification",
        provide_context=True,
        python_callable=send_success_msg,
    )

    no_notification = DummyOperator(task_id="no_need_for_notification")

    load_files >> get_packages >> sync_timestamps >> build_message >> branching
    branching >> send_notification
    branching >> no_notification
