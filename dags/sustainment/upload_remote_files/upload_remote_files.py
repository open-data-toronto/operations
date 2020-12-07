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
import json
from urllib.parse import urljoin

sys.path.append(Variable.get("repo_dir"))
from utils import airflow as airflow_utils  # noqa: E402
from utils import ckan as ckan_utils  # noqa: E402

job_settings = {
    "description": "Uploads files from opendata.toronto.ca to respective CKAN resource",
    "schedule": "0,30 * * * *",
    "start_date": datetime(2020, 11, 10, 0, 30, 0),
}

job_file = Path(os.path.abspath(__file__))
job_name = job_file.name[:-3]

active_env = Variable.get("active_env")
ckan_creds = Variable.get("ckan_credentials_secret", deserialize_json=True)
ckan = ckanapi.RemoteCKAN(**ckan_creds[active_env])


def send_success_msg(**kwargs):
    msg = kwargs.pop("ti").xcom_pull(task_ids="build_message")
    airflow_utils.message_slack(
        name=job_name,
        message_type="success",
        msg=msg,
        prod_webhook=active_env == "prod",
        active_env=active_env,
    )


def send_failure_msg(self):
    airflow_utils.message_slack(
        name=job_name,
        message_type="error",
        msg="Job not finished",
        prod_webhook=active_env == "prod",
        active_env=active_env,
    )


def load_remote_files():
    with open(job_file.parent / "config.yaml", "r") as f:
        config = yaml.load(f, yaml.SafeLoader)

    return config


def get_packages_to_sync(**kwargs):
    remote_files = kwargs.pop("ti").xcom_pull(task_ids="load_file_list")

    all_packages = ckan.action.package_search(rows=10000)["results"]
    to_sync = list(remote_files.keys())

    packages = [p for p in all_packages if p["name"] in to_sync]

    return packages


def upload_remote_files(**kwargs):
    ti = kwargs.pop("ti")
    remote_files = ti.xcom_pull(task_ids="load_file_list")
    packages = ti.xcom_pull(task_ids="get_packages")

    def upload(package, remote_files):
        files = remote_files[package["name"]]
        resources = package["resources"]

        package_upload_results = []
        for name, details in files.items():
            logging.info(f"Attempting: {name} | {details['url']}")
            resource = [r for r in resources if r["name"] == name]

            assert (
                len(resource) <= 1
            ), f"Expected 0 or 1 resource named {name}. Found {len(resource)}."

            should_upload = False

            headers = requests.head(details["url"]).headers
            file_last_modified = parser.parse(headers["Last-Modified"])

            if len(resource) == 0:
                metadata = {
                    "package_id": package["id"],
                    "name": name,
                    "format": details["format"],
                    "is_preview": False,
                    "extract_job": f"Airflow: {kwargs['dag'].dag_id}",
                }

                api_func = "resource_create"
                should_upload = True

            else:
                r = resource[0]
                metadata = {"id": r["id"]}
                api_func = "resource_patch"

                if r["last_modified"]:
                    resource_last_modified = parser.parse(r["last_modified"] + " UTC")
                else:
                    logging.info(f"{r['name']}: No last_modified, using created.")
                    resource_last_modified = parser.parse(r["created"] + " UTC")

                difference_in_seconds = (
                    file_last_modified.timestamp() - resource_last_modified.timestamp()
                )

                if difference_in_seconds == 0:
                    logging.info(f"{r['name']}: up to date, nothing to upload")
                    should_upload = False

                else:
                    should_upload = True

            if should_upload:
                assert (
                    "Last-Modified" in headers
                ), f"No Last-Modified in headers. URL may be broken and was directed? {json.dumps(dict(headers))}"

                res = requests.post(
                    urljoin(ckan.address, f"api/3/action/{api_func}"),
                    data=metadata,
                    headers={"Authorization": ckan.apikey},
                    files={
                        "upload": (
                            Path(details["url"]).name,
                            requests.get(details["url"]).content,
                        )
                    },
                ).json()["result"]

                ckan_utils.update_resource_last_modified(
                    ckan=ckan,
                    resource_id=res["id"],
                    new_last_modified=file_last_modified,
                )

                record = {
                    "package_name": package["name"],
                    "resource_name": res["name"],
                    "resource_id": res["id"],
                    "file": details["url"],
                    "last_modified": file_last_modified.strftime("%Y-%m-%d %H:%M"),
                    "size_mb": round(
                        float(headers["Content-Length"]) / (1024 * 1024), 1
                    ),
                }

                package_upload_results.append({**record, "result": "uploaded"})

        return package_upload_results

    upload_results = []
    for package in packages:
        name = package["name"]
        logging.info(name)

        try:
            results = upload(package, remote_files)

        except Exception:
            logging.error(f"{name}:\n{traceback.format_exc()}")
            results = [{"package_name": name, "result": "error"}]

        upload_results.extend(results)

    return upload_results


def build_notification_message(**kwargs):
    upload_results = kwargs.pop("ti").xcom_pull(task_ids="upload_files")
    message_lines = [""]

    for result_type in ["uploaded", "error"]:
        result_type_resources = [
            r for r in upload_results if r["result"] == result_type
        ]

        if len(result_type_resources) == 0:
            continue

        lines = [f"*{result_type}*"]

        for index, r in enumerate(result_type_resources):
            if result_type == "error":
                lines.append("{}. {} ".format(index + 1, r["package_name"]))
                continue

            lines.append(
                "{}: `{}` | {} MB".format(
                    r["resource_name"],
                    " ".join(r["last_modified"].split("T")),
                    r["size_mb"],
                )
            )

        message_lines.extend(lines)

    notification_msg = "\n".join(message_lines)

    if all(["error" not in notification_msg, "uploaded" not in notification_msg]):
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
        task_id="load_file_list",
        python_callable=load_remote_files,
    )

    get_packages = PythonOperator(
        task_id="get_packages",
        provide_context=True,
        python_callable=get_packages_to_sync,
    )

    upload_files = PythonOperator(
        task_id="upload_files",
        provide_context=True,
        python_callable=upload_remote_files,
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

    load_files >> get_packages >> upload_files >> build_message >> branching
    branching >> send_notification
    branching >> no_notification
