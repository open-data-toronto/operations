from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow import DAG
import ckanapi
from pathlib import Path
import os
import sys
import logging

sys.path.append(Variable.get("repo_dir"))
from dags import utils as airflow_utils  # noqa: E402
from jobs.utils import common as common_utils  # noqa: E402

job_settings = {
    "description": "Identifies empty datastore resources and send to Slack",
    "schedule": "15 15,23 * * *",
    "start_date": days_ago(1),
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
        ckan_url=ckan.address,
        message_type="success",
        msg=msg,
    )


def send_failure_msg():
    airflow_utils.message_slack(
        name=job_name,
        ckan_url=ckan.address,
        message_type="error",
        msg="",
    )


def pprint_2d_list(matrix):
    s = [[str(e) for e in row] for row in matrix]
    lens = [max(map(len, col)) for col in zip(*s)]
    fmt = "\t".join("{{:{}}}".format(x) for x in lens)
    table = [fmt.format(*row) for row in s]
    return "\n".join(table)


def run():
    packages = common_utils.get_all_packages(ckan)
    logging.info(f"Retrieved {len(packages)} packages")
    datastore_resources = []

    logging.info("Identifying datastore resources")
    for package in packages:
        for resource in package["resources"]:
            if resource["url_type"] != "datastore":
                continue
            response = ckan.action.datastore_search(id=resource["id"], limit=0)

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

    empties = [r for r in datastore_resources if r["row_count"] == 0]

    if not len(empties):
        logging.info("No empty resources found")
        return {"message_type": "success", "msg": "No empties"}

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
) as dag:
    run_job = PythonOperator(
        task_id="run_job",
        python_callable=run,
    )

    send_notification = PythonOperator(
        task_id="send_notification",
        provide_context=True,
        python_callable=send_success_msg,
    )

    run_job >> send_notification
