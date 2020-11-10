from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.models import Variable
from pathlib import Path
from airflow import DAG
import logging
import ckanapi
import sys
import os

sys.path.append(Variable.get("repo_dir"))
from dags import utils as airflow_utils  # noqa: E402
import jobs.utils.common as common_utils  # noqa: E402

job_settings = {
    "description": "Gets datasets behind expected refresh date",
    "schedule": "0 16 * * 3",
    "start_date": days_ago(0),
}

job_file = Path(os.path.abspath(__file__))
job_name = job_file.name[:-3]

ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials", deserialize_json=True)
CKAN = ckanapi.RemoteCKAN(**CKAN_CREDS[ACTIVE_ENV])

TIME_MAP = {
    "daily": 1,
    "weekly": 7,
    "monthly": 30,
    "quarterly": 52 * 7 / 4,
    "semi-annually": 52 * 7 / 2,
    "annually": 365,
}


def send_success_msg(**kwargs):
    msg = kwargs.pop("ti").xcom_pull(task_ids="build_message")
    airflow_utils.message_slack(name=job_name, **msg)


def send_failure_msg(self):
    airflow_utils.message_slack(
        name=job_name,
        message_type="error",
        msg="Job not finished",
    )


def get_packages_behind_refresh(**kwargs):
    packages = kwargs.pop("ti").xcom_pull(task_ids="get_packages")
    run_time = datetime.utcnow()

    packages_behind = []

    logging.info("Looping through packages")
    for p in packages:
        refresh_rate = p.pop("refresh_rate").lower()

        if refresh_rate not in TIME_MAP.keys() or p["is_retired"]:
            continue

        last = p.pop("last_refreshed")
        last = datetime.strptime(last, "%Y-%m-%dT%H:%M:%S.%f")

        days_behind = (run_time - last).days
        next_refreshed = last + timedelta(days=TIME_MAP[refresh_rate])

        if days_behind > TIME_MAP[refresh_rate]:
            row = {
                "name": p["name"],
                "email": p["owner_email"],
                "publisher": p["owner_division"],
                "rate": refresh_rate,
                "last": last.strftime("%Y-%m-%d"),
                "next": next_refreshed.strftime("%Y-%m-%d"),
                "days": days_behind,
            }
            packages_behind.append(row)
            logging.info("{name}: Behind {days} days. {email}".format(**row))

    return packages_behind


def build_notification_message(**kwargs):
    ti = kwargs.pop("ti")
    packages_behind = ti.xcom_pull(task_ids="get_packages_behind")

    lines = [
        "_{name}_ ({rate}): `{last}`. Behind {days} days. {email}".format(**p)
        for p in sorted(packages_behind, key=lambda x: -x["days"])
    ]

    lines = [f"{i+1}. {l}" for i, l in enumerate(lines)]

    return {
        "message_type": "success",
        "msg": "\n".join(lines),
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
    tags=["sustainment"],
) as dag:

    packages = PythonOperator(
        task_id="get_packages",
        python_callable=common_utils.get_all_packages,
        op_args=[CKAN],
    )

    get_packages_behind = PythonOperator(
        task_id="get_packages_behind",
        provide_context=True,
        python_callable=get_packages_behind_refresh,
    )

    build_message = PythonOperator(
        task_id="build_message",
        provide_context=True,
        python_callable=build_notification_message,
    )

    send_notification = PythonOperator(
        task_id="send_notification",
        provide_context=True,
        python_callable=send_success_msg,
    )

    packages >> get_packages_behind >> build_message >> send_notification
