from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from pathlib import Path
from airflow import DAG
import logging
import ckanapi
import os

from utils import airflow_utils
from utils import ckan_utils

from utils_operators.slack_operators import (
    GenericSlackOperator,
    task_failure_slack_alert,
)

job_settings = {
    "description": "Gets datasets behind expected refresh date",
    "schedule": "30 04 * * 3",
    "start_date": datetime(2020, 11, 9, 0, 30, 0),
}

job_file = Path(os.path.abspath(__file__))
job_name = job_file.name[:-3]

ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN = ckanapi.RemoteCKAN(**CKAN_CREDS[ACTIVE_ENV])

TIME_MAP = {
    "daily": 1,
    "weekly": 7,
    "monthly": 30,
    "quarterly": 52 * 7 / 4,
    "semi-annually": 52 * 7 / 2,
    "annually": 365,
}

def parse_datetime(input):
    for format in [
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d"
    ]:
        try:
            return datetime.strptime(input, format)

        except ValueError:
            pass

def get_packages_behind(**kwargs):
    packages = kwargs.pop("ti").xcom_pull(task_ids="get_packages")
    run_time = datetime.utcnow()

    packages_behind = []

    logging.info("Looping through packages")
    for p in packages:
        refresh_rate = p.pop("refresh_rate").lower()

        if (refresh_rate not in TIME_MAP.keys() or 
                p.get("is_retired", True) in ["true", "True", True]):
            continue

        last = p.pop("last_refreshed")
        last = parse_datetime(last)
        logging.info(last)

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
            logging.info(row)
            packages_behind.append(row)
            logging.info("{name}: Behind {days} days. {email}".format(**row))

    # sort list of dicts by days behind
    sortedlist = sorted(packages_behind, key=lambda d: d["days"], reverse=True)

    output = sortedlist[:5]

    return output


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
        "msg": "\n" + "\n".join(lines),
    }


default_args = airflow_utils.get_default_args(
    {
        "on_failure_callback": task_failure_slack_alert, 
        "start_date": job_settings["start_date"]}
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
        task_id="get_packages",
        python_callable=ckan_utils.get_all_packages,
        op_args=[CKAN],
    )

    get_packages_behind = PythonOperator(
        task_id="get_packages_behind",
        provide_context=True,
        python_callable=get_packages_behind,
    )

    build_message = PythonOperator(
        task_id="build_message",
        provide_context=True,
        python_callable=build_notification_message,
    )

    send_notification = GenericSlackOperator(
        task_id="send_notification",
        message_header="Top 5 Stalest Datasets",
        message_content_task_id="build_message",
        message_content_task_key="msg",
        message_body="",
    )

    packages >> get_packages_behind >> build_message >> send_notification
