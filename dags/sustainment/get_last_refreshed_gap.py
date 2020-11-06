from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow import DAG
from pathlib import Path
import ckanapi
import sys
import os

sys.path.append(Variable.get("repo_dir"))
import dags.utils as airflow_utils  # noqa: E402
import jobs.utils.common as common_utils  # noqa: E402

job_settings = {
    "description": "Gets datasets behind published refresh rate and by how long",
    "schedule": "0 16 * * 2,4",
    "start_date": days_ago(1),
}

job_file = Path(os.path.abspath(__file__))
job_name = job_file.name[:-3]

active_env = Variable.get("active_env")
ckan_creds = Variable.get("ckan_credentials", deserialize_json=True)
ckan = ckanapi.RemoteCKAN(**ckan_creds[active_env])


def send_success_msg(**kwargs):
    msg = kwargs.pop("ti").xcom_pull(task_ids="build_message")
    airflow_utils.message_slack(name=job_name, ckan_url=ckan.address, **msg)


def send_failure_msg():
    airflow_utils.message_slack(
        name=job_name,
        ckan_url=ckan.address,
        message_type="error",
        msg="",
    )


def get_package_list():
    return common_utils.get_all_packages(ckan)


def get_packages_behind_refresh(**kwargs):
    packages = kwargs.pop("ti").xcom_pull(task_ids="get_packages")
    run_time = datetime.utcnow()

    packages_behind = []

    logging.info("Looping through packages")
    for p in packages:
        refresh_rate = p.pop("refresh_rate").lower()

        if refresh_rate not in TIME_MAP.keys() or p["is_retired"]:
            continue

        last_refreshed = p.pop("last_refreshed")
        last_refreshed = datetime.strptime(last_refreshed, "%Y-%m-%dT%H:%M:%S.%f")

        days_behind = (run_time - last_refreshed).days
        next_refreshed = last_refreshed + timedelta(days=TIME_MAP[refresh_rate])

        if days_behind > TIME_MAP[refresh_rate]:
            details = {
                "name": p["name"],
                "email": p["owner_email"],
                "publisher": p["owner_division"],
                "rate": refresh_rate,
                "last": last_refreshed.strftime("%Y-%m-%d"),
                "next": next_refreshed.strftime("%Y-%m-%d"),
                "days": days_behind,
            }
            packages_behind.append(details)
            logging.info(
                f"{details['name']}: Behind {details['days']} days. {details['email']}"
            )

    return packages_behind


def build_notification_message(**kwargs):
    packages_behind = kwargs.pop("ti").xcom_pull(task_ids="get_packages_behind")

    lines = [
        "_{name}_ ({rate}): refreshed `{last}`. Behind *{days} days*. {email}".format(
            **p
        )
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
) as dag:

    get_packages = PythonOperator(
        task_id="get_packages",
        python_callable=get_package_list,
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

    get_packages >> get_packages_behind >> build_message >> send_notification
