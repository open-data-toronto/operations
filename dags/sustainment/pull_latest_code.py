from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow import DAG
from datetime import datetime
from pathlib import Path
import logging
import sys
import os

repo_dir = Variable.get("repo_dir")

sys.path.append(repo_dir)
from utils import airflow as airflow_utils  # noqa: E402

job_settings = {
    "description": "Pulls latest code from GitHub repo. Updated dags must be deleted and restarted manually.",
    "start_date": datetime(2020, 11, 10, 0, 30, 0),
    "schedule": "@once",
}


def compare_changed_to_list(**kwargs):
    changes = kwargs.pop("ti").xcom_pull(task_ids="pull_repo")

    dags = kwargs.pop("ti").xcom_pull(task_ids="list_dags")

    logging.info(f"changes: {changes}")
    logging.info(f"list_dags: {list_dags}")


job_file = Path(os.path.abspath(__file__))
job_name = job_file.name[:-3]

default_args = airflow_utils.get_default_args(
    {
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

    pull_repo = BashOperator(
        task_id="pull_repo",
        bash_command=f"git -C {repo_dir} pull; echo $?",
        xcom_push=True,
    )

    list_dags = BashOperator(
        task_id="list_dags",
        bash_command="airflow list_dags; echo $?",
        xcom_push=True,
    )

    compare = PythonOperator(
        task_id="compare", provide_context=True, python_callable=compare_changed_to_list
    )

    pull_repo >> list_dags >> compare
