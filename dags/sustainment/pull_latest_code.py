from airflow.operators.bash import BashOperator

from airflow.models import Variable
from airflow import DAG
from datetime import datetime
from pathlib import Path
import os

from utils import airflow_utils

job_settings = {
    "description": "Pulls repo code. Updated dags must be deleted and restarted.",
    "start_date": datetime(2020, 11, 10, 0, 30, 0),
    "schedule": "@once",
}


job_file = Path(os.path.abspath(__file__))
job_name = job_file.name[:-3]

default_args = airflow_utils.get_default_args(
    {"retries": 0, "start_date": job_settings["start_date"],}
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
        bash_command=f"git -C {Variable.get('repo_dir')} pull; echo $?",
    )

    pull_repo
