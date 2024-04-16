from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from utils import airflow_utils

with DAG(
    "pull_latest_code",
    default_args=airflow_utils.get_default_args(
        {"retries": 0, "start_date": datetime(2020, 11, 10, 0, 30, 0)}
    ),
    description="Pulls repo code.",
    schedule_interval="*/5 * * * *",
    tags=["sustainment"],
    catchup=False,
) as dag:

    pull_repo = BashOperator(
        task_id="pull_repo",
        bash_command=f"git config --global --add safe.directory /data/operations; git -C {Variable.get('repo_dir')} pull; echo $?",
    )

    pull_repo
