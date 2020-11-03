from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from pathlib import Path
from airflow import DAG
import jobs.run as job
import os

PATH = Path(os.path.abspath(__file__))
JOB = PATH.name[:-3]

default_args = {
    "owner": "Carlos",
    "depends_on_past": False,
    "start_date": days_ago(0),
    "email": ["carlos.hernandez@toronto.ca"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    JOB,
    default_args=default_args,
    description="Pipe to get datasets behind published refresh rate, and by how long",
    schedule_interval="0 11 * * 2,4",
)

run = PythonOperator(
    task_id="run",
    provide_context=True,
    op_kwargs={"args_list": ["--job", JOB]},
    python_callable=job.run,
    dag=dag,
)
