from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from pathlib import Path
from airflow import DAG
import os
import sys

sys.path.append("/data/operations")
import jobs.run as job

PATH = Path(os.path.abspath(__file__))
JOB = PATH.name[:-3]

default_args = {
    "owner": "Carlos",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "email": ["carlos.hernandez@toronto.ca"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

dag = DAG(
    JOB,
    default_args=default_args,
    description="Pipe to identify empty datastore resources and send them to Slack",
    schedule_interval=timedelta(minutes=10),
)

run = PythonOperator(
    task_id="run",
    provide_context=True,
    op_kwargs={"args_list": ["--job", JOB]},
    python_callable=job.run,
    dag=dag,
)
