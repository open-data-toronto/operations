# tasks_log_monitor.py
"""
This DAG ensures that there are only a set number of days worth of airflow logs stored in our EFS Volume
It deletes logs that are too old
"""
 
import os
import shutil
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator

from utils_operators.directory_operator import CreateLocalDirectoryOperator, DeleteLocalDirectoryOperator
from utils_operators.slack_operators import task_success_slack_alert, task_failure_slack_alert, GenericSlackOperator

from utils import airflow_utils

DEFAULT_ARGS = airflow_utils.get_default_args(
    {
        "owner": "Mackenzie",
        "depends_on_past": False,
        "email": ["mackenzie.nichols4@toronto.ca"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "on_failure_callback": task_failure_slack_alert,
        "retries": 0,
        "start_date": datetime(2021, 8, 19, 0, 0, 0)

    })
DESCRIPTION = "Ensures only a set amount of days of airflow task logs are stored on our server, so we dont fill the server disk up"
SCHEDULE = "0 0 * * *" # Every day at midnight
TAGS=["sustainment"]

logs_basedir = "/data/logs/scheduler/"

## This is the number of days of logs we'll keep - anything outside this range will be deleted
days_allowed = 30


today = datetime.today()
earliest_allowable_date = today - timedelta( days = days_allowed )

# return folder names in list are >30 days old
def find_logs_to_delete(**kwargs):
    all_folders = kwargs.pop("ti").xcom_pull("get_folder_names").split(" ")
    folders_to_delete = []
    for folder in all_folders:
        if folder == "latest":
            continue
        if datetime.strptime(folder, "%Y-%m-%d") < earliest_allowable_date:
            folders_to_delete.append( folder )
    
    return {"folders": folders_to_delete}

# delete folders from previous task
def delete_logs(**kwargs):
    folders_to_delete = kwargs.pop("ti").xcom_pull("find_logs_to_delete")["folders"]
    if len(folders_to_delete) > 0:
        for folder in folders_to_delete:
            shutil.rmtree( logs_basedir + folder)
            logging.info("Deleted: " + logs_basedir + folder)
        return {"folders": folders_to_delete}
    else:
        logging.info("No logs to delete!")
        return {"folders": ["Nothing"]}

with DAG(
    "tasks_log_monitor",
    description = DESCRIPTION,
    default_args = DEFAULT_ARGS,
    schedule_interval = SCHEDULE,
    tags=TAGS
) as dag:

    # get folder names
    get_folder_names = BashOperator(
        task_id = "get_folder_names",
        bash_command = "ls " + logs_basedir + " | tr '\n' ' ' "
    )

    find_logs_to_delete = PythonOperator(
        task_id = "find_logs_to_delete",
        python_callable = find_logs_to_delete,
        provide_context = True
    )

    delete_logs = PythonOperator(
        task_id = "delete_logs",
        python_callable = delete_logs,
        provide_context = True
    )

    message_slack = GenericSlackOperator(
        task_id = "message_slack",
        message_header = "Airflow Logs Monitoring",
        message_content_task_id = "delete_logs",
        message_content_task_key = "folders",
        message_body = "deleted"

    )


    # if 30 days - today is later than name then DELETE (for each name in folder names)

    get_folder_names >> find_logs_to_delete >> delete_logs >> message_slack