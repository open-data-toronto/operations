from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime
from airflow.models import Variable
import pandas as pd
import ckanapi
import logging
import hashlib
from io import BytesIO
from pathlib import Path
from airflow import DAG
import requests
import json
import os
import sys

sys.path.append(Variable.get("repo_dir"))
from dags import utils as airflow_utils  # noqa: E402

job_settings = {
    "description": "Replace COVID 19 cases data in PROD with CSV file from TPH loaded in QA",
    "schedule": "59 14 * * 3",
    "start_date": datetime(2020, 11, 10, 13, 35, 0),
}

JOB_FILE = Path(os.path.abspath(__file__))
JOB_NAME = JOB_FILE.name[:-3]
PACKAGE_ID = JOB_NAME.replace("_", "-")

# ACTIVE_ENV = Variable.get("active_env")
ACTIVE_ENV = "dev"
CKAN_CREDS = Variable.get("ckan_credentials", deserialize_json=True)
TARGET_CKAN = ckanapi.RemoteCKAN(**CKAN_CREDS[ACTIVE_ENV])
SOURCE_CKAN = ckanapi.RemoteCKAN(**CKAN_CREDS["qa"])

SOURCE_FILE_NAME = "covid19cases.csv"


def send_success_msg(**kwargs):
    msg = kwargs.pop("ti").xcom_pull(task_ids="build_message")
    airflow_utils.message_slack(
        name=JOB_NAME,
        message_type="success",
        msg=msg,
    )


def send_failure_msg(self):
    airflow_utils.message_slack(
        name=JOB_NAME,
        message_type="error",
        msg="Job not finished",
    )


def backup_previous_data(**kwargs):
    ti = kwargs.pop("ti")
    package = ti.xcom_pull(task_ids="get_target_package")
    backups = Path(Variable.get("backups_dir")) / JOB_NAME

    resource = package["resources"][0]
    record_count = TARGET_CKAN.action.datastore_search(id=resource["id"], limit=0)[
        "total"
    ]

    datastore_response = TARGET_CKAN.action.datastore_search(
        id=resource["id"], limit=record_count
    )

    data = pd.DataFrame(datastore_response["records"])

    if "_id" in data.columns.values:
        data = data.drop("_id", axis=1)

    data_hash = hashlib.md5()
    data_hash.update(data.to_csv(index=False).encode("utf-8"))
    unique_id = data_hash.hexdigest()

    data_path = backups / f"data.{unique_id}.parquet"
    if not data_path.exists():
        data.to_parquet(data_path)

    fields = [f for f in datastore_response["fields"] if f["id"] != "_id"]

    fields_path = backups / f"fields.{unique_id}.json"
    if not fields_path.exists():
        with open(fields_path, "w") as f:
            json.dump(fields, f)

    return {
        "fields": fields_path,
        "data": data_path,
        "records": data.shape[0],
        "columns": data.shape[1],
        "unique_id": unique_id,
    }


def get_new_data(**kwargs):
    ti = kwargs.pop("ti")
    tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_data_dir"))
    package = SOURCE_CKAN.action.package_show(id=PACKAGE_ID)

    resource = [r for r in package["resources"] if r["name"] == SOURCE_FILE_NAME][0]

    file_content = requests.get(resource["url"]).content
    data = pd.read_csv(BytesIO(file_content))

    filename = "raw_data"
    filepath = tmp_dir / f"{filename}.parquet"

    data.to_parquet(filepath)

    return filepath


def get_target_package():
    package = TARGET_CKAN.action.package_show(id=PACKAGE_ID)

    datastore_resources = [
        r
        for r in package["resources"]
        if r["datastore_active"] or r["url_type"] == "datastore"
    ]

    assert (
        len(datastore_resources) == 1 and len(package["resources"]) == 1
    ), logging.error(
        "Expected 1 resource but got {} total and {} in the datastore".format(
            len(package["resources"]), len(datastore_resources)
        )
    )

    return package


def prep_data(**kwargs):
    ti = kwargs.pop("ti")
    tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_data_dir"))
    new_data_fp = Path(ti.xcom_pull(task_ids="get_new_data"))

    data = pd.read_parquet(new_data_fp)

    for date_field in ["Episode Date", "Reported Date"]:
        data[date_field] = (pd.to_datetime(data[date_field])).dt.strftime("%Y-%m-%d")

    filename = "ready_to_load"
    filepath = tmp_dir / f"{filename}.parquet"

    data.to_parquet(filepath)

    return filepath


def get_unique_id(**kwargs):
    ti = kwargs.pop("ti")
    data_fp = Path(ti.xcom_pull(task_ids="prep_data"))
    data = pd.read_parquet(data_fp).sort_values(by="Assigned_ID")

    data_hash = hashlib.md5()
    data_hash.update(data.to_csv(index=False).encode("utf-8"))

    return data_hash.hexdigest()


def confirm_data_is_new(**kwargs):
    ti = kwargs.pop("ti")
    data_to_load_unique_id = Path(ti.xcom_pull(task_ids="create_tmp_data_dir"))
    backups = Path(Variable.get("backups_dir")) / JOB_NAME

    data_files = []
    for f in os.listdir(backups):
        if os.path.isfile(backups / f):
            parts = f.split(".")
            unique_id = [
                p for p in parts if p not in ["parquet", "json", "fields", "data"]
            ][0]
            data_files.append(unique_id)

    unique_ids = list(set(data_files))

    if data_to_load_unique_id not in unique_ids:
        return "delete_old_records"

    logging.info(f"Data has already been loaded, ID: {data_to_load_unique_id}")

    return "build_loaded_message"


def delete_old_records(**kwargs):
    package = kwargs.pop("ti").xcom_pull(task_ids="get_target_package")
    resource = package["resources"][0]

    res = TARGET_CKAN.action.datastore_delete(id=resource["id"], filters={})

    return res


def insert_new_records(**kwargs):
    ti = kwargs.pop("ti")
    package = ti.xcom_pull(task_ids="get_target_package")
    resource = package["resources"][0]

    data_fp = Path(ti.xcom_pull(task_ids="prep_data"))
    data = pd.read_parquet(data_fp).sort_values(by="Assigned_ID")
    records = data.to_dict(orient="records")

    res = TARGET_CKAN.action.datastore_create(id=resource["id"], records=records)

    return res


def return_branch(**kwargs):
    ti = kwargs.pop("ti")
    resource = ti.xcom_pull(task_ids="read_staging_ckan")

    if resource is None:
        return "no_need_for_notification"

    return "read_new_data"


def build_message(**kwargs):
    ti = kwargs.pop("ti")
    backup_details = ti.xcom_pull(task_ids="backup_previous_data")
    unique_id = ti.xcom_pull(task_ids="get_unique_id")

    if "already_loaded" in kwargs:
        return "COVID data already loaded: {} records, and unique ID: {}".format(
            backup_details["records"], unique_id
        )

    previous_data_records = backup_details["records"]

    new_data_fp = ti.xcom_pull(task_ids="prep_data")
    new_data = pd.read_parquet(new_data_fp)

    return "COVID data refreshed: from {} to {} records".format(
        previous_data_records, new_data.shape[0]
    )


default_args = airflow_utils.get_default_args(
    {
        "on_failure_callback": send_failure_msg,
        "start_date": job_settings["start_date"],
    }
)

with DAG(
    JOB_NAME,
    default_args=default_args,
    description=job_settings["description"],
    schedule_interval=job_settings["schedule"],
) as dag:

    create_tmp_dir = PythonOperator(
        task_id="create_tmp_data_dir",
        python_callable=airflow_utils.create_dir_with_dag_name,
        op_kwargs={"dag_id": JOB_NAME, "dir_variable_name": "tmp_dir"},
    )

    create_backups_dir = PythonOperator(
        task_id="create_backups_dir",
        python_callable=airflow_utils.create_dir_with_dag_name,
        op_kwargs={"dag_id": JOB_NAME, "dir_variable_name": "backups_dir"},
    )

    source_data = PythonOperator(
        task_id="get_new_data",
        python_callable=get_new_data,
        provide_context=True,
    )

    target_package = PythonOperator(
        task_id="get_target_package",
        python_callable=get_target_package,
    )

    backup_previous = PythonOperator(
        task_id="backup_previous_data",
        python_callable=backup_previous_data,
        provide_context=True,
    )

    prepare_data = PythonOperator(
        task_id="prep_data",
        python_callable=prep_data,
        provide_context=True,
    )

    new_data_unique_id = PythonOperator(
        task_id="get_unique_id",
        python_callable=get_unique_id,
        provide_context=True,
    )

    data_is_new = BranchPythonOperator(
        task_id="confirm_data_is_new",
        python_callable=confirm_data_is_new,
        provide_context=True,
    )

    delete_old = PythonOperator(
        task_id="delete_old_records",
        python_callable=delete_old_records,
        provide_context=True,
    )

    insert_new = PythonOperator(
        task_id="insert_new_records",
        python_callable=insert_new_records,
        provide_context=True,
    )

    loaded_msg = PythonOperator(
        task_id="build_message",
        python_callable=build_message,
        provide_context=True,
    )

    nothing_to_load_msg = PythonOperator(
        task_id="build_loaded_message",
        python_callable=build_message,
        op_kwargs={"already_loaded": True},
    )

    send_notification = PythonOperator(
        task_id="send_success_msg",
        python_callable=send_success_msg,
        provide_context=True,
        trigger_rule="one_success",
    )

    create_tmp_dir >> source_data >> prepare_data >> new_data_unique_id >> data_is_new

    [data_is_new, backup_previous] >> delete_old >> insert_new >> loaded_msg

    loaded_msg >> send_notification

    data_is_new >> nothing_to_load_msg >> send_notification

    target_package >> create_backups_dir >> backup_previous

    create_backups_dir >> data_is_new
