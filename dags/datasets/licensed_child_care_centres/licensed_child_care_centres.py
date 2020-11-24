from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
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
from dateutil import parser

sys.path.append(Variable.get("repo_dir"))
from utils import airflow as airflow_utils  # noqa: E402
from utils import ckan as ckan_utils  # noqa: E402

job_settings = {
    "description": "Take data from opendata.toronto.ca (CSV) and put into datastore",
    "schedule": "@once",
    "start_date": datetime(2020, 11, 24, 13, 35, 0),
}

JOB_FILE = Path(os.path.abspath(__file__))
JOB_NAME = JOB_FILE.name[:-3]
PACKAGE_ID = JOB_NAME.replace("_", "-")

# ACTIVE_ENV = Variable.get("active_env")
ACTIVE_ENV = "dev"
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN = ckanapi.RemoteCKAN(**CKAN_CREDS[ACTIVE_ENV])
SOURCE_CSV = "http://opendata.toronto.ca/childrens.services/licensed-child-care-centres/child-care.csv"  # noqa: E501
RESOURCE_NAME = "Child care centres"


def send_success_msg(**kwargs):
    ti = kwargs.pop("ti")
    msg_task_id = ti.xcom_pull(task_ids="msg_task_id")
    msg = ti.xcom_pull(task_ids=msg_task_id)

    airflow_utils.message_slack(
        name=JOB_NAME,
        message_type="success",
        msg=msg,
        prod_webhook=ACTIVE_ENV == "prod",
        active_env=ACTIVE_ENV,
    )


def send_failure_msg(self):
    airflow_utils.message_slack(
        name=JOB_NAME,
        message_type="error",
        msg="Job not finished",
        active_env=ACTIVE_ENV,
        prod_webhook=ACTIVE_ENV == "prod",
    )


def get_file_last_modified():
    return requests.head(SOURCE_CSV).headers["Last-Modified"]


def get_data_file(**kwargs):
    ti = kwargs.pop("ti")
    tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_dir"))

    file_content = requests.get(SOURCE_CSV).content
    data = pd.read_csv(BytesIO(file_content), encoding="latin1")

    filename = "new_data_raw"
    filepath = tmp_dir / f"{filename}.parquet"

    data.to_parquet(filepath)

    return filepath


def get_package():
    return CKAN.action.package_show(id=PACKAGE_ID)


def is_resource_new(**kwargs):
    ti = kwargs.pop("ti")
    package = ti.xcom_pull(task_ids="get_package")

    logging.info(f"resources found: {[r['name'] for r in package['resources']]}")

    is_new = RESOURCE_NAME not in [r["name"] for r in package["resources"]]

    if is_new:
        return "resource_is_new"

    return "resource_is_not_new"


def get_resource_id():
    package = ckan_utils.get_package(ckan=CKAN, package_id=PACKAGE_ID)
    resources = package["resources"]

    resource = [r for r in resources if r["name"] == RESOURCE_NAME][0]

    assert (
        resource["datastore_active"] is True
    ), f"Resource {RESOURCE_NAME} found but not in datastore"

    return resource["id"]


def backup_old_data(**kwargs):
    ti = kwargs.pop("ti")
    package = ti.xcom_pull(task_ids="get_package")
    backups = Path(Variable.get("backups_dir")) / JOB_NAME

    resource_id = [r for r in package["resources"] if r["name"] == RESOURCE_NAME][0][
        "id"
    ]
    logging.info(f"Resource ID: {resource_id}")

    record_count = CKAN.action.datastore_search(id=resource_id, limit=0)["total"]

    datastore_response = CKAN.action.datastore_search(
        id=resource_id, limit=record_count
    )
    records = datastore_response["records"]
    logging.info(f"Example record retrieved: {json.dumps(records[0])}")

    data = pd.DataFrame(records)
    logging.info(f"Columns: {data.columns.values}")

    if "_id" in data.columns.values:
        data = data.drop("_id", axis=1)

    data_hash = hashlib.md5()
    data_hash.update(data.sort_values(by="LOC_ID").to_csv(index=False).encode("utf-8"))
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
    }


def get_new_data_unique_id(**kwargs):
    ti = kwargs.pop("ti")
    data_fp = Path(ti.xcom_pull(task_ids="get_data_file"))
    data = pd.read_parquet(data_fp)

    data_hash = hashlib.md5()
    data_hash.update(data.sort_values(by="LOC_ID").to_csv(index=False).encode("utf-8"))

    return data_hash.hexdigest()


def is_data_new(**kwargs):
    ti = kwargs.pop("ti")
    data_to_load_unique_id = ti.xcom_pull(task_ids="get_new_data_unique_id")
    backups = Path(Variable.get("backups_dir")) / JOB_NAME

    for f in os.listdir(backups):
        if not os.path.isfile(backups / f):
            continue
        logging.info(f"File in backups: {f}")

        if os.path.isfile(backups / f) and data_to_load_unique_id in f:
            logging.info(f"Data has already been loaded, ID: {data_to_load_unique_id}")
            return "data_is_not_new"

    logging.info(f"Data has not been loaded, new ID: {data_to_load_unique_id}")
    return "data_is_new"


def delete_old_records(**kwargs):
    resource_id = kwargs.pop("ti").xcom_pull(task_ids="get_resource_id")

    return CKAN.action.datastore_delete(id=resource_id, filters={})


def insert_new_records(**kwargs):
    ti = kwargs.pop("ti")
    resource_id = ti.xcom_pull(task_ids="get_resource_id")
    data_fp = Path(ti.xcom_pull(task_ids="get_data_file"))

    data = pd.read_parquet(data_fp)
    records = data.to_dict(orient="records")

    ckan_utils.insert_datastore_records(
        ckan=CKAN,
        resource_id=resource_id,
        records=records,
        chunk_size=int(Variable.get("ckan_insert_chunk_size")),
    )
    return CKAN.action.datastore_create(id=resource_id, records=records)


def build_message(**kwargs):
    ti = kwargs.pop("ti")
    unique_id = ti.xcom_pull(task_ids="get_new_data_unique_id")

    if "already_loaded" in kwargs:
        return f"Data is not new, UID of backup files: {unique_id}. Nothing to load."

    new_data_fp = ti.xcom_pull(task_ids="get_data_file")
    new_data = pd.read_parquet(new_data_fp)

    return f"Refreshed: {new_data.shape[0]} records"


def update_resource_last_modified(**kwargs):
    ti = kwargs.pop("ti")
    resource_id = ti.xcom_pull(task_ids="get_resource_id")
    last_modified_string = ti.xcom_pull(task_ids="get_file_last_modified")
    last_modified = parser.parse(last_modified_string)

    return ckan_utils.update_resource_last_modified(
        ckan=CKAN,
        resource_id=resource_id,
        new_last_modified=last_modified,
    )


def build_data_dict(**kwargs):
    ti = kwargs.pop("ti")
    data_fp = Path(ti.xcom_pull(task_ids="get_data_file"))
    data = pd.read_parquet(data_fp)

    fields = []

    for field, dtype in data.dtypes.iteritems():
        ckan_type_map = {"int64": "int", "object": "text", "float64": "float"}
        fields.append({"type": ckan_type_map[dtype.name], "id": field})

    return fields


def create_new_resource(**kwargs):
    fields = kwargs.pop("ti").xcom_pull(task_ids="build_data_dict")

    CKAN.action.datastore_create(
        resource={
            "package_id": PACKAGE_ID,
            "name": RESOURCE_NAME,
            "format": "csv",
            "is_preview": True,
        },
        fields=fields,
        records=[],
    )


default_args = airflow_utils.get_default_args(
    {
        "on_failure_callback": send_failure_msg,
        "start_date": job_settings["start_date"],
        "retries": 0,
        # "retry_delay": timedelta(minutes=3),
    }
)

with DAG(
    JOB_NAME,
    default_args=default_args,
    description=job_settings["description"],
    schedule_interval=job_settings["schedule"],
    catchup=False,
) as dag:

    create_tmp_dir = PythonOperator(
        task_id="create_tmp_dir",
        python_callable=airflow_utils.create_dir_with_dag_name,
        op_kwargs={"dag_id": JOB_NAME, "dir_variable_name": "tmp_dir"},
    )

    create_backups_dir = PythonOperator(
        task_id="create_backups_dir",
        python_callable=airflow_utils.create_dir_with_dag_name,
        op_kwargs={"dag_id": JOB_NAME, "dir_variable_name": "backups_dir"},
    )

    source_data = PythonOperator(
        task_id="get_data_file",
        python_callable=get_data_file,
        provide_context=True,
    )

    last_modified = PythonOperator(
        task_id="get_file_last_modified",
        python_callable=get_file_last_modified,
    )

    new_resource = PythonOperator(
        task_id="create_new_resource",
        python_callable=create_new_resource,
        provide_context=True,
    )

    package = PythonOperator(
        task_id="get_package",
        python_callable=CKAN.action.package_show,
        op_kwargs={"id": PACKAGE_ID},
    )

    old_data = PythonOperator(
        task_id="backup_old_data",
        python_callable=backup_old_data,
        provide_context=True,
    )

    data_dict = PythonOperator(
        task_id="build_data_dict",
        python_callable=build_data_dict,
        provide_context=True,
    )

    new_data_unique_id = PythonOperator(
        task_id="get_new_data_unique_id",
        python_callable=get_new_data_unique_id,
        provide_context=True,
    )

    is_data_new_branch = BranchPythonOperator(
        task_id="is_data_new",
        python_callable=is_data_new,
        provide_context=True,
    )

    is_resource_new_branch = BranchPythonOperator(
        task_id="is_resource_new", python_callable=is_resource_new, provide_context=True
    )

    resource_id = PythonOperator(
        task_id="get_resource_id",
        python_callable=get_resource_id,
        trigger_rule="none_failed",
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
        task_id="build_loaded_msg",
        python_callable=build_message,
        provide_context=True,
    )

    nothing_to_load_msg = PythonOperator(
        task_id="build_nothing_to_load_msg",
        python_callable=build_message,
        op_kwargs={"already_loaded": True},
        provide_context=True,
    )

    resource_is_not_new = DummyOperator(
        task_id="resource_is_not_new",
    )

    resource_is_new = DummyOperator(
        task_id="resource_is_new",
    )

    data_is_new = DummyOperator(
        task_id="data_is_new",
    )

    data_is_not_new = DummyOperator(
        task_id="data_is_not_new",
    )

    send_loaded_notification = PythonOperator(
        task_id="send_success_msg",
        python_callable=send_success_msg,
        provide_context=True,
        op_kwargs={"msg_task_id": "build_loaded_msg"},
    )

    send_nothing_to_load_msg = PythonOperator(
        task_id="send_nothing_to_load_msg",
        python_callable=send_success_msg,
        provide_context=True,
        op_kwargs={"msg_task_id": "build_nothing_to_load_msg"},
    )

    delete_tmp_files = PythonOperator(
        task_id="delete_tmp_files",
        python_callable=airflow_utils.delete_file,
        op_kwargs={"task_ids": ["get_data_file"]},
        provide_context=True,
        trigger_rule="none_failed",
    )

    delete_tmp_dir = PythonOperator(
        task_id="delete_tmp_dir",
        python_callable=airflow_utils.delete_tmp_data_dir,
        op_kwargs={"dag_id": JOB_NAME},
    )

    update_timestamp = PythonOperator(
        task_id="update_resource_last_modified",
        python_callable=update_resource_last_modified,
        provide_context=True,
    )

    create_tmp_dir >> source_data >> new_data_unique_id >> is_data_new_branch

    create_backups_dir >> old_data

    package >> is_resource_new_branch

    is_resource_new_branch >> resource_is_new >> data_dict >> new_resource >> resource_id

    is_resource_new_branch >> resource_is_not_new >> old_data >> resource_id

    resource_id >> is_data_new_branch

    is_data_new_branch >> data_is_new >> delete_old >> insert_new
    insert_new >> update_timestamp >> loaded_msg >> send_loaded_notification

    data_is_new >> last_modified >> update_timestamp

    is_data_new_branch >> data_is_not_new >> nothing_to_load_msg
    nothing_to_load_msg >> send_nothing_to_load_msg

    [
        send_nothing_to_load_msg,
        send_loaded_notification,
    ] >> delete_tmp_files >> delete_tmp_dir
