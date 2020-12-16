from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from airflow.models import Variable
import pandas as pd
import ckanapi
import logging
import hashlib
from pathlib import Path
from airflow import DAG
import requests
import json
import os
import sys
from dateutil import parser

sys.path.append(Variable.get("repo_dir"))
from utils import airflow as airflow_utils  # noqa: E402
from utils import agol as agol_utils  # noqa: E402
from utils import ckan as ckan_utils  # noqa: E402

job_settings = {
    "description": "Take earlyon.json from opendata.toronto.ca and put into datastore",
    "schedule": "0 17 * * *",
    "start_date": datetime(2020, 11, 24, 13, 35, 0),
}

JOB_FILE = Path(os.path.abspath(__file__))
JOB_NAME = JOB_FILE.name[:-3]
PACKAGE_ID = JOB_NAME.replace("_", "-")

# ACTIVE_ENV = Variable.get("active_env")
ACTIVE_ENV = "qa"
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN = ckanapi.RemoteCKAN(**CKAN_CREDS[ACTIVE_ENV])
SRC_FILE = "http://opendata.toronto.ca/childrens.services/child-family-programs/earlyon.json"  # noqa: E501
RESOURCE_NAME = "EarlyON Child and Family Centres"


def send_success_msg(**kwargs):
    ti = kwargs.pop("ti")
    msg = ti.xcom_pull(task_ids="build_message")

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


def get_file(**kwargs):
    ti = kwargs.pop("ti")
    tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_dir"))

    response = requests.get(SRC_FILE)

    data = pd.DataFrame(response.json())

    filename = "new_data_raw"
    filepath = tmp_dir / f"{filename}.parquet"

    logging.info(f"Read {data.shape[0]} records")

    data.to_parquet(filepath)

    file_last_modified = response.headers["last-modified"]

    return {"path": filepath, "file_last_modified": file_last_modified}


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


def get_resource():
    package = ckan_utils.get_package(ckan=CKAN, package_id=PACKAGE_ID)
    resources = package["resources"]

    resource = [r for r in resources if r["name"] == RESOURCE_NAME][0]

    return resource


def backup_previous_data(**kwargs):
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
    data_hash.update(data.sort_values(by="loc_id").to_csv(index=False).encode("utf-8"))
    unique_id = data_hash.hexdigest()

    logging.info(f"Unique ID generated: {unique_id}")

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
    data_fp = Path(ti.xcom_pull(task_ids="transform_data"))
    data = pd.read_parquet(data_fp)

    data_hash = hashlib.md5()
    data_hash.update(
        data.sort_values(by="loc_id").round(10).to_csv(index=False).encode("utf-8")
    )

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


def delete_previous_records(**kwargs):
    ti = kwargs.pop("ti")
    resource_id = ti.xcom_pull(task_ids="get_resource")["id"]
    backup = ti.xcom_pull(task_ids="backup_previous_data")

    if backup is not None:
        CKAN.action.datastore_delete(id=resource_id, filters={})

        record_count = CKAN.action.datastore_search(id=resource_id, limit=0)["total"]

        msg = f"Records in resource after cleanup: {record_count}"

    else:
        msg = "No backups found, nothing to delete"

    logging.info(msg)


def transform_data(**kwargs):
    ti = kwargs.pop("ti")
    tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_dir"))
    data_fp = Path(ti.xcom_pull(task_ids="get_file")["path"])

    data = pd.read_parquet(data_fp)

    data["geometry"] = data.apply(
        lambda x: json.dumps({"type": "Point", "coordinates": [x["long"], x["lat"]]})
        if x["long"] and x["lat"]
        else "",
        axis=1,
    )

    data = agol_utils.remove_geo_columns(data)

    filename = "new_data_transformed"
    filepath = tmp_dir / f"{filename}.parquet"

    data.to_parquet(filepath)

    return filepath


def insert_new_records(**kwargs):
    ti = kwargs.pop("ti")
    resource_id = ti.xcom_pull(task_ids="get_resource")["id"]
    data_fp = Path(ti.xcom_pull(task_ids="transform_data"))
    backup = ti.xcom_pull(task_ids="backup_previous_data")

    data = pd.read_parquet(data_fp)
    records = data.to_dict(orient="records")

    if backup is None:
        fields = ti.xcom_pull(task_ids="build_data_dict")
    else:
        with open(Path(backup["fields"]), "r") as f:
            fields = json.load(f)

    ckan_utils.insert_datastore_records(
        ckan=CKAN,
        resource_id=resource_id,
        records=records,
        fields=fields,
        chunk_size=int(Variable.get("ckan_insert_chunk_size")),
    )

    return len(records)


def build_message(**kwargs):
    ti = kwargs.pop("ti")

    records_inserted = ti.xcom_pull(task_ids="insert_new_records")

    if records_inserted is None:

        resource = ti.xcom_pull(task_ids="update_resource_last_modified")
        last_modified = parser.parse(resource["last_modified"]).strftime(
            "%Y-%m-%d %H:%M"
        )

        return f"New file, no new data. New last modified timestamp: {last_modified}"

    new_data_fp = Path(ti.xcom_pull(task_ids="transform_data"))

    new_data = pd.read_parquet(new_data_fp)

    return f"Refreshed: {new_data.shape[0]} records"


def update_resource_last_modified(**kwargs):
    ti = kwargs.pop("ti")
    resource_id = ti.xcom_pull(task_ids="get_resource")["id"]
    last_modified_string = ti.xcom_pull(task_ids="get_file")["file_last_modified"]

    return ckan_utils.update_resource_last_modified(
        ckan=CKAN,
        resource_id=resource_id,
        new_last_modified=parser.parse(last_modified_string),
    )


def is_file_new(**kwargs):
    ti = kwargs.pop("ti")
    resource = ti.xcom_pull(task_ids="get_resource")
    last_modified_string = ti.xcom_pull(task_ids="get_file")["file_last_modified"]

    file_last_modified = parser.parse(last_modified_string)
    last_modified_attr = resource["last_modified"]

    if not last_modified_attr:
        last_modified_attr = resource["created"]

    resource_last_modified = parser.parse(last_modified_attr + " UTC")

    difference_in_seconds = (
        file_last_modified.timestamp() - resource_last_modified.timestamp()
    )

    logging.info(
        f"{difference_in_seconds} seconds between file and resource last modified times"
    )

    if difference_in_seconds == 0:
        return "file_is_not_new"

    return "file_is_new"


def build_data_dict(**kwargs):
    ti = kwargs.pop("ti")
    data_fp = Path(ti.xcom_pull(task_ids="transform_data"))
    data = pd.read_parquet(data_fp)

    fields = []

    for field, dtype in data.dtypes.iteritems():
        ckan_type_map = {"int64": "int", "object": "text", "float64": "float"}
        fields.append({"type": ckan_type_map[dtype.name], "id": field})

    return fields


def create_new_resource(**kwargs):
    return CKAN.action.resource_create(
        package_id=PACKAGE_ID,
        name=RESOURCE_NAME,
        format="geojson",
        is_preview=True,
        url_type="datastore",
        extract_job=f"Airflow: {kwargs['dag'].dag_id}",
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
        task_id="get_file",
        python_callable=get_file,
        provide_context=True,
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

    previous_data = PythonOperator(
        task_id="backup_previous_data",
        python_callable=backup_previous_data,
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

    is_file_new_branch = BranchPythonOperator(
        task_id="is_file_new",
        python_callable=is_file_new,
        provide_context=True,
    )

    resource = PythonOperator(
        task_id="get_resource",
        python_callable=get_resource,
        trigger_rule="none_failed",
    )

    delete_previous = PythonOperator(
        task_id="delete_previous_records",
        python_callable=delete_previous_records,
        provide_context=True,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        provide_context=True,
    )

    insert_new = PythonOperator(
        task_id="insert_new_records",
        python_callable=insert_new_records,
        provide_context=True,
    )

    notification_msg = PythonOperator(
        task_id="build_message",
        python_callable=build_message,
        provide_context=True,
    )

    resource_is_not_new = DummyOperator(
        task_id="resource_is_not_new",
    )

    resource_is_new = DummyOperator(
        task_id="resource_is_new",
    )

    file_is_new = DummyOperator(
        task_id="file_is_new",
    )

    file_is_not_new = DummyOperator(
        task_id="file_is_not_new",
    )

    data_is_new = DummyOperator(
        task_id="data_is_new",
    )

    data_is_not_new = DummyOperator(
        task_id="data_is_not_new",
    )

    send_notification = PythonOperator(
        task_id="send_notification",
        python_callable=send_success_msg,
        provide_context=True,
    )

    delete_tmp_dir = PythonOperator(
        task_id="delete_tmp_dir",
        python_callable=airflow_utils.delete_tmp_data_dir,
        op_kwargs={"dag_id": JOB_NAME, "recursively": True},
        trigger_rule="none_failed",
    )

    update_timestamp = PythonOperator(
        task_id="update_resource_last_modified",
        python_callable=update_resource_last_modified,
        provide_context=True,
        trigger_rule="one_success",
    )

    create_tmp_dir >> source_data >> transform >> new_data_unique_id >> is_data_new_branch

    create_backups_dir >> previous_data

    package >> is_resource_new_branch

    is_resource_new_branch >> resource_is_new >> data_dict >> new_resource >> resource

    transform >> resource_is_new

    is_resource_new_branch >> resource_is_not_new >> previous_data >> resource

    is_data_new_branch >> data_is_new >> delete_previous >> insert_new

    insert_new >> update_timestamp

    is_data_new_branch >> data_is_not_new >> update_timestamp

    [transform, resource] >> is_file_new_branch

    is_file_new_branch >> file_is_not_new >> delete_tmp_dir

    is_file_new_branch >> file_is_new >> is_data_new_branch

    update_timestamp >> notification_msg >> send_notification

    send_notification >> delete_tmp_dir
